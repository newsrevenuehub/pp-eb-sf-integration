import logging
from datetime import datetime, timedelta
from pprint import pformat

from celery import Celery
from celery.app.log import TaskFormatter
from celery.schedules import crontab
from celery.utils.log import get_task_logger
from email_validator import EmailNotValidError, validate_email
from kombu import Queue

from config import (
    CELERY_BROKER_URL,
    CELERY_MAX_RETRIES,
    CELERY_RETRY_BACKOFF,
    LOG_LEVEL,
    ORGS,
    SENTRY_DSN,
    SENTRY_ENABLE,
    SENTRY_ENVIRONMENT,
    ZONE,
)
from eb_sf import upsert_campaign, upsert_campaign_member, upsert_contact, upsert_opportunity
from import_paypal import PAYPAL_ORGS, import_org_period

celery_logger = get_task_logger(__name__)
celery_logger.setLevel(LOG_LEVEL)

if SENTRY_ENABLE:
    celery_logger.info("Enabling Sentry")
    import sentry_sdk
    from sentry_sdk.integrations.celery import CeleryIntegration
    from sentry_sdk.integrations.logging import LoggingIntegration

    sentry_logging = LoggingIntegration(level=logging.DEBUG, event_level=logging.WARNING)  # Capture debug and above as breadcrumbs
    sentry_sdk.init(
        dsn=SENTRY_DSN,
        environment=SENTRY_ENVIRONMENT,
        integrations=[sentry_logging, CeleryIntegration()],
    )


WORKER_LOG_FORMAT = "%(levelname)s %(name)s/%(module)s:%(lineno)d - %(message)s"
WORKER_TASK_LOG_FORMAT = "%(levelname)s [%(task_id).8s] %(name)s/%(module)s:%(lineno)d - %(message)s"


celery_app = Celery("worker", broker=CELERY_BROKER_URL)


@celery_app.task()
def import_last_x_days_paypal(days=3):
    celery_logger.info("---> starting import_last_x_days...")

    today = datetime.now(tz=ZONE)
    days_ago = (today - timedelta(days=days)).strftime("%Y-%m-%d")

    start_date = f"{days_ago}T00:00:00-0000"
    end_date = f"{today.strftime('%Y-%m-%d')}T23:59:59-0000"

    for org in PAYPAL_ORGS.values():
        celery_logger.info(f"Importing for {org}...")
        import_org_period(start_date=start_date, end_date=end_date, org=org)


beat_schedule = {
    "import-paypal-once-per-day": {"task": "celery_app.import_last_x_days_paypal", "schedule": crontab(minute="00", hour="23")}
}

celery_app.conf.update(
    #    broker_pool_limit=1, # max number of connections that can be open at one time
    #    worker_concurrency=1,  # number of concurrent worker/processes/threads
    #    worker_hijack_root_logger=False,
    beat_schedule=beat_schedule,
    broker_heartbeat=None,
    result_backend=None,
    task_acks_late=True,
    task_default_rate_limit="1000/h",
    task_ignore_result=True,
    task_queues=[Queue(org.slug) for org in ORGS.values()] + [Queue("celery")],
    timezone="US/Central",
    worker_log_format=WORKER_LOG_FORMAT,
    worker_prefetch_multiplier=1,  # how many tasks to fetch at once per worker
    worker_task_log_format=WORKER_TASK_LOG_FORMAT,
)

for log in ["npsp", "eb_sf", "paypal_sf", "import_paypal"]:
    logger = logging.getLogger(log)
    logger.propagate = False
    formatter = TaskFormatter(WORKER_TASK_LOG_FORMAT)
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)


class RateLimitException(Exception):
    pass


@celery_app.task(autoretry_for=(Exception,), retry_backoff=CELERY_RETRY_BACKOFF, max_retries=CELERY_MAX_RETRIES)
def handle_attendee_updated(org_slug: str, attendee: dict) -> bool:

    email = attendee["profile"]["email"]
    try:
        valid = validate_email(email)
        email = valid.email
    except EmailNotValidError as error:
        celery_logger.info("Email %s invalid: %s; discarding", email, error)
        return None

    attendee_url = attendee["resource_uri"]
    celery_logger.info("Updating attendee %s for %s", attendee_url, org_slug)
    org = ORGS[org_slug]

    event_id = attendee["event_id"]
    attendee = org.eb_connection.get_event_attendee(id=event_id, attendee_id=attendee["id"])
    try:
        check_eventbrite_status_code(attendee)
    except NotFoundException as error:
        # I don't get why you send us updates for objects that you then say don't exist
        celery_logger.info("object doesn't exist; skipping: %s", error)
        return True
    ticket_class_id = attendee["ticket_class_id"]

    event = org.eb_connection.get_event(event_id, expand="ticket_classes")
    check_eventbrite_status_code(event)
    ticket_class: dict = dict()
    for tclass in event["ticket_classes"]:
        if tclass["id"] == ticket_class_id:
            ticket_class = tclass
            break

    campaign = upsert_campaign(event=event, org=org)
    contact = upsert_contact(attendee=attendee, org=org)
    upsert_campaign_member(attendee=attendee, org=org, contact=contact, campaign=campaign, eventbrite_id=attendee["id"])

    amount = attendee["costs"]["gross"]["value"] / 100
    ticket_category = ticket_class["category"]
    if ticket_category == "add_on":
        celery_logger.info("[%s] ticket category 'add_on' not supported; skipping", org)
        return True

    record_type_name = org.type_map[ticket_category]

    if record_type_name.lower() == "ignore":  # kludgy
        celery_logger.info("%s configured to ignore opportunities of type %s; doing nothing", org, ticket_category)
        return True

    if amount > 0:
        celery_logger.info("Attendee %s cost is %s so upserting Opportunity...", attendee_url, amount)
        upsert_opportunity(contact=contact, attendee=attendee, campaign=campaign, org=org, event=event, ticket_class=ticket_class)
    else:
        celery_logger.debug("$0 amount so no opportunity created")

    celery_logger.info("Contact %s for %s processed for %s", contact, attendee_url, org)
    return True


@celery_app.task(autoretry_for=(Exception,), retry_backoff=CELERY_RETRY_BACKOFF, max_retries=CELERY_MAX_RETRIES)
def handle_attendee_checked_in(org_slug: str, attendee=None):

    attendee_url = attendee["resource_uri"]
    event_id = attendee["event_id"]
    org = ORGS[org_slug]

    celery_logger.info("Checking attendee %s in to event...", attendee_url)

    attendee = org.eb_connection.get_event_attendee(id=event_id, attendee_id=attendee["id"], expand="event")
    try:
        check_eventbrite_status_code(attendee)
    except NotFoundException as error:
        # I don't get why you send us updates for objects that you then say don't exist
        celery_logger.info("object doesn't exist; skipping: %s", error)
        return
    event = attendee["event"]
    campaign = upsert_campaign(event=event, org=org)

    contact = upsert_contact(attendee, org=org)
    member = upsert_campaign_member(attendee=attendee, contact=contact, campaign=campaign, eventbrite_id=attendee["id"], org=org)

    celery_logger.info("%s checked in to %s", member, event_id)

    return True


@celery_app.task(autoretry_for=(Exception,), retry_backoff=CELERY_RETRY_BACKOFF, max_retries=CELERY_MAX_RETRIES)
def handle_event_updated(org_slug: str, event):
    event_url = event["resource_uri"]
    celery_logger.info("Updating event %s", event_url)
    org = ORGS[org_slug]
    event = org.eb_connection.get_event(event["id"])
    upsert_campaign(event=event, org=org)
    return True


class NotFoundException(Exception):
    pass


def check_eventbrite_status_code(eventbrite_object):
    header = eventbrite_object.headers["X-Rate-Limit"]
    splits = header.split()
    string = " ".join([splits[1], splits[2]])
    celery_logger.info(string)
    if eventbrite_object.status_code == 429:
        raise RateLimitException(string)
    elif eventbrite_object.status_code == 404:
        raise NotFoundException(eventbrite_object["error"])
    elif eventbrite_object.status_code >= 400:
        raise Exception(eventbrite_object["error"])
    elif not eventbrite_object.ok:
        raise Exception(eventbrite_object["error"])
    return


@celery_app.task(autoretry_for=(Exception,), retry_backoff=CELERY_RETRY_BACKOFF, max_retries=CELERY_MAX_RETRIES)
def queue_request(action: str, org_slug: str, the_request: str) -> bool:

    org = ORGS[org_slug]
    eventbrite_object = org.eb_connection.webhook_to_object(the_request)
    celery_logger.debug(the_request)
    if eventbrite_object.status_code == 404:
        # I don't get why you send us updates for objects that you then say don't exist
        celery_logger.info("object doesn't exist; skipping")
        return

    celery_logger.debug("eventbrite_object: %s", pformat(eventbrite_object))
    celery_logger.info("Received %s request for org %s: %s", action, org_slug, eventbrite_object["resource_uri"])
    if action == "attendee.updated":
        handle_attendee_updated.apply_async(queue=org_slug, kwargs={"attendee": eventbrite_object, "org_slug": org_slug})
    elif action in ["event.updated", "event.created"]:
        handle_event_updated.apply_async(queue=org_slug, kwargs={"event": eventbrite_object, "org_slug": org_slug})
    elif action == "barcode.checked_in":
        handle_attendee_checked_in.apply_async(queue=org_slug, kwargs={"attendee": eventbrite_object, "org_slug": org_slug})
    elif action in ["order.placed", "order.updated", "order.refunded"]:
        order = org.eb_connection.get_order(eventbrite_object["id"], expand="attendees")
        for attendee in order["attendees"]:
            attendee_url = attendee["resource_uri"]
            celery_logger.debug(pformat(attendee))
            celery_logger.info("Queueing attendee %s update for order...", attendee_url)
            handle_attendee_updated.apply_async(queue=org_slug, kwargs={"attendee": attendee, "org_slug": org_slug})
        if not order["attendees"]:
            celery_logger.info("No attendees listed on order")
    else:
        celery_logger.info("action %s not supported", action)

    return True
