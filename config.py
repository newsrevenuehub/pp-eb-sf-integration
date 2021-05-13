import logging
import os
from functools import partial

from environs import Env
from eventbrite import Eventbrite
from pytz import timezone

from models import Organization

env = Env()

LOG_LEVEL = env("LOG_LEVEL", "DEBUG")
ZONE = timezone(env("TIMEZONE", "US/Central"))

SENTRY_ENABLE = env.bool("SENTRY_ENABLE", False)
SENTRY_DSN = env("SENTRY_DSN")
SENTRY_ENVIRONMENT = env("SENTRY_ENVIRONMENT", "test")

SALESFORCE_API_VERSION = env("SALESFORCE_API_VERSION", "v48.0")

CELERY_BROKER_URL = env("CELERY_BROKER_URL")
CELERY_MAX_RETRIES = env.int("CELERY_MAX_RETRIES", 1)
CELERY_RETRY_BACKOFF = env.int("CELERY_RETRY_BACKOFF", 5)  # in seconds

logger = logging.getLogger(__name__)


def construct_org(org_slug: str) -> Organization:

    type_map: dict = dict()
    with env.prefixed(f"{org_slug.upper()}_"):
        connector_api_key = env("CONNECTOR_API_KEY")
        eventbrite_token = env("EVENTBRITE_TOKEN", None)
        eventbrite_org_id = env("EVENTBRITE_ORG_ID", None)
        if eventbrite_token:
            for mapping in env("TYPE_MAP").split(","):
                eb_type, sf_type = mapping.split(":")
                type_map[eb_type] = sf_type
        paypal_client_id = env("PAYPAL_CLIENT_ID", None)
        paypal_client_secret = env("PAYPAL_CLIENT_SECRET", None)
        paypal_property = env("PAYPAL_PROPERTY", None)

    with env.prefixed(f"{org_slug.upper()}_SALESFORCE_"):
        sf_config = SalesforceConfig(
            client_id=env("CLIENT_ID"),
            client_secret=env("CLIENT_SECRET"),
            username=env("USERNAME"),
            password=env("PASSWORD"),
            host=env("HOST"),
            api_version=SALESFORCE_API_VERSION,
            slug=org_slug,
        )
        logger.debug(sf_config)

    org = Organization(
        slug=org_slug,
        eventbrite_token=eventbrite_token,
        eventbrite_org_id=eventbrite_org_id,
        connector_api_key=connector_api_key,
        sf_config=sf_config,
        sfc=SalesforceConnection(config=sf_config),
        eb_connection=Eventbrite(eventbrite_token),
        type_map=type_map,
        paypal_client_id=paypal_client_id,
        paypal_client_secret=paypal_client_secret,
        paypal_property=paypal_property if paypal_property else "Property1",
    )
    return org


ORGS: dict = dict()

for slug in [x.split("_")[0].lower() for x in os.environ.keys() if x.endswith("CONNECTOR_API_KEY")]:
    logger.info("Found configuration for %s", slug)
    ORGS[slug] = construct_org(slug)
