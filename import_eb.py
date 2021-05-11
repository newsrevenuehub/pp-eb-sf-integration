# import logging
import os
import os.path
import time
from datetime import datetime, timedelta
from urllib.parse import urlparse

import requests
import typer
from dateutil import parser, tz
from dateutil.parser import parserinfo

from celery_app import handle_attendee_updated, handle_event_updated
from config import ORGS

# logger = logging.getLogger(__name__)
# formatter = logging.Formatter(fmt="%(levelname)s %(name)s/%(module)s:%(lineno)d - %(message)s")
# console = logging.StreamHandler()
# console.setFormatter(formatter)
# logger.addHandler(console)
# logger.setLevel(LOG_LEVEL)
# # logger.propagate = False

tzinfos = {x: tz.tzutc() for x in parserinfo().UTCZONE}
os.environ["TZ"] = "UTC"
time.tzset()

# We use the Eventbrite REST API directly here instead of the SDK because the SDK doesn't support pagination or Organizations


def fetch_list_from_eb(url: str, eb_api_key: str) -> dict:

    headers = {"Authorization": f"Bearer {eb_api_key}"}
    path = urlparse(url).path
    path = path.rstrip("/")
    item = os.path.basename(path)
    response = requests.get(url, headers=headers).json()
    final_response = response[item]
    while response["pagination"]["has_more_items"] is True:
        typer.echo("more items; fetching...")
        interim_url = url + "?continuation=" + response["pagination"]["continuation"]
        typer.echo(interim_url)
        response = requests.get(interim_url, headers=headers).json()
        final_response.extend(response[item])

    return final_response


def fetch_single_from_eb(url: str, eb_api_key: str) -> dict:
    headers = {"Authorization": f"Bearer {eb_api_key}"}
    return requests.get(url, headers=headers).json()


def get_eb_organization(org_slug: str) -> dict:
    eb_api_key = ORGS[org_slug].eventbrite_token
    eb_org_id = ORGS[org_slug].eventbrite_org_id
    url = "https://www.eventbriteapi.com/v3/users/me/organizations/"
    organizations = fetch_list_from_eb(url, eb_api_key=eb_api_key)
    if eb_org_id:
        organizations = [org for org in organizations if org["id"] == eb_org_id]
    return organizations[0]


def process_attendees(org_slug: str, event: dict):
    event_id = event["id"]
    event_url = event["url"]
    eb_api_key = ORGS[org_slug].eventbrite_token
    attendees = fetch_list_from_eb(url=f"https://www.eventbriteapi.com/v3/events/{event_id}/attendees/", eb_api_key=eb_api_key)
    typer.echo(f"Found {len(attendees)} attendees for {event_url}")

    for attendee in attendees:
        handle_attendee_updated.apply_async(queue=org_slug, kwargs={"org_slug": org_slug, "attendee": attendee})


def process_events(org_slug, days: int = 90):

    eb_api_key = ORGS[org_slug].eventbrite_token
    eb_org = get_eb_organization(org_slug=org_slug)
    url = f"https://www.eventbriteapi.com/v3/organizations/{eb_org['id']}/events/"

    events = fetch_list_from_eb(url=url, eb_api_key=eb_api_key)
    typer.echo(f"Found {len(events)} events for {eb_org['name']}")

    for event in events:

        event_url = event["url"]
        end_date = parser.parse(event["end"]["utc"], tzinfos=tzinfos)
        if (age := datetime.now(tz=tz.tzutc()) - end_date) > timedelta(days=days):
            typer.echo(f"{event_url} age {age} is older than {days} days; skipping...")
            continue
        typer.echo(f"handling event {event_url}...")
        handle_event_updated.apply_async(queue=org_slug, kwargs={"org_slug": org_slug, "event": event})

        process_attendees(org_slug=org_slug, event=event)


# May want to
# - process all attendees from all events in the last 90 days (or anytime in the future)
# - process a single event
# - process a single attendee
# - process a single event with all its attendees
def main(org_slug: str, days: int = 90, attendee: str = None, event: str = None, include_attendees: bool = False):

    if not any([attendee, event]):
        typer.echo(f"Processing last {days} days of events for {org_slug}...")
        process_events(org_slug=org_slug, days=days)
        return

    eb_api_key = ORGS[org_slug].eventbrite_token

    if event and attendee:
        typer.echo("Can't specify both and event and an attendee.")
        return

    if event:
        typer.echo(f"Processing {event}...")
        event = fetch_single_from_eb(url=event, eb_api_key=eb_api_key)
        handle_event_updated.apply_async(queue=org_slug, kwargs={"org_slug": org_slug, "event": event})
        if include_attendees:
            typer.echo(f"Processing attendees for {event}...")
            process_attendees(org_slug=org_slug, event=event)

    if attendee:
        typer.echo(f"Processing {attendee}...")
        attendee = fetch_single_from_eb(url=attendee, eb_api_key=eb_api_key)
        handle_attendee_updated.apply_async(queue=org_slug, kwargs={"org_slug": org_slug, "attendee": attendee})


if __name__ == "__main__":
    typer.run(main)
