import logging
import datetime

from npsp import Campaign, CampaignMember, Contact, Opportunity, CampaignMemberStatus, Address
from models import Organization

logger = logging.getLogger(__name__)


def opportunity_from_eb_event(
    contact: Contact,
    attendee: dict,
    campaign: Campaign,
    org: Organization,
    event: dict,
    ticket_class: dict,
) -> Opportunity:

    ticket_category = ticket_class["category"]
    event_name = event["name"]["text"]
    attendee_name = f"{attendee['profile']['first_name']} {attendee['profile']['last_name']}"
    opportunity_name = f"{attendee_name} - {event_name}"[:80]  # limited to 80 chars
    ticket_class_name = ticket_class["name"]
    record_type_name = org.type_map[ticket_category]
    gross_amount = attendee["costs"]["gross"]["value"] / 100
    base_price = attendee["costs"]["base_price"]["value"] / 100
    donor_selected_amount = gross_amount if ticket_class["include_fee"] else base_price
    stage_name = "Refunded" if attendee["refunded"] else "Closed Won"
    eventbrite_id = attendee["id"]
    close_date = datetime.datetime.strptime(attendee["created"], "%Y-%m-%dT%H:%M:%SZ").date()

    # if the account, contact, lead source, amount, and close date are the same either throw an exception or consider it the same opp
    return Opportunity(
        account_id=contact.account_id,
        amount=gross_amount,
        campaign_id=campaign.id_,
        close_date=close_date,
        contact_id_for_role=contact.id_,
        donor_selected_amount=donor_selected_amount,
        eventbrite_ticket_type=ticket_class_name,
        eventbrite_id=eventbrite_id,
        lead_source="Eventbrite",
        name=opportunity_name,
        net_amount=base_price,
        record_type_name=record_type_name,
        sfc=org.sfc,
        stage_name=stage_name,
    )


def upsert_opportunity(
    contact: Contact, attendee: dict, campaign: Campaign, org: Organization, event: dict, ticket_class: dict
) -> Opportunity:
    opportunity = opportunity_from_eb_event(
        contact=contact, attendee=attendee, campaign=campaign, org=org, event=event, ticket_class=ticket_class
    )
    opportunity = opportunity.upsert(
        overwrite=["amount", "donor_selected_amount", "eventbrite_ticket_type", "name", "net_amount", "record_type_name", "stage_name"]
    )

    if opportunity.created:
        logger.info("Opportunity %s created for %s", opportunity, org)
    return opportunity


def get_or_create_campaign_member(
    contact: Contact,
    campaign: Campaign,
    org: Organization,
    eventbrite_id: str,
) -> CampaignMember:

    member = CampaignMember(
        contact_id=contact.id_,
        campaign_id=campaign.id_,
        eventbrite_id=eventbrite_id,
        sfc=org.sfc,
    ).get_or_create()
    if member.created:
        logger.info("Campaign member %s created for Campaign %s (%s)", member, campaign, org)

    return member


def campaign_from_eb_event(event, org: Organization) -> Campaign:
    status_map = {
        "draft": "Planned",
        "live": "In Progress",
        "started": "In Progress",
        "ended": "Completed",
        "completed": "Completed",
        "canceled": "Aborted",
        "deleted": "Aborted",
    }
    status = status_map[event["status"]]
    start_date = datetime.datetime.strptime(event["start"]["local"], "%Y-%m-%dT%H:%M:%S").date()
    return Campaign(
        sfc=org.sfc,
        eventbrite_id=event["id"],
        name=event["name"]["text"],
        start_date=start_date,
        status=status,
    )


def upsert_campaign(event, org: Organization) -> Campaign:
    campaign = campaign_from_eb_event(event=event, org=org).upsert(overwrite=["name", "status"])
    event_url = event["resource_uri"]

    if campaign.created:
        logger.info("Campaign %s created for event %s (%s)", campaign, event_url, org)

    CampaignMemberStatus(sfc=org.sfc, campaign_id=campaign.id_, label="Checked In").get_or_create()
    CampaignMemberStatus(sfc=org.sfc, campaign_id=campaign.id_, label="Registered").get_or_create()
    CampaignMemberStatus(sfc=org.sfc, campaign_id=campaign.id_, label="Deleted").get_or_create()
    CampaignMemberStatus(sfc=org.sfc, campaign_id=campaign.id_, label="Not Attending").get_or_create()

    return campaign


def address_from_attendee(attendee):

    country = None
    city = None
    state = None
    postal_code = None
    street = None
    if "answers" in attendee and attendee["answers"]:
        for question in attendee["answers"]:
            if question["question"] in ["Postal Code", "Zip Code"]:
                try:
                    postal_code = question["answer"]
                except KeyError:
                    logger.debug("No answer to zip/postal code question")
                break
    try:
        address = attendee["profile"]["addresses"]["bill"]
        city = address["city"]
        state = address["region"]
        postal_code = address["postal_code"]
        country = address["country"]
    except KeyError as error:
        address = None
        logger.debug("Complete address not found in attendee record for %s: %s", attendee, error)
    else:
        if "address_2" in address:
            street = address["address_1"] + ", " + address["address_2"]
        else:
            street = address["address_1"]

    address = Address(
        mailing_country=country, mailing_city=city, mailing_state=state, mailing_postal_code=postal_code, mailing_street=street
    )
    return address


def contact_from_eb_event(attendee, org: Organization) -> Contact:

    first_name = attendee["profile"]["first_name"]
    last_name = attendee["profile"]["last_name"]
    email = attendee["profile"]["email"]
    company = attendee["profile"]["company"] if "company" in attendee["profile"] else None
    address = address_from_attendee(attendee)

    return Contact(
        email=email,
        sfc=org.sfc,
        first_name=first_name,
        last_name=last_name,
        eventbrite_company_name=company,
        mailing_country=address.mailing_country,
        mailing_state=address.mailing_state,
        mailing_postal_code=address.mailing_postal_code,
        mailing_city=address.mailing_city,
        mailing_street=address.mailing_street,
        lead_source="Eventbrite",
    )


def upsert_contact(attendee, org: Organization) -> Contact:

    contact = contact_from_eb_event(attendee=attendee, org=org).upsert()
    if contact.created:
        logger.info("Contact %s created for %s", contact, org)

    return contact


def upsert_campaign_member(
    attendee,
    contact: Contact,
    campaign: Campaign,
    org: Organization,
    eventbrite_id: str,
) -> CampaignMember:

    attendee_status_map = {
        "Attending": "Registered",
        "Checked In": "Checked In",
        "Deleted": "Deleted",
        "Not Attending": "Not Attending",
    }
    status = attendee_status_map[attendee["status"]]

    member = CampaignMember(
        contact_id=contact.id_, campaign_id=campaign.id_, eventbrite_id=eventbrite_id, sfc=org.sfc, status=status
    ).upsert(overwrite=["status"])

    if member.created:
        logger.info("Campaign member %s created for Campaign %s (%s)", member, campaign, org)

    return member
