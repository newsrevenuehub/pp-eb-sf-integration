import logging
from dataclasses import dataclass
from datetime import date, datetime
from re import sub
from typing import Optional, Union
from urllib.parse import urlencode, urlunsplit

import httpx

from models import Address, Organization, PeriodType
from npsp import Contact, Opportunity, RecurringDonation

logger = logging.getLogger(__name__)


DATE_FORMAT = "%Y-%m-%dT%H:%M:%S%z"


def build_api_url(host, path, params=None, scheme="https"):
    query = None
    if params:
        query = urlencode(query=params, safe=":")
    return urlunsplit((scheme, host, path, query, ""))


class PaypalConnection:
    def __init__(self, client_id, client_secret):
        self.client_id = client_id
        self.client_secret = client_secret
        self.host = "api.paypal.com"
        self.access_token = self._get_access_token()
        self.headers = {
            "Authorization": f"Bearer {self.access_token}",
            "User-Agent": "PostmanRuntime/8.26.10",
            "Content-Type": "application/json",
        }

    def _get_access_token(self):
        payload = "grant_type=client_credentials"
        url = build_api_url(host=self.host, path="/v1/oauth2/token", params=None)
        headers = {
            "Accept": "application/json",
            "Accept-Language": "en_US",
            "User-Agent": "PayPalImporter/1.0.0",
        }
        response = httpx.post(url, auth=(self.client_id, self.client_secret), headers=headers, data=payload)
        self.access_token = response.json()["access_token"]
        self.headers = {"Authorization": f"Bearer {self.access_token}"}
        return self.access_token

    def get_subscription(self, subscription_id):
        path = f"/v1/billing/subscriptions/{subscription_id}"
        url = build_api_url(host=self.host, path=path)
        response = httpx.get(url, headers=self.headers)
        if response.status_code == 401:
            self._get_access_token()
            response = httpx.get(url, headers=self.headers)
        return response.json()

    def get_transactions(self, url):
        response = httpx.get(url, headers=self.headers)
        if response.status_code == 401:
            self._get_access_token()
            response = httpx.get(url, headers=self.headers)
        response = response.json()
        if next_url := [x["href"] for x in response["links"] if x["rel"] == "next"]:
            next_url = next_url[0]
            print(next_url)
            logger.info("Found another page of results; fetching")
            return response["transaction_details"] + self.get_transactions(url=next_url)
        return response["transaction_details"]


@dataclass
class Name:
    first_name: str
    last_name: str


def name_from_paypal_transaction(raw_transaction: dict) -> Name:
    payer_info = raw_transaction["payer_info"]
    payer_name = payer_info["payer_name"]

    if "surname" in payer_name:
        first_name = payer_name["given_name"]
        last_name = payer_name["surname"]
        return Name(first_name=first_name, last_name=last_name)

    if "name" in raw_transaction["shipping_info"]:
        name = raw_transaction["shipping_info"]["name"]
        try:
            first_name, last_name = name.split(",")
            last_name = last_name.strip()
            first_name = first_name.strip()
        except ValueError:
            first_name, last_name = name.strip().rsplit(" ", maxsplit=1)

        return Name(first_name=first_name, last_name=last_name)

    if "alternate_full_name" in payer_name:
        first_name, last_name = payer_name["alternate_full_name"].strip().rsplit(" ", maxsplit=1)
        return Name(first_name=first_name, last_name=last_name)

    return None


@dataclass
class PaypalTransaction:
    id_: str
    event_code: str
    reference_id_type: str
    reference_id: str
    account_id: str
    transaction_date: date
    gross_amount: float
    fee_amount: float
    status: str
    subject: str
    email: str
    note: str
    name: Name
    address: Address

    @classmethod
    def from_dict(cls, data):
        info = data["transaction_info"]
        payer_info = data["payer_info"]
        transaction_date = info["transaction_initiation_date"]
        transaction_date = datetime.strptime(transaction_date, DATE_FORMAT).date()
        gross_amount = float(info["transaction_amount"]["value"])
        # the fee amount comes through as a negative so we reverse that here:
        fee_amount = -float(info["fee_amount"]["value"]) if "fee_amount" in info else 0
        subject = info["transaction_subject"] if "transaction_subject" in info else None
        note = info["transaction_note"] if "transaction_note" in info else None
        reference_id = info["paypal_reference_id"] if "paypal_reference_id" in info else None
        reference_id_type = info["paypal_reference_id_type"] if "paypal_reference_id_type" in info else None
        email = payer_info["email_address"].lower() if "email_address" in payer_info else None
        account_id = info["paypal_account_id"] if "paypal_account_id" in info else None
        name = name_from_paypal_transaction(raw_transaction=data)
        address = address_from_paypal_transaction(raw_transaction=data)

        transaction = cls(
            account_id=account_id,
            address=address,
            email=email,
            event_code=info["transaction_event_code"],
            fee_amount=fee_amount,
            gross_amount=gross_amount,
            id_=info["transaction_id"],
            name=name,
            reference_id_type=reference_id_type,
            reference_id=reference_id,
            status=info["transaction_status"],
            subject=subject,
            transaction_date=transaction_date,
            note=note,
        )

        return transaction


class UnsupportedSubscriptionInterval(Exception):
    pass


class DateRangeToleranceExceeded(Exception):
    pass


@dataclass
class PaypalSubscription:
    id_: str
    status: str
    email: str
    amount: float
    payer_id: str
    current_payment_date: date
    create_time: date
    installment_period: Optional[PeriodType] = None

    @classmethod
    def from_dict(cls, data):
        current_payment_date = datetime.strptime(data["billing_info"]["last_payment"]["time"], DATE_FORMAT).date()

        subscription = cls(
            id_=data["id"],
            status=data["status"],
            email=data["subscriber"]["email_address"].lower(),
            amount=float(data["billing_info"]["last_payment"]["amount"]["value"]),
            payer_id=data["subscriber"]["payer_id"],
            current_payment_date=current_payment_date,
            create_time=data["create_time"],
        )
        if data["status"] == "CANCELLED":
            logger.info("subscription %s is canceled; can't determine interval", data["id"])
            return subscription

        if data["status"] == "SUSPENDED":
            logger.info("subscription %s is suspended; can't determine interval", data["id"])
            return subscription

        next_payment_date = datetime.strptime(data["billing_info"]["next_billing_time"], DATE_FORMAT).date()
        days = (next_payment_date - current_payment_date).days
        if days >= 27 and days <= 31:
            subscription.installment_period = PeriodType.monthly
        elif days > 360 and days <= 366:
            subscription.installment_period = PeriodType.yearly
        else:
            logger.warning("Subscription interval not monthly or yearly: %s days", days)

        return subscription


def address_from_paypal_transaction(raw_transaction: dict) -> Union[Address, None]:

    country = None
    city = None
    state = None
    postal_code = None
    street = None

    try:
        address = raw_transaction["shipping_info"]["address"]
        city = address["city"]
        state = address["state"]
        postal_code = address["postal_code"]
        country = address["country_code"]
    except KeyError as error:
        transaction_id = raw_transaction["transaction_info"]["transaction_id"]
        logger.debug("Complete address not found in PayPal transaction %s: %s", transaction_id, error)
        return None
    else:
        if "line2" in address and address["line2"]:
            street = address["line1"] + ", " + address["line2"]
        else:
            street = address["line1"]

    address = Address(
        mailing_country=country, mailing_city=city, mailing_state=state, mailing_postal_code=postal_code, mailing_street=street
    )
    return address


def contact_from_paypal_transaction(transaction, email, org) -> Contact:

    contact = Contact(
        email=email,
        sfc=org.sfc,
        lead_source="PayPal",
        first_name=transaction.name.first_name,
        last_name=transaction.name.last_name,
    )
    if transaction.address:
        contact.mailing_country = transaction.address.mailing_country
        contact.mailing_city = transaction.address.mailing_city
        contact.mailing_state = transaction.address.mailing_state
        contact.mailing_postal_code = transaction.address.mailing_postal_code
        contact.mailing_street = transaction.address.mailing_street

    return contact


def opportunity_from_paypal_transaction(transaction: PaypalTransaction, contact: Contact, org: Organization) -> Opportunity:
    if transaction.subject:
        name = f"PayPal: {transaction.subject} ({transaction.email})"
    else:
        name = f"PayPal: {transaction.email}"

    opportunity = Opportunity(
        account_id=contact.account_id,
        amount=transaction.gross_amount,
        donor_selected_amount=transaction.gross_amount,
        net_amount=transaction.gross_amount - transaction.fee_amount,
        sfc=org.sfc,
        stage_name="Closed Won",
        paypal_account_id=transaction.account_id,
        lead_source="PayPal",
        name=name,
        paypal_transaction_id=transaction.id_,
        close_date=transaction.transaction_date,
        encouraged_by=transaction.note,
        org_property=org.paypal_property,
    )
    return opportunity


def recurring_donation_from_paypal_subscription(subscription, transaction_date, contact, org) -> RecurringDonation:

    if subscription.status != "ACTIVE":
        raise Exception(f"Subscription {subscription.id_} is not ACTIVE")

    recurring_donation = RecurringDonation(
        amount=subscription.amount,
        contact_id=contact.id_,
        date_established=transaction_date,
        installment_period=subscription.installment_period.value,
        installments=None,
        lead_source="PayPal",
        name=f"{transaction_date} for {subscription.email} ({'PayPal'})",
        open_ended_status="Open",
        sfc=org.sfc,
        paypal_account_id=subscription.payer_id,
        paypal_subscription_id=subscription.id_,
        org_property=org.paypal_property,
    )

    return recurring_donation


def update_opportunity(opportunity, transaction):
    opportunity.close_date = transaction.transaction_date
    opportunity.stage_name = "Closed Won"
    opportunity.paypal_transaction_id = transaction.id_
    opportunity.paypal_account_id = transaction.account_id
    opportunity.amount = transaction.gross_amount
    opportunity.donor_selected_amount = transaction.gross_amount
    opportunity.net_amount = transaction.gross_amount - transaction.fee_amount
    opportunity.encouraged_by = transaction.note
    opportunity.save()


def find_closest_opportunity(opportunities, transaction, org):
    logger.debug("[%s] transaction_date: %s", org, transaction.transaction_date)
    opps = [(opp, abs((opp.close_date - transaction.transaction_date).days)) for opp in opportunities]
    opportunity, days_delta = min(opps, key=lambda x: x[1])

    if days_delta > 10:
        raise DateRangeToleranceExceeded(f"time delta too great: {days_delta}")
    logger.debug("[%s] closest opportunity: %s: %s", org, opportunity, opportunity.close_date)
    logger.debug("[%s] time delta: %s", org, days_delta)
    return opportunity


def find_email(transaction: PaypalTransaction, subscription: PaypalSubscription):
    if transaction.email:
        return transaction.email
    return subscription.email


def process_as_single_opportunity(transaction, org, subscription=None):
    email = find_email(transaction=transaction, subscription=subscription)
    contact = contact_from_paypal_transaction(transaction=transaction, email=email, org=org).upsert()
    opportunity_from_paypal_transaction(transaction=transaction, contact=contact, org=org).upsert()
    return


def process_refund(transaction, org, ppc):
    opportunity = Opportunity.get(sfc=org.sfc, paypal_transaction_id=transaction.reference_id)
    opportunity.stage_name = "Refunded"
    opportunity.save()
    # since this was refunded there's a decent chance the sub was canceled; so we'd close the RD
    if not opportunity.recurring_donation_id:
        # there's no recurring donation; skip
        return
    recurring_donation = RecurringDonation.get(sfc=org.sfc, id_=opportunity.recurring_donation_id)
    if recurring_donation.open_ended_status == "Closed":
        # it's already closed; skip
        return
    raw_subscription = ppc.get_subscription(subscription_id=recurring_donation.paypal_subscription_id)
    subscription = PaypalSubscription.from_dict(data=raw_subscription)
    if subscription == "CANCELLED":
        recurring_donation.open_ended_status = "Closed"
        recurring_donation.save()
    return


def process_subscription_payment(transaction, subscription, org):
    # we can't do get_or_create because we'd need the contact and we don't have that yet
    recurring_donation = RecurringDonation.get(sfc=org.sfc, paypal_subscription_id=transaction.reference_id)

    # if this is the first we're seeing the subscription and it's canceled don't bother creating an RD:
    if not recurring_donation and subscription.status == "CANCELLED":
        logger.info("[%s] First contact with subscription %s but it's canceled; creating single opportunity", org, subscription.id_)
        process_as_single_opportunity(transaction=transaction, org=org, subscription=subscription)
        return

    if not recurring_donation and subscription.status == "SUSPENDED":
        logger.info("[%s] First contact with subscription %s but it's suspended; creating single opportunity", org, subscription.id_)
        process_as_single_opportunity(transaction=transaction, org=org, subscription=subscription)
        return

    if not recurring_donation and not subscription.installment_period:
        logger.info("[%s] Unsupported subscription %s interval; creating single opportunity", org, subscription.id_)
        process_as_single_opportunity(transaction=transaction, org=org, subscription=subscription)
        return

    if not recurring_donation:
        logger.info("[%s] RD didn't exist; creating", org)
        email = find_email(transaction=transaction, subscription=subscription)
        contact = contact_from_paypal_transaction(transaction=transaction, email=email, org=org).upsert()
        recurring_donation = recurring_donation_from_paypal_subscription(
            subscription=subscription, transaction_date=transaction.transaction_date, contact=contact, org=org
        )
        recurring_donation.save()

    if subscription.status == "CANCELLED":
        logger.info("[%s] Subscription %s is canceled; closing RD", org, subscription.id_)
        recurring_donation.open_ended_status = "Closed"
        recurring_donation.save()

    opportunities = recurring_donation.opportunities()
    try:
        opportunity = find_closest_opportunity(opportunities=opportunities, transaction=transaction, org=org)
        logger.info("[%s] Found closest opportunity: %s; updating", org, opportunity)
        update_opportunity(opportunity=opportunity, transaction=transaction)
    except DateRangeToleranceExceeded as error:
        logger.warning("[%s] %s: creating new Opportunity and adding to Recurring Donation", org, error)
        email = find_email(transaction=transaction, subscription=subscription)
        contact = contact_from_paypal_transaction(transaction=transaction, email=email, org=org).upsert()
        opportunity = opportunity_from_paypal_transaction(transaction=transaction, contact=contact, org=org)
        opportunity.recurring_donation_id = recurring_donation.id_
        opportunity.installment_period = subscription.installment_period.value
        opportunity.type_ = "Recurring Donation"
        update_opportunity(opportunity=opportunity, transaction=transaction)
    return


def handle_paypal_charge_external_initiation(org: Organization, raw_transaction: dict, ppc: PaypalConnection):
    transaction = PaypalTransaction.from_dict(data=raw_transaction)
    logger.info("[%s] Processing PayPal transaction %s for %s %s", org, transaction.id_, transaction.email, transaction.transaction_date)
    if transaction.event_code in [
        "T0400",  # General withdrawal from PayPal account.
        "T0401",  # AutoSweep.
        "T0003",  # Pre-approved payment (BillUser API). Either sent or received.
        "T0007",  # Website payments standard payment.
        "T0300",  # General funding of PayPal account.
        "T0101",  # Website payments. Pro account monthly fee.
        "T0200",  # General currency conversion.
        "T1501",  # Account hold for open authorization.
        "T1105",  # Reversal of general account hold.
        "T1106",  # Payment reversal, initiated by PayPal.
    ]:
        logger.info("[%s] skipping transaction %s with event code %s", org, transaction.id_, transaction.event_code)
        return

    # T0000: General: received payment of a type not belonging to the other T00nn categories.
    if transaction.event_code == "T0000" and transaction.gross_amount < 0:
        logger.warning(
            "[%s] transaction %s value is <0: %s (event code %s); skipping",
            org,
            transaction.id_,
            transaction.gross_amount,
            transaction.event_code,
        )
        return

    if transaction.event_code == "T1107" and transaction.gross_amount > 0:
        # I think this is when someone has refunded the org's account
        logger.warning(
            "[%s] transaction %s value is <0: %s (event code %s); skipping",
            org,
            transaction.id_,
            transaction.gross_amount,
            transaction.event_code,
        )
        return

    if transaction.event_code == "T1107":  # Payment refund, initiated by merchant.
        logger.info("[%s] Processing refund for original txn %s via %s", org, transaction.reference_id, transaction.id_)
        process_refund(transaction=transaction, org=org, ppc=ppc)
        return

    if transaction.event_code in [
        "T0013",  # Donation payment.
        "T0000",  # General: received payment of a type not belonging to the other T00nn categories.
        "T0011",  # Mobile payment, made through a mobile phone.
        "T0001",  # MassPay payment. Looks like this is some kind of PayPal Giving Fund?
    ]:
        process_as_single_opportunity(transaction=transaction, org=org)
        return

    if transaction.event_code != "T0002":  # Subscription payment. Either payment sent or payment received.
        raise Exception(f"Unknown event code: {transaction.event_code}")
    if transaction.gross_amount < 0:
        logger.warning("[%s] transaction %s value is <0: %s; skipping", org, transaction.id_, transaction.gross_amount)
        return
    if transaction.status != "S":
        raise Exception("Transaction status isn't 'S'")
    if transaction.event_code == "T0002" and transaction.reference_id_type == "SUB":
        raw_subscription = ppc.get_subscription(subscription_id=transaction.reference_id)
        subscription = PaypalSubscription.from_dict(data=raw_subscription)
        logger.info("Found subscription %s", subscription.id_)

        if subscription.status not in ["ACTIVE", "CANCELLED", "SUSPENDED"]:
            raise Exception("unknown subscription status")
        process_subscription_payment(transaction=transaction, subscription=subscription, org=org)

    if transaction.reference_id_type != "SUB":
        logger.warning("[%s] subscription transaction %s has no associated subcription id", org, transaction.id_)
        # look for it by account id instead of reference/subscription ID:
        recurring_donation = RecurringDonation.get(sfc=org.sfc, paypal_account_id=transaction.account_id)
        if recurring_donation:
            opportunities = recurring_donation.opportunities()
            opportunity = find_closest_opportunity(opportunities=opportunities, transaction=transaction, org=org)
            logger.info("[%s] Found closest opportunity: %s; updating", org, opportunity)
            update_opportunity(opportunity=opportunity, transaction=transaction)
            return

        # if we didn't find an RD then process as a single:
        process_as_single_opportunity(transaction=transaction, org=org)
        return
