import logging
from datetime import datetime, timedelta

import fire

from config import ORGS, ZONE
from npsp import Account, Opportunity, RecurringDonationsSettings
from paypal_sf import (
    PaypalConnection,
    build_api_url,
    handle_paypal_charge_external_initiation,
    PaypalSubscription,
    find_email,
    PaypalTransaction,
)

logger = logging.getLogger(__name__)
formatter = logging.Formatter(fmt="%(levelname)s %(name)s/%(module)s:%(lineno)d - %(message)s")
console = logging.StreamHandler()
console.setFormatter(formatter)
logger.addHandler(console)
logger.setLevel(logging.INFO)
logger.propagate = False

for log in ["npsp", "paypal_sf"]:
    mod_logger = logging.getLogger(log)
    mod_logger.propagate = False
    formatter = logging.Formatter(fmt="%(levelname)s %(name)s/%(module)s:%(lineno)d - %(message)s")
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    mod_logger.addHandler(handler)
    mod_logger.setLevel(logging.INFO)

PAYPAL_ORGS = {k: v for k, v in ORGS.items() if v.paypal_client_id}


def import_org_period(start_date, end_date, org):

    params = {"page_size": 500, "page": 1, "start_date": start_date, "end_date": end_date, "fields": "all"}
    path = "/v1/reporting/transactions"
    url = build_api_url(host="api.paypal.com", path=path, params=params)
    ppc = PaypalConnection(client_id=org.paypal_client_id, client_secret=org.paypal_client_secret)
    transactions = ppc.get_transactions(url)
    logger.info(f"found {len(transactions)} transactions to process...")
    for raw_transaction in transactions:
        handle_paypal_charge_external_initiation(org=org, raw_transaction=raw_transaction, ppc=ppc)


def import_period(start_date, end_date, org_slug=None, all_orgs=False):

    start_date += "T00:00:00-0000"
    end_date += "T23:59:59-0000"

    if all_orgs:
        for org in PAYPAL_ORGS.values():
            logger.info(f"Importing for {org}...")
            import_org_period(start_date=start_date, end_date=end_date, org=org)
        return

    try:
        org = PAYPAL_ORGS[org_slug]
    except KeyError:
        logger.critical("%s isn't configured for PayPal access", org_slug)
        return

    import_org_period(start_date=start_date, end_date=end_date, org=org)


def main():
    fire.Fire(import_period)


if __name__ == "__main__":
    main()
