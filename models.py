import logging
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Union

from eventbrite import Eventbrite
from pydantic import BaseModel, HttpUrl

from npsp import Address, Contact, Opportunity, RecurringDonation, SalesforceConfig, SalesforceConnection

logger = logging.getLogger(__name__)

class PeriodType(str, Enum):
    monthly = "monthly"
    yearly = "yearly"
    once = "once"

    @staticmethod
    def from_str(string):
        if string.lower() == "monthly":
            return PeriodType.monthly
        if string.lower() == "yearly":
            return PeriodType.yearly
        else:
            return PeriodType.once



@dataclass
class Organization:
    slug: Optional[str] = None
    eventbrite_token: Optional[str] = None
    eventbrite_org_id: Optional[str] = None
    connector_api_key: Optional[str] = None
    sf_config: Optional[SalesforceConfig] = None
    sfc: Optional[SalesforceConnection] = None
    eb_connection: Optional[Eventbrite] = None
    type_map: Optional[dict] = None
    paypal_client_id: Optional[str] = None
    paypal_client_secret: Optional[str] = None
    paypal_property: Optional[str] = None

    def __repr__(self):
        return self.slug

    def validate_salesforce_config(self) -> bool:
        logger.info("Validating Salesforce configuration for %s", self.slug)
        self.sfc.test_connection()
        REQUIRED_RECORD_TYPES = {"Campaign": ["Event"], "Opportunity": ["Event Ticket"]}
        # REQUIRED_PICKLIST_VALUES = {"CampaignMember": {"Status": ["Checked In", "Sent"]}}

        module_name = __import__("npsp")

        for sf_object_name in [
            "CampaignMember",
            "RecurringDonation",
            "Contact",
            "Campaign",
            "CampaignMember",
            "CampaignMemberStatus",
            "Opportunity",
        ]:
            logger.info("Validating schema for %s", sf_object_name)
            obj = getattr(module_name, sf_object_name)
            schema = getattr(module_name, sf_object_name).schema(sfc=self.sfc)
            object_field_names = [x["name"] for x in schema["fields"]]
            for required_field in obj.sf_field_names:
                if required_field == "RecordType":
                    continue
                if required_field not in object_field_names:
                    print(f"{required_field} not available on {sf_object_name} object for {self}")
                    # raise AttributeError(f"{required_field} not available on {sf_object_name} object for {self}")

            if sf_object_name == "Opportunity":
                for record_type_name in self.type_map.values():
                    if record_type_name.lower() == "ignore":
                        continue
                    if record_type_name not in [x["name"] for x in schema["recordTypeInfos"]]:
                        print(f"'{record_type_name}' record type not available on Opportunity object for {self}")
                        # raise AttributeError(f"'{record_type_name}' record type not available on Opportunity object for {self}")

        # check for required record types
        for object_name, required_record_types in REQUIRED_RECORD_TYPES.items():
            schema = getattr(module_name, object_name).schema(sfc=self.sfc)
            record_types = [x["name"] for x in schema["recordTypeInfos"]]
            for required_record_type in required_record_types:
                if required_record_type not in record_types:
                    raise AttributeError(f"record type {required_record_type} not available on {object_name} object for {self}")

        return True

    def validate_eventbrite_config(self) -> bool:
        logger.info("Validating Eventbrite configuration for %s", self.slug)
        if not self.eb_connection.get_user().ok:
            raise Exception(f"Failed to connect to Eventbrite for {self.slug} (token is probably incorrect)")
        return True
