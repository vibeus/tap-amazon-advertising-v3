import singer
import json
import time

from datetime import datetime, timedelta

from .base import Base

LOGGER = singer.get_logger()

# use BASE_URL[config['region']]
BASE_URL = {
    'NA': 'https://advertising-api.amazon.com',
    'EU': 'https://advertising-api-eu.amazon.com',
    'FE': 'https://advertising-api-fe.amazon.com'
}

TOKEN_URL = 'https://api.amazon.com/auth/o2/token'
SCOPES = ["advertising::campaign_management"]

class SponsoredBrandsCampaigns(Base):

    @property
    def name(self):
        return "sponsored_brands_campaigns_v3"

    @property
    def key_properties(self):
        return ["profileId", "campaignId"]

    @property
    def api_path(self):
        return "/sb/v4/campaigns/list"

    @property
    def accept_type(self):
        return "application/vnd.sbcampaignresource.v4+json"

    @property
    def content_type(self):
        return "text/plain"

    @property
    def list_name(self):
        return "campaigns"


class SponsoredBrandsAdGroups(Base):

    @property
    def name(self):
        return "sponsored_brands_ad_groups_v3"

    @property
    def key_properties(self):
        return ["profileId", "campaignId", "adGroupId"]


    @property
    def api_path(self):
        return "/sb/v4/adGroups/list"

    @property
    def accept_type(self):
        return "application/vnd.sbadgroupresource.v4+json"

    @property
    def content_type(self):
        return "text/plain"

    @property
    def list_name(self):
        return "adGroups"

