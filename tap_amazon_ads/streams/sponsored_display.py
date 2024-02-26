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

class SponsoredDisplayCampaigns(Base):

    @property
    def name(self):
        return "sponsored_display_campaigns_v3"

    @property
    def key_properties(self):
        return ["profileId", "campaignId"]

    @property
    def api_path(self):
        return "/sd/campaigns"

    @property
    def content_type(self):
        return "application/json"

    @property
    def request_method(self):
        return 'GET'


class SponsoredDisplayAdGroups(Base):

    @property
    def name(self):
        return "sponsored_display_ad_groups_v3"

    @property
    def key_properties(self):
        return ["profileId", "campaignId", "adGroupId"]


    @property
    def api_path(self):
        return "/sd/adGroups"


    @property
    def content_type(self):
        return "application/json"

    @property
    def request_method(self):
        return 'GET'

