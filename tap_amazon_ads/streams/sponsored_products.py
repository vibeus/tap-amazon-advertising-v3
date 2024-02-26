import singer
import json
import time

from datetime import datetime, timedelta

from .base import Base

LOGGER = singer.get_logger()

class SponsoredProductsCampaigns(Base):
    @property
    def name(self):
        return "sponsored_products_campaigns_v3"

    @property
    def key_properties(self):
        return ["profileId", "campaignId"]

    @property
    def api_path(self):
        return "/sp/campaigns/list"

    @property
    def accept_type(self):
        return "application/vnd.spCampaign.v3+json"

    @property
    def content_type(self):
        return "application/vnd.spCampaign.v3+json"

    @property
    def list_name(self):
        return "campaigns"


class SponsoredProductsAdGroups(Base):
    @property
    def name(self):
        return "sponsored_products_ad_groups_v3"

    @property
    def key_properties(self):
        return ["profileId", "campaignId", "adGroupId"]

    @property
    def api_path(self):
        return "/sp/adGroups/list"

    @property
    def accept_type(self):
        return "application/vnd.spAdGroup.v3+json"

    @property
    def content_type(self):
        return "application/vnd.spAdGroup.v3+json"

    @property
    def list_name(self):
        return "adGroups"


class SponsoredProductsProductAds(Base):
    @property
    def name(self):
        return "sponsored_products_product_ads"

    @property
    def key_properties(self):
        return ["profileId", "campaignId", "adGroupId"]

    @property
    def api_path(self):
        return "/sp/productAds/list"

    @property
    def accept_type(self):
        return "application/vnd.spProductAd.v3+json"

    @property
    def content_type(self):
        return "application/vnd.spProductAd.v3+json"

    @property
    def list_name(self):
        return "productAds"



