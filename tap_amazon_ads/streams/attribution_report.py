import singer
import json
import time

from datetime import datetime, timedelta
from dateutil.parser import parse

from .base import ReportBase

LOGGER = singer.get_logger()

class AttributionReportCampaignPerformance(ReportBase):
    @property
    def name(self):
        return "attribution_report_campaign_performance"

    @property
    def key_properties(self):
        return ["profileId", "campaignId", "date"]

    @property
    def api_path(self):
        return "/attribution/report"

    @property
    def content_type(self):
        return "application/json"

    def transform_date_format(self, date):
        return parse(date).isoformat()

    def get_body(self, start, end):
        return {
            "reportType": "PERFORMANCE",
            "endDate": end.strftime('%Y%m%d'),
            "count": 1000,
            "startDate": start.strftime('%Y%m%d'),
            "groupBy": "CAMPAIGN",
            "metrics": ",".join([
                "Click-throughs",
                "attributedDetailPageViewsClicks14d",
                "attributedAddToCartClicks14d",
                "attributedPurchases14d",
                "unitsSold14d",
                "attributedSales14d",
                "attributedTotalDetailPageViewsClicks14d",
                "attributedTotalAddToCartClicks14d",
                "attributedTotalPurchases14d",
                "totalUnitsSold14d",
                "totalAttributedSales14d",
                "brb_bonus_amount",
            ])
        }

