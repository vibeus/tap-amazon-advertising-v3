import singer
import json
import time

from datetime import datetime, timedelta

from .base import ReportBase

LOGGER = singer.get_logger()

# Campaign report / Placement report
class SponsoredBrandsReportCampaigns(ReportBase):
    @property
    def name(self):
        return "sponsored_brands_report_v3_campaigns"

    @property
    def key_properties(self):
        return ["profileId", "campaignId", "date"]

    @property
    def metric_types(self):
        return [
            "purchases", # v2: attributedConversions
            "sales", # v2: attributedSales
            "unitsSoldClicks", # v2: attributedUnitsOrdered
            "detailPageViewsClicks",  # v2: attributedDetailPageViewsClicks
            "viewClickThroughRate",  # v2: SponsoredBrandsVideo
            "video5SecondViewRate",  # v2: SponsoredBrandsVideo
            "video5SecondViews",  # v2: SponsoredBrandsVideo
            "videoCompleteViews",  # v2: SponsoredBrandsVideo
            "videoFirstQuartileViews",  #v2: SponsoredBrandsVideo
            "videoMidpointViews",  # v2: SponsoredBrandsVideo
            "videoThirdQuartileViews",  # v2: SponsoredBrandsVideo
            "videoUnmutes",  # v2: SponsoredBrandsVideo
            "viewableImpressions",  # v2: SponsoredBrandsVideo
            "viewabilityRate",  # v2: SponsoredBrandsVideo
        ]

    @property
    def conversion_window(self):
        return None

    def get_configuration(self):
        return {
            "adProduct": "SPONSORED_BRANDS",
            "groupBy": ["campaign"],
            "columns": [
                "date",
                "campaignName",
                "campaignId",
                "campaignStatus",
                "campaignBudgetAmount",
                "campaignBudgetType",
                "campaignBudgetCurrencyCode"
                ] + self.gen_metrics_names(self.metric_types, conversion_window=self.conversion_window),
            "reportTypeId": "sbCampaigns",
            "timeUnit": "DAILY",
            "format": "GZIP_JSON"
        }

# get performance data at the ad group level by creating an adGroups report
class SponsoredBrandsReportAdGroups(ReportBase):
    @property
    def name(self):
        return "sponsored_brands_report_v3_ad_groups"

    @property
    def key_properties(self):
        return ["profileId", "campaignId", "adGroupId", "date"]

    @property
    def metric_types(self):
        return [
            "purchases", # v2: attributedConversions
            "sales", # v2: attributedSales
            "unitsSoldClicks", # v2: attributedUnitsOrdered
            "detailPageViewsClicks",  # v2: attributedDetailPageViewsClicks
            "viewClickThroughRate",  # v2: SponsoredBrandsVideo
            "video5SecondViewRate",  # v2: SponsoredBrandsVideo
            "video5SecondViews",  # v2: SponsoredBrandsVideo
            "videoCompleteViews",  # v2: SponsoredBrandsVideo
            "videoFirstQuartileViews",  #v2: SponsoredBrandsVideo
            "videoMidpointViews",  # v2: SponsoredBrandsVideo
            "videoThirdQuartileViews",  # v2: SponsoredBrandsVideo
            "videoUnmutes",  # v2: SponsoredBrandsVideo
            "viewableImpressions",  # v2: SponsoredBrandsVideo
            "viewabilityRate",  # v2: SponsoredBrandsVideo
        ]

    @property
    def conversion_window(self):
        return None

    def get_configuration(self):
        return {
            "adProduct": "SPONSORED_BRANDS",
            "groupBy": ["adGroup"],
            "columns": [
                "date",
                "campaignName",
                "campaignId",
                "adGroupName",
                "adGroupId",
                ] + self.gen_metrics_names(self.metric_types, conversion_window=self.conversion_window),
            "reportTypeId": "sbAdGroup",
            "timeUnit": "DAILY",
            "format": "GZIP_JSON"
        }

class SponsoredBrandsReportSearchTerm(ReportBase):
    @property
    def name(self):
        return "sponsored_brands_report_v3_search_term"

    @property
    def key_properties(self):
        return ["profileId", "campaignId", "adGroupId", "keywordId", "date"]

    @property
    def metric_types(self):
        return [
            "purchases", # v2: attributedConversions
            "sales", # v2: attributedSales
            "unitsSold", # v2:viewAttributedUnitsOrdered
            "purchasesClicks", # v2: attributedConversions
            "salesClicks",  # v2: attributedSales
            # "unitsSoldClicks", # v2: attributedUnitsOrdered
            # "detailPageViewsClicks",  # v2: attributedDetailPageViewsClicks
            "viewClickThroughRate",  # v2: SponsoredBrandsVideo
            "video5SecondViewRate",  # v2: SponsoredBrandsVideo
            "video5SecondViews",  # v2: SponsoredBrandsVideo
            "videoCompleteViews",  # v2: SponsoredBrandsVideo
            "videoFirstQuartileViews",  #v2: SponsoredBrandsVideo
            "videoMidpointViews",  # v2: SponsoredBrandsVideo
            "videoThirdQuartileViews",  # v2: SponsoredBrandsVideo
            "videoUnmutes",  # v2: SponsoredBrandsVideo
            "viewableImpressions",  # v2: SponsoredBrandsVideo
            "viewabilityRate",  # v2: SponsoredBrandsVideo
        ]

    @property
    def conversion_window(self):
        return None

    def get_configuration(self):
        return {
            "adProduct": "SPONSORED_BRANDS",
            "groupBy": ["searchTerm"],
            "columns": [
                "date",
                "campaignName",
                "campaignId",
                "campaignStatus",
                "campaignBudgetAmount",
                "campaignBudgetType",
                "campaignBudgetCurrencyCode",
                "adGroupName",
                "adGroupId",
                "searchTerm",
                "keywordId",
                "keywordText",
                "matchType",
                "keywordType"
                ] + self.gen_metrics_names(self.metric_types, conversion_window=self.conversion_window),
            "reportTypeId": "sbSearchTerm",
            "filters": [
                {
                    "field": "keywordType",
                    "values": [
                    "BROAD",
                    "PHRASE",
                    "EXACT",
                    "TARGETING_EXPRESSION",
                    "TARGETING_EXPRESSION_PREDEFINED"
                    ]
                }
            ],
            "timeUnit": "DAILY",
            "format": "GZIP_JSON"
        }

# Purchased product report:
class SponsoredBrandsReportPurchasedProduct(ReportBase):
    @property
    def name(self):
        return "sponsored_brands_report_v3_purchased_product"

    @property
    def key_properties(self):
        return ["profileId", "campaignId", "adGroupId", "keywordId", "date", "purchasedAsin"]

    @property
    def metric_types(self):
        return [
            "orders", # v2: attributedConversions
            "sales", # v2: attributedSales
            "unitsSold", # v2: attributedUnitsOrdered
        ]

    @property
    def conversion_window(self):
        return None

    def get_configuration(self):
        return {
            "adProduct": "SPONSORED_BRANDS",
            "groupBy": ["purchasedAsin"],
            "columns": [
                "date",
                "campaignName",
                "campaignId",
                "campaignBudgetCurrencyCode",
                "adGroupName",
                "adGroupId",
                "attributionType",
                "purchasedAsin",
                "productName",
                "productCategory",
                ] + self.gen_metrics_names(self.metric_types, conversion_window=self.conversion_window, base=[]),
            "reportTypeId": "sbPurchasedProduct",
            "timeUnit": "DAILY",
            "format": "GZIP_JSON"
        }


# Targeting report
# targeting in v2: keywordType in ("TARGETING_EXPRESSION", "TARGETING_EXPRESSION_PREDEFINED")
# keyword in v2: keywordType in ("BROAD", "PHRASE", "EXACT")
class SponsoredBrandsReportTargeting(ReportBase):
    @property
    def name(self):
        return "sponsored_brands_report_v3_targeting"

    @property
    def key_properties(self):
        return ["profileId", "campaignId", "adGroupId", "keywordId", "keywordType", "date"]

    @property
    def metric_types(self):
        return [
            "purchases", # v2: attributedConversions
            "sales", # v2: attributedSales
            "unitsSold", # v2:viewAttributedUnitsOrdered
            "purchasesClicks", # v2: attributedConversions
            "salesClicks",  # v2: attributedSales
            # "unitsSoldClicks", # v2: attributedUnitsOrdered
            # "detailPageViewsClicks",  # v2: attributedDetailPageViewsClicks
            "viewClickThroughRate",  # v2: SponsoredBrandsVideo
            "video5SecondViewRate",  # v2: SponsoredBrandsVideo
            "video5SecondViews",  # v2: SponsoredBrandsVideo
            "videoCompleteViews",  # v2: SponsoredBrandsVideo
            "videoFirstQuartileViews",  #v2: SponsoredBrandsVideo
            "videoMidpointViews",  # v2: SponsoredBrandsVideo
            "videoThirdQuartileViews",  # v2: SponsoredBrandsVideo
            "videoUnmutes",  # v2: SponsoredBrandsVideo
            "viewableImpressions",  # v2: SponsoredBrandsVideo
            "viewabilityRate",  # v2: SponsoredBrandsVideo
        ]

    @property
    def conversion_window(self):
        return None

    def get_configuration(self):
        return {
            "adProduct": "SPONSORED_BRANDS",
            "groupBy": ["targeting"],
            "columns": [
                "date",
                "campaignName",
                "campaignId",
                "campaignStatus",
                "campaignBudgetAmount",
                "campaignBudgetType",
                "campaignBudgetCurrencyCode",
                "adGroupName",
                "adGroupId",
                "keywordId",
                "keywordText",
                "keywordType",
                "matchType",
                "targetingExpression",
                "targetingId",
                "targetingText",
                "targetingType"
            ] + self.gen_metrics_names(self.metric_types, conversion_window=self.conversion_window),
            "reportTypeId": "sbTargeting",
            "filters": [
                {
                    "field": "adKeywordStatus",
                    "values": [
                        "ENABLED", "PAUSED", "ARCHIVED"
                    ]
                },
                {
                    "field": "keywordType",
                    "values": [
                        "TARGETING_EXPRESSION", "TARGETING_EXPRESSION_PREDEFINED"
                    ]
                }
            ],
            "timeUnit": "DAILY",
            "format": "GZIP_JSON"
        }

