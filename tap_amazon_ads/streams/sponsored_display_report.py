import singer
import json
import time

from datetime import datetime, timedelta

from .base import ReportBase

LOGGER = singer.get_logger()

# Campaign report / Placement report
class SponsoredDisplayReportCampaigns(ReportBase):
    @property
    def name(self):
        return "sponsored_display_report_v3_campaigns"

    @property
    def key_properties(self):
        return ["profileId", "campaignId", "matchedTargetAsin", "date"]

    @property
    def metric_types(self):
        return [
            "purchases", # v2: attributedConversions
            "sales", # v2: attributedSales
            "unitsSoldClicks", # v2: attributedUnitsOrdered
            "detailPageViewsClicks",  # v2: attributedDetailPageViewsClicks
            "salesPromotedClicks",  #v2: attributedSales14dSameSKU
            "purchasesPromotedClicks", #v2: attributedConversions14dSameSKU
            "newToBrandUnitsSoldClicks", #v2: attributedUnitsOrderedNewToBrand14d
            "newToBrandPurchases", #v2: attributedOrdersNewToBrand14d
            "newToBrandSalesClicks", #v2: attributedSalesNewToBrand14d

            "viewClickThroughRate",  # new to v3
            "videoCompleteViews",  # new to v3
            "videoFirstQuartileViews",  #v2: SponsoredBrandsVideo
            "videoMidpointViews",  # new to v3
            "videoThirdQuartileViews",  # new to v3
            "videoUnmutes",  # new to v3
            "viewabilityRate",  # new to v3
        ]

    @property
    def conversion_window(self):
        return None

    def get_configuration(self):
        return {
            "adProduct": "SPONSORED_DISPLAY",
            "groupBy": ["campaign", "matchedTarget"],
            "columns": [
                "date",
                "campaignName",
                "campaignId",
                "campaignStatus",
                "campaignBudgetAmount",
                "campaignBudgetCurrencyCode",
                "matchedTargetAsin",
                ] + self.gen_metrics_names(self.metric_types, conversion_window=self.conversion_window),
            "reportTypeId": "sdCampaigns",
            "timeUnit": "DAILY",
            "format": "GZIP_JSON"
        }

# get performance data at the ad group level by creating an adGroups report
class SponsoredDisplayReportAdGroups(ReportBase):
    @property
    def name(self):
        return "sponsored_display_report_v3_ad_groups"

    @property
    def key_properties(self):
        return ["profileId", "campaignId", "adGroupId", "matchedTargetAsin", "date"]

    @property
    def metric_types(self):
        return [
            "purchases", # v2: attributedConversions
            "sales", # v2: attributedSales
            "unitsSoldClicks", # v2: attributedUnitsOrdered
            "detailPageViewsClicks",  # v2: attributedDetailPageViewsClicks
            "salesPromotedClicks",  #v2: attributedSales14dSameSKU
            "purchasesPromotedClicks", #v2: attributedConversions14dSameSKU
            "newToBrandUnitsSoldClicks", #v2: attributedUnitsOrderedNewToBrand14d
            "newToBrandPurchases", #v2: attributedOrdersNewToBrand14d
            "newToBrandSalesClicks", #v2: attributedSalesNewToBrand14d

            "viewClickThroughRate",  # new to v3
            "videoCompleteViews",  # new to v3
            "videoFirstQuartileViews",  #v2: SponsoredBrandsVideo
            "videoMidpointViews",  # new to v3
            "videoThirdQuartileViews",  # new to v3
            "videoUnmutes",  # new to v3
            "viewabilityRate",  # new to v3
        ]

    @property
    def conversion_window(self):
        return None

    def get_configuration(self):
        return {
            "adProduct": "SPONSORED_DISPLAY",
            "groupBy": ["adGroup", "matchedTarget"],
            "columns": [
                "date",
                "campaignName",
                "campaignId",
                "adGroupName",
                "adGroupId",
                "matchedTargetAsin",
                ] + self.gen_metrics_names(self.metric_types, conversion_window=self.conversion_window),
            "reportTypeId": "sdAdGroup",
            "timeUnit": "DAILY",
            "format": "GZIP_JSON"
        }

# Purchased product report
class SponsoredDisplayReportPurchasedProduct(ReportBase):
    @property
    def name(self):
        return "sponsored_display_report_v3_purchased_product"

    @property
    def key_properties(self):
        return ["profileId", "campaignId", "adGroupId", "date", "promotedAsin"]

    # @property
    # def metric_types(self):
    #     return [
    #         "salesBrandHalo", #v2: viewAttributedConversions14dOtherSKU
    #         "conversionsBrandHalo", #v2:viewAttributedConversions14dOtherSKU
    #     ]

    @property
    def conversion_window(self):
        return None

    def get_configuration(self):
        return {
            "adProduct": "SPONSORED_DISPLAY",
            "groupBy": ["asin"],
            "columns": [
                "date",
                "campaignName",
                "campaignId",
                "campaignBudgetCurrencyCode",
                "adGroupName",
                "adGroupId",
                "promotedAsin",
                "promotedSku",
                ] + self.gen_metrics_names(self.metric_types, conversion_window=[14], base=[]),
            "reportTypeId": "sdPurchasedProduct",
            "timeUnit": "DAILY",
            "format": "GZIP_JSON"
        }

# Targeting report
class SponsoredDisplayReportTargeting(ReportBase):
    @property
    def name(self):
        return "sponsored_display_report_v3_targeting"

    @property
    def key_properties(self):
        return ["profileId", "campaignId", "adGroupId", "targetingExpression", "targetingId", "targetingExpression", "matchedTargetAsin", "date"]

    @property
    def metric_types(self):
        return [
            "purchases", # v2: attributedConversions
            "sales", # v2: attributedSales
            "unitsSoldClicks", # v2: attributedUnitsOrdered
            "detailPageViewsClicks",  # v2: attributedDetailPageViewsClicks
            "salesPromotedClicks",  #v2: attributedSales14dSameSKU
            "purchasesPromotedClicks", #v2: attributedConversions14dSameSKU
            "newToBrandUnitsSoldClicks", #v2: attributedUnitsOrderedNewToBrand14d
            "newToBrandPurchases", #v2: attributedOrdersNewToBrand14d
            "newToBrandSalesClicks", #v2: attributedSalesNewToBrand14d
            "videoCompleteViews",  # new to v3
        ]

    @property
    def conversion_window(self):
        return None

    def get_configuration(self):
        return {
            "adProduct": "SPONSORED_DISPLAY",
            "groupBy": ["targeting", "matchedTarget"],
            "columns": [
                "date",
                "campaignName",
                "campaignId",
                "campaignBudgetCurrencyCode",
                "adGroupName",
                "adGroupId",
                "targetingExpression",
                "targetingId",
                "targetingText",
                "matchedTargetAsin",
                ] + self.gen_metrics_names(self.metric_types, conversion_window=self.conversion_window),
            "reportTypeId": "sdTargeting",
            "timeUnit": "DAILY",
            "format": "GZIP_JSON"
        }

