import singer
import json
import time

from datetime import datetime, timedelta

from .base import ReportBase

LOGGER = singer.get_logger()

# Campaign report / Placement report
class SponsoredProductsReportCampaigns(ReportBase):
    @property
    def name(self):
        return "sponsored_products_report_v3_campaigns"

    @property
    def key_properties(self):
        return ["profileId", "countryCode", "campaignId", "placementClassification", "date"]

    @property
    def metric_types(self):
        return [
            "purchases","purchasesSameSku", # v2: attributedConversions
            "sales","attributedSalesSameSku", # v2: attributedSales
            "unitsSoldClicks","unitsSoldSameSku" # v2: attributedUnitsOrdered
        ]

    def get_configuration(self):
        return {
            "adProduct": "SPONSORED_PRODUCTS",
            "groupBy": ["campaign", "campaignPlacement"],
            "columns": [
                "date",
                "campaignName",
                "campaignId",
                "campaignStatus",
                "campaignBudgetAmount",
                "campaignBudgetType",
                "campaignBudgetCurrencyCode",
                "placementClassification",]
                 + self.gen_metrics_names(self.metric_types),
            "reportTypeId": "spCampaigns",
            "timeUnit": "DAILY",
            "format": "GZIP_JSON"
        }

# get performance data at the ad group level by creating an adGroups report
class SponsoredProductsReportAdGroups(ReportBase):
    @property
    def name(self):
        return "sponsored_products_report_v3_ad_groups"

    @property
    def key_properties(self):
        return ["profileId", "countryCode", "campaignId", "adGroupId", "date"]

    @property
    def metric_types(self):
        return [
            "purchases","purchasesSameSku", # v2: attributedConversions
            "sales","attributedSalesSameSku", # v2: attributedSales
            "unitsSoldClicks","unitsSoldSameSku" # v2: attributedUnitsOrdered
        ]

    def get_configuration(self):
        return {
            "adProduct": "SPONSORED_PRODUCTS",
            "groupBy": ["campaign","adGroup"],
            "columns": [
                "date",
                "campaignName",
                "campaignId",
                "adGroupName",
                "adGroupId",]
                 + self.gen_metrics_names(self.metric_types),
            "reportTypeId": "spCampaigns",
            "timeUnit": "DAILY",
            "format": "GZIP_JSON"
        }

# Search Term report
class SponsoredProductsReportSearchTerm(ReportBase):
    @property
    def name(self):
        return "sponsored_products_report_v3_search_term"

    @property
    def key_properties(self):
        return ["profileId", "countryCode", "campaignId", "adGroupId", "keywordId", "targeting", "date"]

    @property
    def metric_types(self):
        return [
            "purchases","purchasesSameSku", # v2: attributedConversions
            "sales","attributedSalesSameSku", # v2: attributedSales
            "unitsSoldClicks","unitsSoldSameSku" # v2: attributedUnitsOrdered
        ]

    def get_configuration(self):
        return {
            "adProduct": "SPONSORED_PRODUCTS",
            "groupBy": ["asin"],
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
                "portfolioId",
                "searchTerm",
                "keywordId",
                "keyword",
                "keywordType",
                "matchType",
                "targeting",]
                 + self.gen_metrics_names(self.metric_types),
            "reportTypeId": "spPurchasedProduct",
            "timeUnit": "DAILY",
            "format": "GZIP_JSON"
        }

# Purchased product report:
class SponsoredProductsReportPurchasedProduct(ReportBase):
    @property
    def name(self):
        return "sponsored_products_report_v3_purchased_product"

    @property
    def key_properties(self):
        return ["profileId", "countryCode", "campaignId", "adGroupId", "portfolioId", "keywordId", "date"]

    @property
    def metric_types(self):
        return [
            "purchases","purchasesOtherSku", # v2: attributedConversions
            "sales","salesOtherSku", # v2: attributedSales
            "unitsSoldClicks","unitsSoldOtherSku", # v2: attributedUnitsOrdered
        ]

    def get_configuration(self):
        return {
            "adProduct": "SPONSORED_PRODUCTS",
            "groupBy": ["asin"],
            "columns": [
                "date",
                "campaignName",
                "campaignId",
                "campaignBudgetCurrencyCode",
                "adGroupName",
                "adGroupId",
                "portfolioId",
                "keywordId",
                "keyword",
                "keywordType",
                "matchType",
                "advertisedAsin",
                "purchasedAsin",
                "advertisedSku",]
                 + self.gen_metrics_names(self.metric_types, base=[]),
            "reportTypeId": "spPurchasedProduct",
            "timeUnit": "DAILY",
            "format": "GZIP_JSON"
        }

# Targeting report
# targeting in v2: keywordType in ("TARGETING_EXPRESSION", "TARGETING_EXPRESSION_PREDEFINED")
# keyword in v2: keywordType in ("BROAD", "PHRASE", "EXACT")
class SponsoredProductsReportTargeting(ReportBase):
    @property
    def name(self):
        return "sponsored_products_report_v3_targeting"

    @property
    def key_properties(self):
        return ["profileId", "countryCode", "campaignId", "adGroupId", "portfolioId", "keywordId", "keywordType", "date"]

    @property
    def metric_types(self):
        return [
            "purchases","purchasesSameSku", # v2: attributedConversions
            "sales","attributedSalesSameSku", # v2: attributedSales
            "unitsSoldClicks","unitsSoldSameSku" # v2: attributedUnitsOrdered
        ]

    def get_configuration(self):
        return {
            "adProduct": "SPONSORED_PRODUCTS",
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
                "portfolioId",
                "keywordId",
                "keyword",
                "keywordType",
                "matchType",
                "targeting",]
                 + self.gen_metrics_names(self.metric_types),
            "reportTypeId":"spTargeting",
            "timeUnit": "DAILY",
            "format": "GZIP_JSON"
        }

