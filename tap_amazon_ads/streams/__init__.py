from .sponsored_products import \
                SponsoredProductsCampaigns, \
                SponsoredProductsAdGroups, \
                SponsoredProductsProductAds
from .sponsored_products_report import \
                SponsoredProductsReportCampaigns, \
                SponsoredProductsReportAdGroups, \
                SponsoredProductsReportSearchTerm, \
                SponsoredProductsReportPurchasedProduct, \
                SponsoredProductsReportAdvertisedProduct, \
                SponsoredProductsReportTargeting
from .sponsored_brands import \
                SponsoredBrandsCampaigns, \
                SponsoredBrandsAdGroups
from .sponsored_brands_report import \
                SponsoredBrandsReportCampaigns, \
                SponsoredBrandsReportAdGroups, \
                SponsoredBrandsReportSearchTerm, \
                SponsoredBrandsReportPurchasedProduct, \
                SponsoredBrandsReportTargeting
from .sponsored_display import \
                SponsoredDisplayCampaigns, \
                SponsoredDisplayAdGroups
from .sponsored_display_report import \
                SponsoredDisplayReportCampaigns, \
                SponsoredDisplayReportAdGroups, \
                SponsoredDisplayReportPurchasedProduct, \
                SponsoredDisplayReportTargeting

from .dsp_report import DSPReport
from .attribution_report import AttributionReportCampaignPerformance

def create_stream(stream_id):
    if stream_id == "sponsored_products_campaigns_v3":
        return SponsoredProductsCampaigns()
    if stream_id == "sponsored_products_ad_groups_v3":
        return SponsoredProductsAdGroups()
    if stream_id == "sponsored_products_product_ads_v3":
        return SponsoredProductsProductAds()

    if stream_id == "sponsored_products_report_v3_campaigns":
        return SponsoredProductsReportCampaigns()
    if stream_id == "sponsored_products_report_v3_ad_groups":
        return SponsoredProductsReportAdGroups()
    if stream_id == "sponsored_products_report_v3_search_term":
        return SponsoredProductsReportSearchTerm()
    if stream_id == "sponsored_products_report_v3_purchased_product":
        return SponsoredProductsReportPurchasedProduct()
    if stream_id == "sponsored_products_report_v3_advertised_product":
        return SponsoredProductsReportAdvertisedProduct()
    if stream_id == "sponsored_products_report_v3_targeting":
        return SponsoredProductsReportTargeting()

    if stream_id == "sponsored_brands_campaigns_v3":
        return SponsoredBrandsCampaigns()
    if stream_id == "sponsored_brands_ad_groups_v3":
        return SponsoredBrandsAdGroups()

    if stream_id == "sponsored_brands_report_v3_campaigns":
        return SponsoredBrandsReportCampaigns()
    if stream_id == "sponsored_brands_report_v3_ad_groups":
        return SponsoredBrandsReportAdGroups()
    if stream_id == "sponsored_brands_report_v3_search_term":
        return SponsoredBrandsReportSearchTerm()
    if stream_id == "sponsored_brands_report_v3_purchased_product":
        return SponsoredBrandsReportPurchasedProduct()
    if stream_id == "sponsored_brands_report_v3_targeting":
        return SponsoredBrandsReportTargeting()

    if stream_id == "sponsored_display_campaigns_v3":
        return SponsoredDisplayCampaigns()
    if stream_id == "sponsored_display_ad_groups_v3":
        return SponsoredDisplayAdGroups()

    if stream_id == "sponsored_display_report_v3_campaigns":
        return SponsoredDisplayReportCampaigns()
    if stream_id == "sponsored_display_report_v3_ad_groups":
        return SponsoredDisplayReportAdGroups()
    if stream_id == "sponsored_display_report_v3_purchased_product":
        return SponsoredDisplayReportPurchasedProduct()
    if stream_id == "sponsored_display_report_v3_targeting":
        return SponsoredDisplayReportTargeting()

    if stream_id == "dsp_report":
        return DSPReport()
    if stream_id == "attribution_report_campaign_performance":
        return AttributionReportCampaignPerformance()

    assert False, f"Unsupported stream: {stream_id}"
