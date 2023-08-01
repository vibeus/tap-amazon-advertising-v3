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
from .dsp_report import DSPReport

def create_stream(stream_id):
    if stream_id == "sponsored_products_campaigns":
        return SponsoredProductsCampaigns()
    if stream_id == "sponsored_products_ad_groups":
        return SponsoredProductsAdGroups()
    if stream_id == "sponsored_products_product_ads":
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
    if stream_id == "dsp_report":
        return DSPReport()

    assert False, f"Unsupported stream: {stream_id}"
