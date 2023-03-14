from .sponsored_products import \
                SponsoredProductsCampaigns, \
                SponsoredProductsAdGroups
from .sponsored_products_report import \
                SponsoredProductsReportCampaigns, \
                SponsoredProductsReportAdGroups, \
                SponsoredProductsReportSearchTerm, \
                SponsoredProductsReportPurchasedProduct, \
                SponsoredProductsReportTargeting


def create_stream(stream_id):
    if stream_id == "sponsored_products_campaigns":
        return SponsoredProductsCampaigns()
    if stream_id == "sponsored_products_ad_groups":
        return SponsoredProductsAdGroups()
    if stream_id == "sponsored_products_report_v3_campaigns":
        return SponsoredProductsReportCampaigns()
    if stream_id == "sponsored_products_report_v3_ad_groups":
        return SponsoredProductsReportAdGroups()
    if stream_id == "sponsored_products_report_v3_search_term":
        return SponsoredProductsReportSearchTerm()
    if stream_id == "sponsored_products_report_v3_purchased_product":
        return SponsoredProductsReportPurchasedProduct()
    if stream_id == "sponsored_products_report_v3_targeting":
        return SponsoredProductsReportTargeting()

    assert False, f"Unsupported stream: {stream_id}"
