# tap-amazon-ads (Version 3)

This is a [Singer][1] tap that produces JSON-formatted data following the [Singer spec][2].

This tap:

- Pulls raw data from [Amazon Ads API][3] Version 3
- Extracts the following resources:
    - SponsoredProductsCampaigns
    - SponsoredProductsAdGroups
    - SponsoredProductsReportCampaigns
    - SponsoredProductsReportAdGroups
    - SponsoredProductsReportSearchTerm
    - SponsoredProductsReportPurchasedProduct
    - SponsoredProductsReportTargeting

- Outputs the schema for each resource
- Incrementally pulls data based on the input state

## Install

```bash
git clone git@github.com:vibeus/tap-amazon-advertising-v3.git
cd tap-amazon-advertising-v3
pip install .
```
## Usage

1. Follow [Singer.io Best Practices][5] for setting up separate `tap` and `target` virtualenvs to avoid version conflicts.
2. Create a config file ```~/config.json``` with Amazon API credentials. Multiple accounts across
   different marketplaces are supported.
    ```json
    [
      {
        "client_id": "CLIENT_ID",
        "client_secret": "LWA_CLIENT_SECRET",
        "refresh_token": "REFRESH_TOKEN",
        "redirect_uri": "URL",
        "start_date": "2020-01-01",
        "region": "NA",
        "profiles": [{
          "profile_id": "US_PROFILE_ID",
          "country_code": "US"
        }, {
          "profile_id": "CA_PROFILE_ID",
          "country_code": "CA"
        }]
      }
    ]
    ```
3. Discover catalog: `tap-amazon-ads -c config.json -d > catalog.json`
4. Select stream in the generated `catalog.json`.
    ```
    ...
    "stream": "sponsored_products_report_v3_ad_groups",
    "metadata": [
      {
        "breadcrumb": [],
        "metadata": {
          "table-key-properties": [
            "profileId",
            "countryCode",
            "campaignId",
            "adGroupId",
            "date"
          ],
          "forced-replication-method": "INCREMENTAL",
          "valid-replication-keys": [
            "date"
          ],
          "selected": true  <-- Somewhere in the huge catalog file, in stream metadata.
        }
      },
      ...
    ]
    ...
    ```
5. Use following command to sync all orders with order items, buyer info and shipping address (when available).
```bash
tap-amazon-ads -c config.json --catalog catalog.json > output.txt
```

---

Copyright &copy; 2023 Vibe Inc

[1]: https://singer.io
[2]: https://github.com/singer-io/getting-started/blob/master/SPEC.md
[3]: https://advertising.amazon.com/API/docs/en-us/info/api-overview
[4]: https://github.com/singer-io/getting-started/blob/master/docs/RUNNING_AND_DEVELOPING.md#running-a-singer-tap-with-a-singer-target