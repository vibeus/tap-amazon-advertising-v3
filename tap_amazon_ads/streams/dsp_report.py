import singer
import json
import time
from singer import metadata

from datetime import datetime, timedelta, date
from dateutil.parser import parse

import zlib

import requests
from requests.exceptions import RequestException, HTTPError
import requests_oauthlib

from .base import Base

LOGGER = singer.get_logger()
DEFAULT_BACKOFF_SECONDS = 60
MAX_TRIES = 7
LOOKBACK = 30

# use BASE_URL[config['region']]
BASE_URL = {
    'NA': 'https://advertising-api.amazon.com',
    'EU': 'https://advertising-api-eu.amazon.com',
    'FE': 'https://advertising-api-fe.amazon.com'
}

TOKEN_URL = 'https://api.amazon.com/auth/o2/token'
SCOPES = ["advertising::campaign_management"]

class DSPReport(Base):
    @property
    def name(self):
        return "dsp_report"

    @property
    def key_properties(self):
        return ['date', 'accountId', 'advertiserId', 'orderId', 'lineItemId', 'creativeAdId', 'creativeID']

    @property
    def replication_key(self):
        return "date"

    @property
    def replication_method(self):
        return "INCREMENTAL"

    @property
    def accept_type(self):
        return "application/vnd.dspcreatereports.v3+json"

    @property
    def content_type(self):
        return "application/json"

    def api_path(self, account_id):
        return f"/accounts/{account_id}/dsp/reports"

    def get_configuration(self):
        return [
                "impressions",
                "eCPM",
                "viewableImpressions",
                "measurableImpressions",
                "measurableRate",
                "viewabilityRate",
                "clickThroughs",
                "CTR",
                "eCPC",
                "eRPM14d",
                "grossClickThroughs",
                "grossImpressions",
                "invalidClickThroughs",
                "invalidImpressions",
                "invalidClickThroughsRate",
                "invalidImpressionRate",
                "supplyCost",
                "agencyFee",
                "amazonAudienceFee",
                "amazonPlatformFee",
                "totalFee",
                "totalCost",
                "totalERPM14d",
                "unitsSold14d",
                "sales14d",
                "purchases14d",
                "newToBrandProductSales14d",
                "newToBrandPurchaseRate14d",
                "newToBrandPurchases14d",
                "totalDetailPageClicks14d",
                "totalDetailPageViews14d",
                "totalDetailPageViewsCVR14d",
                "totalDetailPageViewViews14d",
                "totalNewToBrandPurchases14d",
                "totalPurchases14d",
                "totalPurchasesClicks14d",
                "totalPurchasesViews14d",
                "totalSales14d",
                "totalUnitsSold14d",
            ]

    def get_tap_data(self, configs, state):
        for config in configs:
            if not config.get("for_dsp", False):
                LOGGER.info(f"Skipping: not a DSP reporting config")
                continue

            access_token = self.get_authorization(config)
            request_headers = {
                "Accept": self.accept_type,
                "Content-Type": self.content_type,
                "Amazon-Advertising-API-ClientId": config['client_id'],
                "Authorization": "Bearer " + access_token,
            }
            wait_headers = {
                "Accept": "application/vnd.dspgetreports.v3+json",
                "Content-Type": "application/vnd.dspgetreports.v3+json",
                "Amazon-Advertising-API-ClientId": config['client_id'],
                "Authorization": "Bearer " + access_token,
            }

            today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
            self._start_date = config.get("start_date", today) # config start date
            self._backoff_seconds = config.get("rate_limit_backoff_seconds", DEFAULT_BACKOFF_SECONDS)
            self._state = state.copy()

            for profile in config["profiles"]:
                yield from self.get_stream_data(profile, request_headers, wait_headers, BASE_URL[config['region']])

    def get_stream_data(self, profile, request_headers, wait_headers, api_base):
        yesterday = datetime.utcnow() - timedelta(days=1)
        WINDOW_start = datetime.utcnow() - timedelta(days=89)

        state_date = self._state.get(profile['country_code'], self._start_date) # state start date

        start = max(WINDOW_start, parse(state_date) - timedelta(days=LOOKBACK))
        end = min(start + timedelta(days=30), yesterday)

        max_rep_key = start
        counter = 0
        while start < yesterday and counter < MAX_TRIES:
            try:
                LOGGER.info(f"start snycing {self.name} from {start.date().isoformat()} to {end.date().isoformat()} for {profile['country_code']}")
                # Create a report
                LOGGER.info(f"{api_base}{self.api_path(profile['account_id'])}")
                resp = requests.post(url=f"{api_base}{self.api_path(profile['account_id'])}", headers=request_headers,
                                     json={"dimensions": ["ORDER", "LINE_ITEM", "CREATIVE"], "timeUnit": "DAILY",
                                           "startDate": start.date().isoformat(), "endDate": end.date().isoformat(),
                                           "metrics": self.get_configuration() })
                if not (resp.status_code >= 200 and resp.status_code <= 299):
                    LOGGER.warning(f"{self.name} create request error: {resp.status_code}: {resp.text}")
                    if resp.status_code == 429:
                        LOGGER.info(f"Waiting 60 seconds...")
                        time.sleep(60)
                    counter += 1
                    continue

                report_checking_url = f"{api_base}{self.api_path(profile['account_id'])}/{resp.json()['reportId']}"
                LOGGER.info(report_checking_url)
                time.sleep(60) # wait 15 second for report processing

                counter = 0
                doc = None
                for i in range(MAX_TRIES):
                    report_info = requests.get(url=report_checking_url, headers=wait_headers)
                    if report_info.status_code != 200:
                        LOGGER.warning(f"{self.name} info request error: {report_info.status_code}: {report_info.text}")
                        continue
                    if report_info.json()["status"] == "SUCCESS":
                        doc = requests.get(url=report_info.json()["location"]).json()
                        LOGGER.info(f"{self.name} from {start.date().isoformat()} to {end.date().isoformat()} for {profile['country_code']} is generated...")
                        break
                    if report_info.json()["status"] == "CANCELLED":
                        LOGGER.warning(f"{self.name} from {start.date().isoformat()} to {end.date().isoformat()} for {profile['country_code']} is cancelled...")
                        break
                    else:
                        timeout = (i + 2) ** 2
                        LOGGER.warning(f"report is {report_info.json()['status']}. Waiting {timeout} seconds...")
                        time.sleep(timeout)
                if doc:
                    for row in doc:
                        for col in ['date', 'orderStartDate', 'orderEndDate', 'lineItemStartDate', 'lineItemEndDate']:
                            row[col] = datetime.fromtimestamp(int(row[col])/1000)
                        rep_key = row.get(self.replication_key)
                        # if rep_key and rep_key > max_rep_key:
                        #     max_rep_key = rep_key

                        if rep_key and rep_key > max_rep_key:
                            max_rep_key = rep_key

                        row["accountId"] = profile["account_id"]
                        row["countryCode"] = profile["country_code"]

                        yield row

                start = end + timedelta(days=1)
                end = min(start + timedelta(days=30), yesterday)

            except RequestException as e:
                LOGGER.warning(f"{e} Waiting {self._backoff_seconds} seconds...")
                time.sleep(self._backoff_seconds)

        self._state[profile["country_code"]] = max_rep_key.isoformat()