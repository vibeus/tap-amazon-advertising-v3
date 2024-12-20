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

class Base:
    def __init__(self):
        self._start_date = ""
        self._state = {}

    @property
    def name(self):
        return "base_stream"

    @property
    def key_properties(self):
        return ['id']

    @property
    def replication_key(self):
        return None

    @property
    def replication_method(self):
        return "FULL_TABLE"

    @property
    def state(self):
        return self._state

    @property
    def api_path(self):
        return "/"

    @property
    def accept_type(self):
        return None

    @property
    def content_type(self):
        return "application/json"

    @property
    def list_name(self):
        return None

    @property
    def request_method(self):
        return 'POST'

    def get_metadata(self, schema):
        mdata = metadata.get_standard_metadata(
            schema=schema,
            key_properties=self.key_properties,
            valid_replication_keys=[self.replication_key],
            replication_method=self.replication_method,
        )

        return mdata

    def get_authorization(self, config):
        client_id = config.get('client_id')
        oauth = requests_oauthlib.OAuth2Session(
                    client_id,
                    redirect_uri=config.get('redirect_uri'),
                    scope=SCOPES)

        tokens = oauth.refresh_token(
                    TOKEN_URL,
                    refresh_token=config.get('refresh_token'),
                    client_id=config.get('client_id'),
                    client_secret=config.get('client_secret'))

        return tokens['access_token']

    def get_tap_data(self, configs, state):
        for config in configs:
            if config.get("for_dsp", False):
                LOGGER.info(f"Skipping: DSP reporting config")
                continue

            base_headers = {
                "Content-Type": self.content_type,
                "Amazon-Advertising-API-ClientId": config['client_id'],
                "Authorization": "Bearer " + self.get_authorization(config),
            }

            if self.accept_type:
                base_headers.update({"Accept": self.accept_type})

            self._backoff_seconds = config.get("rate_limit_backoff_seconds", DEFAULT_BACKOFF_SECONDS)

            for profile in config["profiles"]:
                headers = base_headers.copy()
                headers["Amazon-Advertising-API-Scope"] = profile['profile_id']
                yield from self.get_stream_data(profile, headers, BASE_URL[config['region']])

    def get_stream_data(self, profile, headers, api_base):
        LOGGER.info(f"start syncing {self.name} for {profile['country_code']}")
        next_token = None
        counter = 0
        while True and counter < MAX_TRIES:
            try:
                if not next_token:
                    resp = requests.request(method=self.request_method, url=f"{api_base}{self.api_path}", headers=headers)
                else:
                    resp = requests.request(method=self.request_method,url=f"{api_base}{self.api_path}", headers=headers,
                                            json={"nextToken": next_token})

                if resp.status_code != 200:
                    LOGGER.warning(f"{resp.text}")
                    counter += 1
                    continue

                resp = resp.json()
                counter = 0
                if self.list_name and type(resp) != list:
                    list_res = resp[self.list_name]
                else:
                    list_res = resp

                for row in list_res:
                    row["profileId"] = profile["profile_id"]
                    row["countryCode"] = profile["country_code"]
                    yield row

                if 'nextToken' in resp:
                    next_token = resp["nextToken"]
                else:
                    break

            except RequestException as e:
                LOGGER.warning(f"{e} Waiting {self._backoff_seconds} seconds...")
                time.sleep(self._backoff_seconds)


class ReportBase(Base):
    @property
    def key_properties(self):
        return ['date', 'profileId']

    @property
    def replication_key(self):
        return "date"

    @property
    def replication_method(self):
        return "INCREMENTAL"

    @property
    def api_path(self):
        return "/reporting/reports"

    @property
    def content_type(self):
        return "application/vnd.createasyncreportrequest.v3+json"

    @property
    def metric_types(self):
        return []

    @property
    def conversion_window(self):
        return [1, 7, 14, 30]

    def get_data_retention(self):
        if 'sponsored_products' in self.name:
            return 95
        if 'sponsored_brands' in self.name:
            return 60
        if 'sponsored_display' in self.name:
            return 65
        if 'attribution_report' in self.name:
            return 365

    def gen_metrics_names(self, metric_types=[], conversion_window=[1, 7, 14, 30], base=["impressions","clicks","cost"]):
        cols = base.copy()
        for metric_type in metric_types:
            if conversion_window:
                for window in conversion_window:
                    cols += [f"{metric_type}{window}d"]
            else:
                cols += [metric_type]
        return cols

    def get_configuration(self):
        return {}

    def get_body(self, start, end):
        return {
            "startDate": start.date().isoformat(),
            "endDate": end.date().isoformat(),
            "configuration": self.get_configuration()
        }

    def get_report_document(self, report_url):
        document = requests.get(url=report_url).content
        extracted = zlib.decompress(document, 16+zlib.MAX_WBITS)
        decoded = extracted.decode('utf-8')
        return json.loads(decoded)

    def transform_date_format(self, date):
        return date

    def get_tap_data(self, configs, state):
        today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0).isoformat()

        self._state = state.copy()

        for config in configs:
            if config.get("for_dsp", False):
                LOGGER.info(f"Skipping: DSP reporting config")
                continue

            self._start_date = config.get("start_date", today) # config start date
            self._backoff_seconds = config.get("rate_limit_backoff_seconds", DEFAULT_BACKOFF_SECONDS)

            base_headers = {
                "Content-Type": self.content_type,
                "Amazon-Advertising-API-ClientId": config['client_id'],
                "Authorization": "Bearer " + self.get_authorization(config),
            }

            for profile in config["profiles"]:
                headers = base_headers.copy()
                headers["Amazon-Advertising-API-Scope"] = profile['profile_id']
                yield from self.get_stream_data(profile, headers, BASE_URL[config['region']])

    def get_stream_data(self, profile, headers, api_base):
        yesterday = datetime.utcnow() - timedelta(days=1)

        WINDOW_start = datetime.utcnow() - timedelta(days=self.get_data_retention())

        state_date = self._state.get(profile['country_code'], self._start_date) # state start date

        start = max(WINDOW_start, parse(state_date) - timedelta(days=LOOKBACK))
        end = min(start + timedelta(days=30), yesterday)

        max_rep_key = start
        counter = 0
        skipFlag = False
        while start < yesterday and counter < MAX_TRIES and not skipFlag:
            try:
                LOGGER.info(f"start snycing {self.name} from {start.date().isoformat()} to {end.date().isoformat()} for {profile['country_code']}")
                # Create a report
                body = {"startDate": start.date().isoformat(), "endDate": end.date().isoformat(),
                                            "configuration": self.get_configuration()}
                resp = requests.post(url=f"{api_base}{self.api_path}", headers=headers,
                                        json=self.get_body(start, end))
                if resp.status_code != 200:
                    LOGGER.warning(f"{self.name} create request error: {resp.text}")
                    counter += 1
                    continue

                time.sleep(15) # wait 15 second for report processing

                counter = 0
                doc = resp.json().get("reports", None)

                if not doc:
                    for i in range(MAX_TRIES):
                        report_info = requests.get(url='{}/reporting/reports/{}'.format(api_base, resp.json()['reportId']), headers=headers)
                        if report_info.status_code != 200:
                            LOGGER.warning(f"{self.name} info request error: {report_info.text}")
                            continue
                        if report_info.json()["status"] == "COMPLETED":
                            doc = self.get_report_document(report_info.json()["url"])
                            LOGGER.info(f"{self.name} from {start.date().isoformat()} to {end.date().isoformat()} for {profile['country_code']} is generated...")
                            break
                        if report_info.json()["status"] == "CANCELLED":
                            LOGGER.warning(f"{self.name} from {start.date().isoformat()} to {end.date().isoformat()} for {profile['country_code']} is cancelled...")
                            break
                        else:
                            timeout = (i + 2) ** 2
                            LOGGER.warning(f"report is {report_info.json()['status']}. Waiting {timeout} seconds...")
                            time.sleep(timeout)

                        if i == MAX_TRIES - 1 and report_info.json()["status"] != "COMPLETED":
                            LOGGER.warning(f"skipping {self.name} due to waiting too long...")
                            skipFlag = True

                if doc:
                    for row in doc:
                        rep_key = row.get(self.replication_key)
                        if rep_key and parse(rep_key) > max_rep_key:
                            max_rep_key = parse(rep_key)

                        row["profileId"] = profile["profile_id"]
                        row["countryCode"] = profile["country_code"]
                        row['date'] = self.transform_date_format(row['date'])
                        yield row

                start = end + timedelta(days=1)
                end = min(start + timedelta(days=30), yesterday)

            except RequestException as e:
                LOGGER.warning(f"{e} Waiting {self._backoff_seconds} seconds...")
                time.sleep(self._backoff_seconds)

        self._state[profile["country_code"]] = max_rep_key.isoformat()
        LOGGER.info(f'{self.name}, {profile["country_code"]}, {max_rep_key.isoformat()}')