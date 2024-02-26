#!/usr/bin/env python3
import os
import re
import json

import argparse
from argparse import Namespace

import singer
from singer import utils, metadata, Transformer
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema
from tap_amazon_ads.streams import create_stream


REQUIRED_CONFIG_KEYS = ["client_id", "client_secret", "refresh_token", "redirect_uri", "start_date", "region", "profiles"]
LOGGER = singer.get_logger()

def parse_args_multiconfigs(required_config_keys):
    '''Parse standard command-line args.
    Parses the command-line arguments mentioned in the SPEC and the
    BEST_PRACTICES documents:
    -c,--config     Config file
    -s,--state      State file
    -d,--discover   Run in discover mode
    -p,--properties Properties file: DEPRECATED, please use --catalog instead
    --catalog       Catalog file
    Returns the parsed args object from argparse. For each argument that
    point to JSON files (config, state, properties), we will automatically
    load and parse the JSON file.
    '''
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-c', '--config',
        help='Config file',
        required=True)

    parser.add_argument(
        '-s', '--state',
        help='State file')

    parser.add_argument(
        '-p', '--properties',
        help='Property selections: DEPRECATED, Please use --catalog instead')

    parser.add_argument(
        '--catalog',
        help='Catalog file')

    parser.add_argument(
        '-d', '--discover',
        action='store_true',
        help='Do schema discovery')

    args = parser.parse_args()

    if args.config:
        setattr(args, 'config_path', args.config)
        args.config = singer.utils.load_json(args.config)
        if not isinstance(args.config, list):
            raise Exception("Config SHOULD be a LIST!")

    if args.state:
        setattr(args, 'state_path', args.state)
        args.state = singer.utils.load_json(args.state)
    else:
        args.state = {}
    if args.properties:
        setattr(args, 'properties_path', args.properties)
        args.properties = singer.utils.load_json(args.properties)
    if args.catalog:
        setattr(args, 'catalog_path', args.catalog)
        args.catalog = Catalog.load(args.catalog)

    for config in args.config:
        singer.utils.check_config(config, required_config_keys)

    return args


def expand_env(config):
    assert isinstance(config, dict)

    def repl(match):
        env_key = match.group(1)
        return os.environ.get(env_key, "")

    def expand(v):
        assert not isinstance(v, dict)
        if isinstance(v, str):
            return re.sub(r"env\[(\w+)\]", repl, v)
        else:
            return v

    copy = {}
    for k, v in config.items():
        if isinstance(v, dict):
            copy[k] = expand_env(v)
        elif isinstance(v, list):
            copy[k] = [expand_env(x) if isinstance(x, dict) else expand(x) for x in v]
        else:
            copy[k] = expand(v)

    return copy


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schemas():
    """Load schemas from schemas folder"""
    schemas = {}
    for filename in os.listdir(get_abs_path("schemas")):
        path = get_abs_path("schemas") + "/" + filename
        file_raw = filename.replace(".json", "")
        with open(path) as file:
            schemas[file_raw] = json.load(file)
    return schemas


def discover():
    raw_schemas = load_schemas()
    streams = []
    for stream_id, schema_dict in raw_schemas.items():
        stream = create_stream(stream_id)
        if "report" in stream_id and stream_id != 'dsp_report':
            for col in stream.gen_metrics_names(stream.metric_types, conversion_window=stream.conversion_window):
                schema_dict["properties"][col] = {
                    "type": ["null", "string"]
                }

        schema = Schema.from_dict(schema_dict)

        streams.append(
            CatalogEntry(
                tap_stream_id=stream_id,
                stream=stream_id,
                schema=schema,
                key_properties=stream.key_properties,
                metadata=stream.get_metadata(schema.to_dict()),
                replication_key=stream.replication_key,
                replication_method=stream.replication_method,
                is_view=None,
                database=None,
                table=None,
                row_count=None,
                stream_alias=None,
            )
        )
    return Catalog(streams)


def sync(configs, state, catalog):
    """Sync data from tap source"""

    for catalog_stream in catalog.get_selected_streams(state):
        stream_id = catalog_stream.tap_stream_id
        LOGGER.info("Syncing stream:" + stream_id)

        singer.write_schema(
            stream_name=stream_id,
            schema=catalog_stream.schema.to_dict(),
            key_properties=catalog_stream.key_properties,
        )

        stream = create_stream(stream_id)
        stream_state = state.get(stream_id, {})

        t = Transformer()
        for row in stream.get_tap_data(configs, stream_state):
            schema = catalog_stream.schema.to_dict()
            mdata = metadata.to_map(catalog_stream.metadata)
            record = t.transform(row, schema, mdata)

            singer.write_records(stream_id, [record])

        state.update({stream_id: stream.state})
        LOGGER.info(f'updated state: {state}')
        singer.write_state(state)


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = parse_args_multiconfigs(REQUIRED_CONFIG_KEYS)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        catalog.dump()
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover()

        args.config = [expand_env(config) for config in args.config]
        sync(args.config, args.state, catalog)

if __name__ == "__main__":
    main()
