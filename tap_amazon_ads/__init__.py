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

def parse_args_list(required_config_keys):
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

    parsed_args = parser.parse_args()

    if parsed_args.config:
        setattr(parsed_args, 'config_path', parsed_args.config)
        config_list = singer.utils.load_json(parsed_args.config)
        if not isinstance(config_list, list):
            raise Exception("Config SHOULD be a LIST!")

    args_list = []
    for config in config_list:
        singer.utils.check_config(config, required_config_keys)
        new_arg = Namespace(config=config)

        if parsed_args.state:
            setattr(parsed_args, 'state_path', parsed_args.state)
            new_arg.state = singer.utils.load_json(parsed_args.state)
        else:
            new_arg.state = {}
        if parsed_args.properties:
            setattr(parsed_args, 'properties_path', parsed_args.properties)
            new_arg.properties = singer.utils.load_json(parsed_args.properties)
        if parsed_args.catalog:
            setattr(parsed_args, 'catalog_path', parsed_args.catalog)
            new_arg.catalog = Catalog.load(parsed_args.catalog)
        else:
            new_arg.catalog = parsed_args.catalog

        new_arg.discover = parsed_args.discover

        args_list.append(new_arg)

    return args_list

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
        if "report" in stream_id:
            for col in stream.gen_metrics_names(stream.metric_types):
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


def sync(config, state, catalog, global_state_dict):
    """Sync data from tap source"""

    # state_dict = {}

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
        for row in stream.get_tap_data(config, stream_state):
            schema = catalog_stream.schema.to_dict()
            mdata = metadata.to_map(catalog_stream.metadata)
            record = t.transform(row, schema, mdata)

            singer.write_records(stream_id, [record])

        if stream_id in global_state_dict:
            global_state_dict[stream_id].update(stream.state)
        else:
            global_state_dict[stream_id] = stream.state
        singer.write_state(global_state_dict)


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args_list = parse_args_list(required_config_keys=REQUIRED_CONFIG_KEYS)

    global_state_dict = args_list[0].state
    for args in args_list:
        # If discover flag was passed, run discovery mode and dump output to stdout
        if args.discover:
            catalog = discover()
            catalog.dump()
            break
        # Otherwise run in sync mode
        else:
            if args.catalog:
                catalog = args.catalog
            else:
                catalog = discover()

            args.config = expand_env(args.config)
            sync(args.config, args.state, catalog, global_state_dict)
    # singer.write_state(global_state_dict)


if __name__ == "__main__":
    main()
