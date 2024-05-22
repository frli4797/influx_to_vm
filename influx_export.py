#!/usr/bin/env python3
"""
 @author Johannes Aalto
 SPDX-License-Identifier: Apache-2.0
"""
import datetime
import logging
import os
import warnings
from typing import Iterable, Dict, List


import pandas as pd
import requests
from influxdb_client import InfluxDBClient
from influxdb_client.client.warnings import MissingPivotFunction

warnings.simplefilter("ignore", MissingPivotFunction)
no_errors = 0


# Create a custom logger
logger = logging.getLogger(__name__)

# Create handlers
c_handler = logging.StreamHandler()
f_handler = logging.FileHandler('migrator.log')
c_handler.setLevel(logging.INFO)
f_handler.setLevel(logging.DEBUG)

# Add handlers to the logger
logger.addHandler(c_handler)
logger.addHandler(f_handler)

try:
    from dotenv import load_dotenv

    load_dotenv(dotenv_path=".env")
except ImportError:
    pass


def get_tag_cols(dataframe_keys: Iterable) -> Iterable:
    """Filter out dataframe keys that are not tags"""
    return (
        k
        for k in dataframe_keys
        if not k.startswith("_") and k not in ["result", "table"]
    )


def get_influxdb_lines(df: pd.DataFrame) -> str:
    """
    Convert the Pandas Dataframe into InfluxDB line protocol.

    The dataframe should be similar to results received from query_api.query_data_frame()

    Not quite sure if this supports all kinds if InfluxDB schemas.
    It might be that influxdb_client package could be used as an alternative to this,
    but I'm not sure about the authorizations and such.

    Protocol description: https://docs.influxdata.com/influxdb/v2.0/reference/syntax/line-protocol/
    """
    logger.info(f"Exporting {df.columns}")

    # line = df["_measurement"]
    line = df["entity_id"]
    line = df["domain"] + "." + line
    # print(f"Entity: {entity_id}")
    for col_name in get_tag_cols(df):
        line += ("," + col_name + "=") + df[col_name].astype(str)

    line += ("," + "unit_of_measurement=") + df["_measurement"].astype(str)

    line += (
            " "
            + df["_field"]
            + "="
            + df["_value"].astype(str)
            + " "
            + df["_time"].astype(int).astype(str)
    )
    return "\n".join(line)


def main(args: Dict[str, str]):
    logger.info("args: " + str(args.keys()))
    bucket = args.pop("bucket")
    url = args.pop("vm_addr")

    for k, v in args.items():
        if v is not None:
            os.environ[k] = v
        logger.info(f"Using {k}={os.getenv(k)}")

    client = InfluxDBClient.from_env_properties()

    query_api = client.query_api()  # use synchronous to see errors

    now_datetime = datetime.datetime.now()
    # start_datetime = now_datetime - datetime.timedelta(days=365)
    start_datetime = datetime.datetime(2024,3,22,16,5,11)

    current_ep = int(now_datetime.timestamp())
    start_ep = int(start_datetime.timestamp())

    step_ep = int(2592000 / 8)  # One month
    now_datetime_str = now_datetime.strftime("%Y%m%d%H%M%S")
    with open(f".migrator_{now_datetime_str}", 'w') as file:

        for current_ep in range(start_ep, current_ep, step_ep):
            loop_date = datetime.datetime.fromtimestamp(current_ep)
            loop_end_date = datetime.datetime.fromtimestamp(current_ep + step_ep)
            logger.info(f"Starting segment {loop_date} to {loop_end_date}")

            # Get all unique series by reading first entry of every table.
            # With latest InfluxDB we could possibly use "schema.measurements()" but this doesn't exist in 2.0
            first_in_series = f"""
            from(bucket: "{bucket}")
            |> range(start: {current_ep}, stop: {current_ep + step_ep})
            |> first()"""
            timeseries: List[pd.DataFrame] = query_api.query_data_frame(first_in_series)

            if len(timeseries) < 1:
                logger.debug("Skipping")
                continue

            # get all unique measurement-field pairs and then fetch and export them one-by-one.
            # With really large databases the results should be possibly split further
            # Something like query_data_frame_stream() might be then useful.

            measurements_and_fields = []
            for df in timeseries:
                for gr in df.groupby(["entity_id", "_field"]):
                    measurements_and_fields.append(gr[0])

            whitelist = []
            whitelist_path = "whitelist.txt"
            if os.path.exists(whitelist_path):
                try:
                    with open(whitelist_path, 'r') as f:
                        whitelist = f.read().splitlines()
                except OSError:
                    logger.debug("Problem reading whitelist. Skipping")

            m_a_f_set = set(measurements_and_fields)
            whitelist_set = set(whitelist)
            intersection = m_a_f_set & whitelist_set


            migrate_segment(bucket, query_api, current_ep, step_ep, measurements_and_fields, file, url)

    # Closing result file.


def migrate_segment(bucket, query_api, current_ep, step_ep, measurements_and_fields, result_file, victoriametrics_url):
    logger.info(f"Found {len(measurements_and_fields)} unique time series")

    field_no = 1
    for meas, field in measurements_and_fields:
        try:
            logger.debug(
                f"Exporting ({field_no}/{len(measurements_and_fields)}) {meas}_{field} "
                f"from {datetime.datetime.fromtimestamp(current_ep)} "
                f"until {datetime.datetime.fromtimestamp(current_ep + step_ep)}")
            whole_series = f"""
                    from(bucket: "{bucket}")
                    |> range(start: {current_ep}, stop: {current_ep + step_ep})
                    |> filter(fn: (r) => r["entity_id"] == "{meas}")
                    |> filter(fn: (r) => r["_field"] == "{field}")
                    """
            field_no += 1
            df = query_api.query_data_frame(whole_series)

            line = get_influxdb_lines(df)
            no_lines = line.count("\n")
            # "db" is added as an extra tag for the value.
            logger.info(f"({field_no}/{len(measurements_and_fields)}) Writing {no_lines} lines to VictoriaMetrics db={bucket}")

            requests.post(f"{victoriametrics_url}/write?db={bucket}", data=line)
            result_file.write(f"+ {meas}\t{field}\t{current_ep}\n")
        except Exception as err:
            logger.error(f"Failed reading or writing {meas} {field}")
            global no_errors
            no_errors += 1
            result_file.write(f"- {meas}\t{field}\t{current_ep}\n")
            if no_errors > 10:
                print("Too many errors. Bailing")
                logger.fatal("Too many errors. Bailing")
                exit(500)
        # EO Try/catch
        result_file.flush()



if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Script for exporting InfluxDB data into victoria metrics instance. \n"
                    " InfluxDB settings can be defined on command line or as environment variables"
                    " (or in .env file if python-dotenv is installed)."
                    " InfluxDB related args described in \n"
                    "https://github.com/influxdata/influxdb-client-python#via-environment-properties"
    )
    parser.add_argument(
        "bucket",
        type=str,
        help="InfluxDB source bucket",
    )
    parser.add_argument(
        "--INFLUXDB_V2_ORG",
        "-o",
        type=str,
        help="InfluxDB organization",
    )
    parser.add_argument(
        "--INFLUXDB_V2_URL",
        "-u",
        type=str,
        help="InfluxDB Server URL, e.g., http://localhost:8086",
    )
    parser.add_argument(
        "--INFLUXDB_V2_TOKEN",
        "-t",
        type=str,
        help="InfluxDB access token.",
    )
    parser.add_argument(
        "--INFLUXDB_V2_SSL_CA_CERT",
        "-S",
        type=str,
        help="Server SSL Cert",
    )
    parser.add_argument(
        "--INFLUXDB_V2_TIMEOUT",
        "-T",
        type=str,
        help="InfluxDB timeout",
    )
    parser.add_argument(
        "--INFLUXDB_V2_VERIFY_SSL",
        "-V",
        type=str,
        help="Verify SSL CERT.",
    )

    parser.add_argument(
        "--vm-addr",
        "-a",
        type=str,
        help="VictoriaMetrics server",
    )
    main(vars(parser.parse_args()))
