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
Chafrom influxdb_client import InfluxDBClient, QueryApi
from influxdb_client.client.warnings import MissingPivotFunction

warnings.simplefilter("ignore", MissingPivotFunction)
no_errors = 0

# Create a custom logger
logger = logging.getLogger(__name__)
logging.basicConfig(filename='migrator.log', encoding='utf-8', level=logging.DEBUG,
                    format='%(asctime)s - %(name)s - %(levelname)s -  %(message)s')

# noinspection PyUnresolvedReferences
try:
    import dotenv

    dotenv.load_dotenv(dotenv_path=".env")
except ImportError as err:
    pass


def _whitelist_measurements(measurements_and_fields: List[tuple]) -> List[tuple]:
    """
    Applies a whitelist to the list of measurements and fields. Does nothing if no whitelist is found.

    :param measurements_and_fields :
    :return:  the new measurements and fields tuple list with the whitelist applied.
    """
    whitelist: List[tuple] = []
    whitelist_path = "whitelist.txt"
    if os.path.exists(whitelist_path):
        try:
            with open(whitelist_path, 'r') as f:
                whitelist_rows = f.read().splitlines()

                for row_str in whitelist_rows:
                    row = row_str.split(' ')
                    if len(row) > 3:
                        tup: tuple = row[1], row[2]
                        whitelist.append(tup)
        except OSError:
            logger.warning("Problem reading whitelist. Skipping")

        if len(whitelist) > 0:
            m_a_f_set = set(measurements_and_fields)
            whitelist_set = set(whitelist)
            measurements_and_fields = list(set.intersection(m_a_f_set, whitelist_set))

    return measurements_and_fields


class InfluxMigrator:
    _from_dt: datetime
    _to_dt: datetime = datetime.datetime.now()
    _bucket_name: str
    _query_api: QueryApi
    _vm_url: str = ""

    def __init__(self, from_datetime: datetime, to_datetime: datetime, bucket_name: str, vm_url: str):
        self._from_dt = from_datetime
        self._to_dt = to_datetime
        self._bucket_name = bucket_name
        self._vm_url: str = vm_url

        now_datetime_str = from_datetime.strftime("%Y%m%d%H%M%S")
        self._progress_file = open(f".migrator_{now_datetime_str}", 'w')

    def __del__(self):
        self._progress_file.close()

    def influx_connect(self):
        client = InfluxDBClient.from_env_properties()
        self._query_api = client.query_api()

    def migrate(self, bucket: str):
        _current_ep = int(self._to_dt.timestamp())
        _start_ep = int(self._from_dt.timestamp())
        step_ep = int(datetime.timedelta(days=100).total_seconds())

        for _current_ep in range(_start_ep, _current_ep, step_ep):
            loop_date = datetime.datetime.fromtimestamp(_current_ep)
            loop_end_date = datetime.datetime.fromtimestamp(_current_ep + step_ep)
            logger.info(f"Starting segment {loop_date} to {loop_end_date}")

            # Get all unique series by reading first entry of every table.
            # With latest InfluxDB we could possibly use "schema.measurements()" but this doesn't exist in 2.0
            first_in_series = f"""
               from(bucket: "{bucket}")
               |> range(start: {_current_ep}, stop: {_current_ep + step_ep})
               |> first()"""
            timeseries: List[pd.DataFrame] = self._query_api.query_data_frame(first_in_series)

            # As we're iterating over time spans we might have an empty data set. If that is the case,
            # let's just continue with the next time span.
            if len(timeseries) < 1:
                logger.debug("Skipping")
                continue

            # get all unique measurement-field pairs and then fetch and export them one-by-one.
            # With really large databases the results should be possibly split further
            # Something like query_data_frame_stream() might be then useful.
            measurements_and_fields = []
            if type(timeseries) is pd.DataFrame:
                df = timeseries
                for gr in df.groupby(["entity_id", "_field"]):
                    measurements_and_fields.append(gr[0])
            # It could (for some reason) also be a list of DataFrames.
            else:
                for df in timeseries:
                    for gr in df.groupby(["entity_id", "_field"]):
                        measurements_and_fields.append(gr[0])

            measurements_and_fields = _whitelist_measurements(measurements_and_fields)
            logger.info(f"Found {len(measurements_and_fields)} unique time series")
            self._migrate_segment(_current_ep, _current_ep + step_ep,
                                  measurements_and_fields, bucket)

    def _get_tag_cols(self, dataframe_keys: Iterable) -> Iterable:
        """
        Filter out dataframe keys that are not tags

        @param dataframe_keys:
        @return:
        """
        return (
            k
            for k in dataframe_keys
            if not k.startswith("_") and k not in ["result", "table"]
        )

    def _list_get_influxdb_lines(self, df_list: list[pd.DataFrame]) -> str:
        line_protocol_str = ""
        for dataf in df_list:
            line_protocol_str = line_protocol_str + "\n" + self._get_influxdb_lines(dataf)
        return line_protocol_str

    def _get_influxdb_lines(self, df: pd.DataFrame) -> str:
        """
        Convert the Pandas Dataframe into InfluxDB line protocol.

        The dataframe should be similar to results received from query_api.query_data_frame()

        Not quite sure if this supports all kinds if InfluxDB schemas.
        It might be that influxdb_client package could be used as an alternative to this,
        but I'm not sure about the authorizations and such.

        Protocol description: https://docs.influxdata.com/influxdb/v2.0/reference/syntax/line-protocol/
        """
        logger.info(f"Exporting {df.columns}")

        if df.empty:
            logger.debug(f"No data points for this")
            return ""

        # line = df["_measurement"]
        line = df["entity_id"]
        line = df["domain"] + "." + line
        # print(f"Entity: {entity_id}")
        for col_name in self._get_tag_cols(df):
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

    def _migrate_segment(self, from_ep, to_ep, measurements_and_fields, bucket: str):
        whole_series = ""
        field_no = 1
        for meas, field in measurements_and_fields:
            try:
                logger.debug(
                    f"Exporting ({field_no}/{len(measurements_and_fields)}) {meas}_{field} "
                    f"from {datetime.datetime.fromtimestamp(from_ep)} "
                    f"until {datetime.datetime.fromtimestamp(to_ep)}")
                whole_series = f"""
                            from(bucket: "{bucket}")
                            |> range(start: {from_ep}, stop: {to_ep})
                            |> filter(fn: (r) => r["entity_id"] == "{meas}")
                            |> filter(fn: (r) => r["_field"] == "{field}")
                            """
                field_no += 1
                df = self._query_api.query_data_frame(whole_series)

                lines_protocol_str: str = ""
                if type(df) is list:
                    lines_protocol_str = self._list_get_influxdb_lines(df)
                else:
                    lines_protocol_str = self._get_influxdb_lines(df)

                no_lines = lines_protocol_str.count("\n")
                logger.info(
                    f"({field_no}/{len(measurements_and_fields)}) "
                    f"Writing {no_lines} lines to VictoriaMetrics db={bucket}")

                # "db" is added as an extra tag for the value.
                requests.post(f"{self._vm_url}/write?db=hass", data=lines_protocol_str)
                self._progress_file.write(f"+ {meas} {field} {from_ep}\n")
                self._progress_file.flush()

            except Exception as err:
                logger.error(f"Failed reading or writing {meas} {field} with error {err}")
                logger.error(f"Query {whole_series}")
                global no_errors
                no_errors += 1
                self._progress_file.write(f"- {meas} {field} {from_ep}\n")
                self._progress_file.flush()
                if no_errors > 10:
                    logger.fatal("Too many errors. Bailing")
                    raise err
            # EO Try/catch


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
    # start_datetime = now_datetime - datetime.timedelta(days=900)
    start_datetime = datetime.datetime(2023, 3, 20, 1, 1, 0)

    stop_datetime = datetime.datetime(2024, 1, 9, 1, 1, 0)
    # current_ep = int(now_datetime.timestamp())
    # start_ep = int(start_datetime.timestamp())
    migrator = InfluxMigrator(start_datetime, stop_datetime, bucket, url)
    migrator.influx_connect()
    migrator.migrate(bucket)


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
    print("All done")
    logger.info("Finished")
