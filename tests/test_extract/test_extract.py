import io
import json
import logging
import os
import datetime
from pprint import pprint
import time
import pandas as pd
import pytest
from moto import mock_aws
import boto3
from psycopg2.errors import ConnectionException, OperationalError
from botocore.exceptions import ClientError
from unittest.mock import Mock, patch

import s3fs
from src.extract import extract_handler, BUCKET
from src.utils.connection import create_connection_to_local, pg8000_connect_to_local

# BEFORE RUNNING, RUN:
#    setup_dbs.sql
#    test_db_utils.py


# TODO ADD THE RUNS ABOVE TO THE CICD FILE

table_list = [
    "address",
    "counterparty",
    "currency",
    "department",
    "design",
    "payment",
    "payment_type",
    "purchase_order",
    "sales_order",
    "staff",
    "transaction",
]


class TestLambdaExtractHandler:

    def test_csv_files_are_sent_to_s3(
        self,
        seed_database,
        mock_get_state_true,
        mock_connection,
        s3_with_bucket,
        s3_client,
    ):
        extract_handler({}, None)
        res = s3_client.list_objects_v2(Bucket=BUCKET)

        assert len(res["Contents"]) == len(table_list) + 1

    def test_empty_csv_files_are_filtered_out(
        self,
        seed_database,
        mock_get_state_false,
        mock_connection,
        s3_with_bucket,
        s3_client,
    ):
        # mock get state false means it's not the first run
        extract_handler({}, None)
        res = s3_client.list_objects_v2(Bucket=BUCKET)

        assert "Contents" not in res.keys()

    def test_csv_files_have_correct_names(
        self,
        seed_database,
        mock_get_state_true,
        mock_connection,
        s3_with_bucket,
        s3_client,
    ):
        extract_handler({}, None)
        res = s3_client.list_objects_v2(Bucket=BUCKET)
        contents: list = res["Contents"]

        contents = [x for x in contents if x["Key"] != "status_check.json"]
        year = datetime.datetime.now(datetime.UTC).strftime("%Y")
        month = datetime.datetime.now(datetime.UTC).strftime("%m")
        day = datetime.datetime.now(datetime.UTC).strftime("%d")

        for ind, record in enumerate(contents):
            key = record["Key"]
            prefix = (key.split("_20"))[0]
            # print(prefix)

            assert prefix == f"{year}/{month}/{day}/{table_list[ind]}"
            assert key[-4:] == ".csv"

    @pytest.mark.skip
    def test_all_OLTP_data_is_sent_on_first_invocation(self, seed_database, mock_get_state_true, s3_client, s3_with_bucket, mock_connection):
        pass

    def test_it_works_on_a_schedule(
        self,
        seed_database,
        mock_get_state_false,
        mock_connection,
        s3_with_bucket,
        s3_client,
    ):
        timestamp = datetime.datetime.now(datetime.UTC)
        conn = pg8000_connect_to_local() # had to use pg8000 because psycopg2 has been patched so it wasn't running in the lambda
        timestamp_str = timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")        
        conn.run("""
                       INSERT INTO currency
                       VALUES
                       (4, 'AUD', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                       RETURNING *;""")
        time.sleep(1)

        extract_handler({}, None)

        res = s3_client.list_objects_v2(Bucket=BUCKET)
        file_key = res["Contents"][0]["Key"]
        file = s3_client.get_object(Bucket=BUCKET, Key=file_key)
        body = file["Body"].read()

        df: pd.DataFrame = pd.read_csv(io.BytesIO(body))

        assert df["currency_id"].iloc[0] == 4
        assert df["currency_code"].iloc[0] == "AUD"
        assert df["created_at"].iloc[0][:10] == timestamp_str[:10]
        assert df["last_updated"].iloc[0][:10] == timestamp_str[:10]


    def test_handles_errors(self, seed_database, s3_client):
        # testing only boto3 errors for now as other errors would cause the function to not run at all
        with pytest.raises(ClientError):
            extract_handler({}, None)


    def test_changes_state_after_first_run(self, seed_database, s3_client, s3_with_bucket):
        initial_state = s3_client.list_objects_v2(Bucket=BUCKET)
        assert "Contents" not in initial_state.keys()

        extract_handler({}, None)

        initial_state = s3_client.get_object(Bucket=BUCKET, Key="status_check.json")
    
        file = json.loads(initial_state["Body"].read().decode())
       
        assert file["is_first_run"] == False
        

    def test_exported_data_logs(self, caplog, s3_client, s3_with_bucket, mock_get_state_true):
        extract_handler({}, None)
        with caplog.at_level(logging.INFO):
            for ind,record in enumerate(caplog.records):
                assert f"Data exported to 's3://{BUCKET}/{table_list[ind]}" in record.message


    def test_no_data_exported_logs(self, caplog, s3_client, s3_with_bucket, mock_get_state_false):
        caplog.clear()
        extract_handler({}, None)
        with caplog.at_level(logging.INFO):
            for ind,record in enumerate(caplog.records):
                assert record.message == f"No data changes in the table {table_list[ind]}"


