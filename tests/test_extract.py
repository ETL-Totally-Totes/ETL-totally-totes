import os
import datetime
from pprint import pprint
import time
import pytest
from moto import mock_aws
import boto3
from unittest.mock import Mock, patch

import s3fs
from src.extract import lambda_extract_handler, BUCKET
from src.utils.connection import create_connection_to_local
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

    def test_csv_files_are_sent_to_s3(self, seed_database, mock_get_state_true, mock_connection: Mock, aws_credentials):
        with mock_aws():
            s3 = boto3.client("s3", region_name="eu-west-2")
            s3.create_bucket(
                        Bucket=BUCKET,
                        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})
            # x = mock_connection.cursor()
            # x.execute("select * from currency")
            # pprint(x.fetchall())
            lambda_extract_handler({}, None)
            res = s3.list_objects_v2(Bucket=BUCKET)
            pprint(res)
            

    def test_empty_csv_files_are_filtered_out(self, seed_database):
        pass

    def test_csv_files_have_correct_names(self, seed_database):
        pass

    def test_all_OLTP_data_is_sent_on_first_invocation(self, seed_database):
        pass

    def test_it_works_on_a_schedule(self, seed_database):
        pass

    def test_handles_errors(self, seed_database):
        pass

    def test_changes_state_after_first_run(self, seed_database):
        pass



