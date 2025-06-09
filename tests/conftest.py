import os
import pandas as pd
import psycopg2
import pytest
from psycopg2.extras import RealDictCursor

from moto import mock_aws
import boto3
from unittest.mock import Mock, patch
from src.extract import BUCKET
from src.transform import TRANSFORM_BUCKET
from tests.test_db.seed import seed_db
from src.utils.connection import create_connection_to_local

#######################
# AWS MOCKING
#######################


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture(scope="function")
def s3_client(aws_credentials):
    """
    Return a mocked S3 client
    """
    with mock_aws():
        yield boto3.client("s3", region_name="eu-west-2")



@pytest.fixture(scope="function")
# @mock_aws
def s3_with_bucket(s3_client):
    """Yields a mocked s3 client with a bucket

    Args:
        s3_client: mocked s3 client

    """
    s3_client.create_bucket(
        Bucket=BUCKET,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    yield s3_client


@pytest.fixture(scope="function")
def s3_with_transform_bucket(s3_client):
    """Yields a mocked s3 client with a transform bucket

    Args:
        s3_client: mocked s3 client

    """
    s3_client.create_bucket(
        Bucket=TRANSFORM_BUCKET,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )   
    yield s3_client


@pytest.fixture(scope="function")
def log_client(aws_credentials):
    with mock_aws():
        log_client = boto3.client("logs")
        yield log_client


##########################
# DB SEEDING AND RUNNING
##########################



@pytest.fixture(autouse=True, scope="module")
def seed_database():
    # env = ".env"
    try:
        seed_db()
    except Exception as e:
        print(e)



##########################
# MOCKS AND PATCHES
##########################



@pytest.fixture()
def mock_get_state_true():
    """Mocks the response from the delete_object_from_bucket aws util

    Yields:
        mock: mock with return value of success response expected
    """
    with patch("src.extract.get_state") as mock:
        mock.return_value = True
        yield mock


@pytest.fixture()
def mock_get_state_false():
    """Mocks the response from the delete_object_from_bucket aws util

    Yields:
        mock: mock with return value of success response expected
    """
    with patch("src.extract.get_state") as mock:
        mock.return_value = False
        yield mock



@pytest.fixture()
def mock_change_state():
    """Mocks the response from the delete_object_from_bucket aws util

    Yields:
        mock: mock with return value of success response expected
    """
    with patch("src.extract.change_state") as mock:
        mock.return_value = None
        yield mock


@pytest.fixture()
def mock_connection():
    """Mocks the response from the delete_object_from_bucket aws util

    Yields:
        mock: mock with return value of success response expected
    """
    with patch("src.extract.create_connection") as mock:
        new_conn = create_connection_to_local()
        mock.return_value = new_conn
        yield mock.return_value

@pytest.fixture()
def test_df():
    test_dict = {
        "index": [1],
        "created_at": ["2025-06-04"],
        "last_updated": ["2025-06-06"],
        "result": ["it works"],
    }
    test_df = pd.DataFrame.from_dict(test_dict)
    test_df.set_index("index", inplace=True)
    yield test_df


@pytest.fixture()
def test_address_df():
    test_dict = {
        "address_id": [15],
        "address_line_1": ["N/A"],
        "address_line_2": ["N/A"],
        "district": ["N/A"],
        "city": ["N/A"],
        "postal_code": ["N/A"],
        "country": ["N/A"],
        "phone": ["N/A"],
        "created_at": ["2025-06-04"],
        "last_updated": ["2025-06-06"],
    }
    test_df = pd.DataFrame.from_dict(test_dict)
    test_df.set_index("address_id", inplace=True)
    yield test_df


@pytest.fixture()
def test_counterparty_df():
    test_dict = {
        "counterparty_id": [1],
        "counterparty_legal_name":["Person"],
        "legal_address_id": [15],
        "commercial_contact": ["Person1"],
        "delivery_contact": ["Person2"],
        "created_at": ["2025-06-04"],
        "last_updated": ["2025-06-06"],
    }
    test_df = pd.DataFrame.from_dict(test_dict)
    test_df.set_index("counterparty_id", inplace=True)
    yield test_df



@pytest.fixture()
def mock_get_logs():
    with patch("src.transform.get_logs") as mock:
        mock.return_value = None
        yield mock

@pytest.fixture()
def mock_get_csv_file_keys():
    with patch("src.transform.get_csv_file_keys") as mock:
        mock.return_value = ["2025/06/05/design.csv"]
        yield mock

@pytest.fixture()
def mock_get_csv_file_keys_v2():
    with patch("src.transform.get_csv_file_keys") as mock:
        mock.return_value = ["2025/06/05/address.csv", "2025/06/05/counterparty.csv"]
        yield mock

@pytest.fixture()
def mock_read_csv_to_df(test_df):
    result = {"2025/06/05/design.csv": test_df}
    with patch("src.transform.read_csv_to_df") as mock:
        for k, v in result.items():
            mock.return_value = {k:v} 
            yield mock

@pytest.fixture()
def mock_read_csv_to_df_v2(test_address_df, test_counterparty_df):
    with patch("src.transform.read_csv_to_df") as mock:
        mock.return_value = {"2025/06/05/address.csv": test_address_df,
                             "2025/06/05/counterparty.csv": test_counterparty_df}
        yield mock



##########################
# REGULAR FIXTURES
##########################

@pytest.fixture()
def logs_no_changes():
    return [
        "INIT_START Runtime Version: python:3.13.v43\tRuntime Version ARN: "
        "arn:aws:lambda:eu-west-2::runtime:df8faab1a4e36a929b5b10ecff95891dfa72d84ddc1402efb6a66f373fa0c7af\n",
        "START RequestId: b947edca-4eed-4d1f-867a-69675d275389 Version: $LATEST\n",
        "[INFO]\t2025-06-04T12:46:22.643Z\tb947edca-4eed-4d1f-867a-69675d275389\tNo "
        "data changes in the table address\n",
        "[INFO]\t2025-06-04T12:46:22.683Z\tb947edca-4eed-4d1f-867a-69675d275389\tNo "
        "data changes in the table counterparty\n",
        "[INFO]\t2025-06-04T12:46:22.686Z\tb947edca-4eed-4d1f-867a-69675d275389\tNo "
        "data changes in the table currency\n",
        "[INFO]\t2025-06-04T12:46:22.688Z\tb947edca-4eed-4d1f-867a-69675d275389\tNo "
        "data changes in the table department\n",
        "[INFO]\t2025-06-04T12:46:22.703Z\tb947edca-4eed-4d1f-867a-69675d275389\tNo "
        "data changes in the table design\n",
        "[INFO]\t2025-06-04T12:46:22.711Z\tb947edca-4eed-4d1f-867a-69675d275389\tNo "
        "data changes in the table payment\n",
        "[INFO]\t2025-06-04T12:46:22.723Z\tb947edca-4eed-4d1f-867a-69675d275389\tNo "
        "data changes in the table payment_type\n",
        "[INFO]\t2025-06-04T12:46:22.726Z\tb947edca-4eed-4d1f-867a-69675d275389\tNo "
        "data changes in the table purchase_order\n",
        "[INFO]\t2025-06-04T12:46:22.751Z\tb947edca-4eed-4d1f-867a-69675d275389\tNo "
        "data changes in the table sales_order\n",
        "[INFO]\t2025-06-04T12:46:22.753Z\tb947edca-4eed-4d1f-867a-69675d275389\tNo "
        "data changes in the table staff\n",
        "[INFO]\t2025-06-04T12:46:22.763Z\tb947edca-4eed-4d1f-867a-69675d275389\tNo "
        "data changes in the table transaction\n",
        "END RequestId: b947edca-4eed-4d1f-867a-69675d275389\n",
        "REPORT RequestId: b947edca-4eed-4d1f-867a-69675d275389\tDuration: 3262.82 "
        "ms\tBilled Duration: 3263 ms\tMemory Size: 128 MB\tMax Memory Used: 118 MB\t"
        "Init Duration: 1017.49 ms\t\n",
    ]


@pytest.fixture()
def logs_with_changes():
    return [
        "INIT_START Runtime Version: python:3.13.v43\tRuntime Version ARN: "
        "arn:aws:lambda:eu-west-2::runtime:df8faab1a4e36a929b5b10ecff95891dfa72d84ddc1402efb6a66f373fa0c7af\n",
        "START RequestId: dd6368f6-510f-4cf3-a4a9-cad237613c0d Version: $LATEST\n",
        "[INFO]\t2025-06-04T14:46:19.183Z\tdd6368f6-510f-4cf3-a4a9-cad237613c0d\tNo "
        "data changes in the table address\n",
        "[INFO]\t2025-06-04T14:46:19.221Z\tdd6368f6-510f-4cf3-a4a9-cad237613c0d\tNo "
        "data changes in the table counterparty\n",
        "[INFO]\t2025-06-04T14:46:19.222Z\tdd6368f6-510f-4cf3-a4a9-cad237613c0d\tNo "
        "data changes in the table currency\n",
        "[INFO]\t2025-06-04T14:46:19.223Z\tdd6368f6-510f-4cf3-a4a9-cad237613c0d\tNo "
        "data changes in the table department\n",
        "[INFO]\t2025-06-04T14:46:19.241Z\tdd6368f6-510f-4cf3-a4a9-cad237613c0d\tNo "
        "data changes in the table design\n",
        "[INFO]\t2025-06-04T14:46:20.049Z\tdd6368f6-510f-4cf3-a4a9-cad237613c0d\tData "
        "exported to 's3://our-totally-totes-ingestion/payment_2025-06-04 "
        "14:46:20.049345+00:00.csv' successfully.\n",
        "[INFO]\t2025-06-04T14:46:20.050Z\tdd6368f6-510f-4cf3-a4a9-cad237613c0d\tNo "
        "data changes in the table payment_type\n",
        "[INFO]\t2025-06-04T14:46:20.053Z\tdd6368f6-510f-4cf3-a4a9-cad237613c0d\tNo "
        "data changes in the table purchase_order\n",
        "[INFO]\t2025-06-04T14:46:20.426Z\tdd6368f6-510f-4cf3-a4a9-cad237613c0d\tData "
        "exported to 's3://our-totally-totes-ingestion/sales_order_2025-06-04 "
        "14:46:20.426766+00:00.csv' successfully.\n",
        "[INFO]\t2025-06-04T14:46:20.428Z\tdd6368f6-510f-4cf3-a4a9-cad237613c0d\tNo "
        "data changes in the table staff\n",
        "[INFO]\t2025-06-04T14:46:20.759Z\tdd6368f6-510f-4cf3-a4a9-cad237613c0d\tData "
        "exported to 's3://our-totally-totes-ingestion/transaction_2025-06-04 "
        "14:46:20.759053+00:00.csv' successfully.\n",
        "END RequestId: dd6368f6-510f-4cf3-a4a9-cad237613c0d\n",
        "REPORT RequestId: dd6368f6-510f-4cf3-a4a9-cad237613c0d\tDuration: 5035.57 "
        "ms\tBilled Duration: 5036 ms\tMemory Size: 128 MB\tMax Memory Used: 125 MB\t"
        "Init Duration: 1234.52 ms\t\n",
    ]
