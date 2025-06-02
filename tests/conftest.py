import os
import psycopg2
import pytest
from psycopg2.extras import RealDictCursor

from moto import mock_aws
import boto3
from unittest.mock import Mock, patch
from src.extract import BUCKET
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


