from moto import mock_aws
from src.utils.utils import read_csv_to_df
import pytest
import boto3
import pandas as pd
import os
import tempfile


# test that you give a csv and get a dataframe
# test it is taking from bucket

@pytest.fixture
def s3_bucket_setup(s3_client):
    with mock_aws():
        bucket_name = "test_bucket"
        object = "test_object.csv"
        bucket = s3_client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})

# @pytest.fixture
# def put_csv_in_bucket(s3_bucket_setup, s3_client):
#         body = """address_id,address_line_1,address_line_2,district	city,postal_code,country,phone,created_at,last_updated\n
#                 1,6826 Herzog Via,Avon,New Patienceburgh,28441,Turkey,1803 637401,20:50.0,20:50.0"""
#         put_object = s3_client.put_object(Bucket=bucket, Key=object, Body=body)
#         get_object = s3_client.get_object(Bucket=bucket, Key=object)
#         response = get_object['Body'].read().decode('utf-8')
#         return response

class TestReadCsvToDf:
    # def test_access_to_s3(self, s3_boto):

    def test_func_returns_df_from_list_of_one_key(self, s3_with_bucket):
        with tempfile.NamedTemporaryFile(mode='w+', delete=False) as tmp:
            body = "address_id,address_line_1,address_line_2,district city,postal_code,country,phone,created_at,last_updated \n1,6826 Herzog Via,Avon,New Patienceburgh,28441,Turkey,1803 637401,20:50.0,20:50.0"
            tmp.write(body)
            tmp_path = tmp.name
            file_name = os.path.basename(tmp_path)
            print(file_name)

        with mock_aws():
            s3_with_bucket.return_value = file_name

        test_input = ['payment_2025-06-04 10:56:19.770508+00:00.csv']
        result = read_csv_to_df(test_input) #returns a dataframe
        print(type(result))
        # expected_df = pd.DataFrame(body) #string of expected df
        # print(expected_df)
        # assert result == expected_df
    
