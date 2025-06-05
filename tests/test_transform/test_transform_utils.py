from moto import mock_aws
from unittest.mock import patch, Mock, MagicMock
from src.utils.utils import read_csv_to_df
import pytest
import boto3
import pandas as pd
import tempfile
from io import BytesIO
from botocore.exceptions import ClientError
import logging

@mock_aws
class TestReadCsvToDf:
    TEST_BUCKET = 'test_ingestion_bucket'
    CSV_1 = "address_id,address_line_1,address_line_2\n1,6826 Herzog Via,Avon"
    CSV_2 = "address2_id,address2_line_1,address_line2_2\n1,12 Gold Close,Harlow"

    def test_single_key_list_yields_single_df(self, aws_credentials):
        s3_client = boto3.client('s3', region_name="eu-west-2")
        s3_client.create_bucket(
        Bucket=self.TEST_BUCKET,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
        s3_client.put_object(Bucket=self.TEST_BUCKET, Key="test_object_1.csv", Body=self.CSV_1)
        test_list = ['test_object_1.csv']
        result = read_csv_to_df(test_list, s3_client, self.TEST_BUCKET)
        expected_df = pd.DataFrame({'address_id':[1],'address_line_1':['6826 Herzog Via'],'address_line_2':['Avon']})
        final = next(result)
        compare_dfs : pd.DataFrame = final[test_list[0]].compare(expected_df)

        assert compare_dfs.shape[0] == 0
        assert compare_dfs.shape == (0,0)
        assert final[test_list[0]].empty is not True
        
    def test_multiple_key_list_yields_multiple_dfs(self, aws_credentials):
        s3_client = boto3.client('s3', region_name="eu-west-2")
        s3_client.create_bucket(
        Bucket=self.TEST_BUCKET,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
        s3_client.put_object(Bucket=self.TEST_BUCKET, Key="test_object_1.csv", Body=self.CSV_1)
        s3_client.put_object(Bucket=self.TEST_BUCKET, Key="test_object_2.csv", Body=self.CSV_2)
        test_list = ['test_object_1.csv','test_object_2.csv']
        result = read_csv_to_df(test_list, s3_client, self.TEST_BUCKET)
        result_list = list(result)
        final_1 = result_list[0]
        final_2 = result_list[1]
        expected_df_1 = pd.DataFrame({'address_id':[1],'address_line_1':['6826 Herzog Via'],'address_line_2':['Avon']})
        expected_df_2 = pd.DataFrame({'address2_id':[1],'address2_line_1':['12 Gold Close'],'address_line2_2':['Harlow']})
        
        assert len(result_list) == 2
        assert final_1["test_object_1.csv"].size == 3
        assert final_2["test_object_2.csv"].size == 3
        assert final_1["test_object_1.csv"].empty is not True
        assert final_2["test_object_2.csv"].empty is not True

    def test_function_raises_index_error_with_empty_list(self, aws_credentials):
        s3_client = boto3.client('s3', region_name="eu-west-2")
        s3_client.create_bucket(
        Bucket=self.TEST_BUCKET,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
        s3_client.put_object(Bucket=self.TEST_BUCKET, Key="test_object_1.csv", Body=self.CSV_1)
        test_list = []
        result = read_csv_to_df(test_list, s3_client, self.TEST_BUCKET)
        
        with pytest.raises(IndexError) as err:
            list(result) # Must iterate or call next() to trigger the exception
        
        assert str(err.value) == "Argument list must not be empty."

    @pytest.mark.skip("TODO LATER")
    def test_function_raises_name_error(self, aws_credentials, caplog):
        s3_client = boto3.client('s3', region_name="eu-west-2")
        s3_client.create_bucket(
        Bucket=self.TEST_BUCKET,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
        # s3_client.put_object(Bucket=self.TEST_BUCKET, Key="test_object_1.csv", Body=self.CSV_1)
        test_list = ['xxx.csv']
        result = read_csv_to_df(test_list, s3_client, self.TEST_BUCKET)
        with caplog.at_level(logging.ERROR): # Capture logs at ERROR level
            result_dfs = list(result)
            assert len(result_dfs) == 0
            assert f"The key '{test_list[0]}' does not exist in bucket '{self.TEST_BUCKET}'." in caplog.text
        #assert next(result) == "The key 'xxx.csv' does not exist in bucket 'test_ingestion_bucket'."
