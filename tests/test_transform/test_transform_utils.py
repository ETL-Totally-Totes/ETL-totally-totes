from moto import mock_aws
from unittest.mock import patch, Mock, MagicMock
from src.utils.utils import read_csv_to_df
import pytest
import boto3
import pandas as pd
import tempfile
from io import BytesIO

#things to mock: 
#1. get_csv_file_keys
#2. s3_client
#3. s3_client.get_object

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
        s3_client.put_object(Bucket=self.TEST_BUCKET, Key="test_object_2.csv", Body=self.CSV_2)
        test_list = ['test_object_1.csv']
        result = read_csv_to_df(test_list, s3_client, self.TEST_BUCKET)
        expected_df = pd.DataFrame({'address_id':[1],'address_line_1':['6826 Herzog Via'],'address_line_2':['Avon']})
        expected = {test_list[0]:expected_df}
        final = next(result)
        compare_dfs : pd.DataFrame = final[test_list[0]].compare(expected_df)
        
        assert compare_dfs.shape[0] == 0
        assert compare_dfs.shape == (0,0)
        
    