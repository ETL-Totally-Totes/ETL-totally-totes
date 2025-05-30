import pytest
from unittest.mock import patch, MagicMock
from src.extract import lambda_extract_handler
# from pg8000.native import Connection
# from tests.test_db.seed import connect_to_db, seed_db


@patch("src.extract.create_connection")
@patch("src.extract.get_state")
@patch("src.extract.change_state")
@patch("src.extract.pd.read_sql")
@patch("src.extract.boto3.client")
@patch("src.extract.pd.DataFrame.to_csv")
def test_lambda_extract_handler_success(
    mock_to_csv, mock_boto_client, mock_read_sql, mock_change_state,
    mock_get_state, mock_create_connection
):
    # Mock DB connection
    mock_conn = MagicMock()
    mock_create_connection.return_value = mock_conn

    # Mock get_state to simulate NOT first run
    mock_get_state.return_value = False

    # Mock DataFrame from SQL
    mock_df = MagicMock()
    mock_df.empty = False
    mock_read_sql.return_value = mock_df

    # Mock boto3 S3 client
    mock_s3 = MagicMock()
    mock_boto_client.return_value = mock_s3

    # Call the handler
    lambda_extract_handler({}, {})

    # ASSERTIONS
    assert mock_create_connection.called
    assert mock_read_sql.call_count == 11  # 11 tables
    assert mock_to_csv.call_count == 11
    assert mock_boto_client.called
    assert not mock_change_state.called  # not first run, so don't change state



# Test that function returns a .csv file
# Test that function executes at correct time interval
# Test that function drops files in a bucket
#mock a database
#mock the result in CSV (data type)
#test psycopg2 connection
#mock an s3 bucket test if the mock csv is in the bucket
#test handling errors
#check that the output csv first row has all the column names