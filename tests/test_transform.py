import logging
from unittest.mock import patch
import pytest
from moto import mock_aws
from src.transform import get_csv_file_keys, get_logs, EXTRACT_BUCKET


class TestGetLogs:
    log_group_name = "test_log"
    log_group_stream = "test_stream"

    def test_returns_logs(self, log_client):
        # Create stream and group
        log_client.create_log_group(logGroupName=self.log_group_name)
        log_client.create_log_stream(
            logGroupName=self.log_group_name, logStreamName=self.log_group_stream
        )
        # Get log group Name
        log_group_response = log_client.describe_log_groups(
            logGroupNamePattern=self.log_group_name
        )
        log_group_name = log_group_response["logGroups"][0]["logGroupName"]
        # Put a new log event
        log_client.put_log_events(
            logGroupName=self.log_group_name,
            logStreamName=self.log_group_stream,
            logEvents=[
                {"timestamp": 1749026774891, "message": "totally totes"},
            ],
        )
        response = get_logs(log_client, log_group_name)
        assert response == ["totally totes"]

    def test_handles_error(self, log_client, caplog):
        get_logs(log_client, "log_group_name")
        with caplog.at_level(logging.INFO):
            assert "an error occured with the log client" in caplog.text


# @pytest.mark.skip
class TestGetCSVFileKeys:
    def test_is_a_pure_function(self):
        test_logs = ["1", "two", "thr33"]
        get_csv_file_keys(test_logs)
        assert test_logs == ["1", "two", "thr33"]

    def test_returns_a_list_of_strings(self, logs_with_changes):
        expected_response = ["sales_order_2025-06-04 14:46:20.426766+00:00.csv"]
        response = get_csv_file_keys(logs_with_changes)
        assert response == expected_response

    def test_returns_empty_list_when_nothing_changes(self, logs_no_changes):
        response = get_csv_file_keys(logs_no_changes)
        assert response == []

    def test_handles_error(self, caplog):
        get_csv_file_keys(42)
        with caplog.at_level(logging.INFO):
            assert "unexpected error occured" in caplog.text


@patch("src.transform.get_logs", None)
@patch("src.transform.get_csv_file_keys", None)
class TestTransformHandler:

    def test_uploads_a_parquet_buffer_to_s3(self, s3_client, s3_with_transform_bucket):
        # patch get_logs and get_csv_file_keys to return NOne. This might not be necessary
        # patch read_csv_to_df and create a new df for that return value
        pass


    def test_migrates_all_data_on_first_invocation(self, s):
        # Might need to test last
        # Put 2 csvs in the mocked bucket, and ensure they're moved
        pass

    @pytest.mark.it("function migrates at least one table on subsequent invocations inclduing sales")
    def test_subsequent_runs_inc_sales(self):
        # Put 2 csvs in the mocked bucket, move them
        # Put another csv after and ensure that it is moved.
        # Test this for essential tables like sales and non essentials like department.
        # Assume that for the mvp, dims won't change cause if they do, more logic will be needed.
        pass

    @pytest.mark.it("function logs that there were no changes for an empty keys list")
    def test_subsequent_runs_no_changes(self, caplog):
        # Should log this information
        pass


    def test_handles_sdk_client_error(self, caplog):
        # Test logs
        pass

    
    def test_handles_index_error(self, caplog):
        pass


    def test_handles_potential_unknown_exception(self, caplog):
        pass



