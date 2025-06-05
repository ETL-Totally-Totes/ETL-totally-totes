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
        # Get log group ARN
        log_group_response = log_client.describe_log_groups(
            logGroupNamePattern=self.log_group_name
        )
        log_group_arn = log_group_response["logGroups"][0]["logGroupArn"]
        # Put a new log event
        log_client.put_log_events(
            logGroupName=self.log_group_name,
            logStreamName=self.log_group_stream,
            logEvents=[
                {"timestamp": 1749026774891, "message": "totally totes"},
            ],
        )
        # Patch log group ARN in source code with the one in Line 22 for testing
        with patch("src.transform.LOG_GROUP_ARN", log_group_arn):
            response = get_logs(log_client)
        assert response == ["totally totes"]

    def test_handles_error(self, log_client, caplog):
        get_logs(log_client)
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
