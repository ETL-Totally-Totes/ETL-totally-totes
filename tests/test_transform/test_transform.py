import logging
from unittest.mock import patch
import pytest
from src.transform import (
    transform_handler,
    get_csv_file_keys,
    get_logs,
    TRANSFORM_BUCKET,
)


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@pytest.mark.skip
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
        with caplog.at_level(logging.INFO):
            get_logs(log_client, "log_group_name")
            assert "an error occured with the log client" in caplog.text


@pytest.mark.skip
class TestGetCSVFileKeys:
    def test_is_a_pure_function(self):
        test_logs = ["1", "two", "thr33"]
        get_csv_file_keys(test_logs)
        assert test_logs == ["1", "two", "thr33"]

    def test_returns_a_list_of_strings(self, logs_with_changes):
        expected_response = [
            "2025/06/04/sales_order_2025-06-04 14:46:20.426766+00:00.csv"
        ]
        response = get_csv_file_keys(logs_with_changes)
        assert response == expected_response

    def test_returns_empty_list_when_nothing_changes(self, logs_no_changes):
        response = get_csv_file_keys(logs_no_changes)
        assert response == []

    def test_handles_error(self, caplog):
        with caplog.at_level(logging.INFO):
            get_csv_file_keys(42)
        assert "unexpected error occured" in caplog.text


# @pytest.mark.skip
class TestTransformHandler:
    @pytest.mark.skip
    def test_uploads_a_parquet_buffer_to_s3(
        self,
        s3_client,
        s3_with_transform_bucket,
        mock_get_logs,
        mock_get_csv_file_keys,
        mock_read_csv_to_df,
    ):

        transform_handler({"log_group_name": "test_log"}, None)
        response = s3_client.list_objects_v2(Bucket=TRANSFORM_BUCKET)

        assert response["Contents"][0]["Key"][-8:] == ".parquet"
        assert response["KeyCount"] == 1

    @pytest.mark.skip
    def test_migrates_all_data_on_first_invocation(
        self,
        s3_client,
        s3_with_transform_bucket,
        mock_get_logs,
        mock_get_csv_file_keys_v2,
        mock_read_csv_to_df_v2,
    ):
        transform_handler({"log_group_name": "test_log"}, None)
        response = s3_client.list_objects_v2(Bucket=TRANSFORM_BUCKET)
        assert response["KeyCount"] == 2
        for file in response["Contents"]:
            assert file["Key"][-8:] == ".parquet"

    @pytest.mark.skip(
        "function migrates at least one table on subsequent invocations inclduing sales"
    )
    def test_subsequent_runs_inc_essentials(
        self,
        s3_client,
        s3_with_transform_bucket,
        mock_get_logs,
        mock_get_csv_file_keys_v2,
        mock_read_csv_to_df_v2,
    ):

        transform_handler({"log_group_name": "test_log"}, None)

        with patch("src.transform.get_csv_file_keys") as mock_keys, patch(
            "src.transform.read_csv_to_df"
        ) as mock_csv_to_df:
            mock_keys.return_value = ["2025/06/05/design.csv"]

            transform_handler({"log_group_name": "test_log"}, None)

            response = s3_client.list_objects_v2(Bucket=TRANSFORM_BUCKET)
            assert response["KeyCount"] == 3
            for file in response["Contents"]:
                assert file["Key"][-8:] == ".parquet"

    # @pytest.mark.xfail
    def test_subsequent_runs_no_changes(self, caplog, mock_get_logs):
        # "function logs that there were no changes for an empty keys list"
        # Should log this information
        with patch("src.transform.get_csv_file_keys") as mock_keys:
            mock_keys.return_value = []
            with caplog.at_level(logging.INFO, logger="src.transform"):
                transform_handler({"log_group_name": "test_log"}, None)
            assert "no data was exported during this execution" in caplog.text

    # @pytest.mark.xfail
    # def test_handles_sdk_client_error(
    #     self,
    #     caplog,
    #     s3_client,
    #     mock_get_logs,
    #     mock_get_csv_file_keys,
    #     mock_read_csv_to_df,
    # ):
    #     # "logging is broken"
    #     caplog.set_level(logging.INFO)
    #     transform_handler({"log_group_name": "test_log"}, None)
    #     assert "an error occured with s3" in caplog.text

    # @pytest.mark.xfail
    # def test_handles_index_error(self, caplog):
    #     # "logging is broken"

    #     pass

    # @pytest.mark.xfail
    # def test_handles_potential_unknown_exception(self, caplog):
    #     # "logging is broken"

    #     pass
