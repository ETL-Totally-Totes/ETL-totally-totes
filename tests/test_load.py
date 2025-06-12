import pytest
import boto3
import pandas as pd
from io import BytesIO
from moto import mock_aws
from unittest.mock import patch, MagicMock
import logging
from sqlalchemy.exc import SQLAlchemyError
from src.load import (
    read_parquet_from_s3,
    write_dataframe_to_postgres,
    ReadParquetError,
    WriteDataFrameError,
)


@mock_aws
class TestReadParquetFromS3:

    @pytest.fixture
    def mock_s3_bytes_object(self):
        df = pd.DataFrame(
            {
                "address_id": [1],
                "address_line_1": ["6826 Herzog Via"],
                "address_line_2": ["Avon"],
            }
        )
        buffer = BytesIO()
        df.to_parquet(buffer)
        buffer.seek(0)
        return buffer.getvalue()

    @pytest.fixture
    def s3_client(self):
        with mock_aws():
            client = boto3.client("s3", region_name="eu-west-2")
            yield client

    @pytest.fixture
    def setup_s3_bucket(self, s3_client, mock_s3_bytes_object):
        bucket_name = "test_transform_bucket"
        s3_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        s3_client.put_object(
            Bucket=bucket_name, Key="test_bytes_object", Body=mock_s3_bytes_object
        )
        return s3_client, bucket_name

    def test_read_parquet_from_s3_util_returns_DF(self, setup_s3_bucket):
        s3_client, bucket_name = setup_s3_bucket

        result = read_parquet_from_s3(
            s3_client, key="test_bytes_object", bucket=bucket_name
        )
        expected = pd.DataFrame(
            {
                "address_id": [1],
                "address_line_1": ["6826 Herzog Via"],
                "address_line_2": ["Avon"],
            }
        )

        pd.testing.assert_frame_equal(result, expected)

    def test_read_parquet_from_s3_client_error_logging(self, caplog, setup_s3_bucket):

        s3_client, bucket_name = setup_s3_bucket
        test_key = "definitely-non-existent-file.parquet"

        with pytest.raises(ReadParquetError) as e:
            read_parquet_from_s3(s3_client, test_key, bucket_name)

        assert "ClientError while accessing S3:" in caplog.text
        assert "The specified key does not exist." in caplog.text


class TestWriteDataframeToPostgresUtil:
    def test_write_dataframe_to_postgres(self):
        df = pd.DataFrame(
            {
                "address_id": [1],
                "address_line_1": ["6826 Herzog Via"],
                "address_line_2": ["Avon"],
            }
        )

        engine = MagicMock()
        connection = MagicMock()
        # need to use MagicMock() for context management
        engine.begin.return_value.__enter__.return_value = connection

        with patch("src.load.create_engine", return_value=engine), patch.object(
            df, "to_sql"
        ) as mock_to_sql:

            write_dataframe_to_postgres(df, "table")

        mock_to_sql.assert_called_once_with(
            "table",
            con=connection,
            if_exists="append",
            method="multi",
        )

    def test_sqlalchemy_error_raised(self, caplog):
        logger = logging.getLogger("src.load")
        logger.setLevel(logging.INFO)
        caplog.set_level(logging.ERROR)

        sample_dataframe = pd.DataFrame(
            {
                "address_id": [1],
                "address_line_1": ["6826 Herzog Via"],
                "address_line_2": ["Avon"],
            }
        )
        table_name = "error_table_sql"

        with patch("src.load.create_engine") as mock_create_engine, patch(
            "pandas.DataFrame.to_sql"
        ) as mock_to_sql:
            mock_engine = MagicMock()
            mock_create_engine.return_value = mock_engine
            mock_connection = MagicMock()
            mock_engine.begin.return_value.__enter__.return_value = mock_connection

            mock_to_sql.side_effect = SQLAlchemyError("Connection failed")

            with pytest.raises(WriteDataFrameError) as e:
                write_dataframe_to_postgres(sample_dataframe, table_name)

            assert (
                f"SQLAlchemyError: Failed to write DataFrame to PostgreSQL table '{table_name}': Connection failed"
                in caplog.text
            )
            assert e.type is WriteDataFrameError

    def test_general_exception_error_raised(self, caplog):
        logger = logging.getLogger("src.load")
        logger.setLevel(logging.INFO)
        caplog.set_level(logging.ERROR)

        sample_dataframe = pd.DataFrame(
            {
                "address_id": [1],
                "address_line_1": ["6826 Herzog Via"],
                "address_line_2": ["Avon"],
            }
        )
        table_name = "error_table_general"

        with patch("src.load.create_engine") as mock_create_engine, patch(
            "pandas.DataFrame.to_sql"
        ) as mock_to_sql:
            mock_engine = MagicMock()
            mock_create_engine.return_value = mock_engine
            mock_connection = MagicMock()
            mock_engine.begin.return_value.__enter__.return_value = mock_connection

            mock_to_sql.side_effect = Exception("Unexpected internal server error")

            with pytest.raises(WriteDataFrameError) as e:
                write_dataframe_to_postgres(sample_dataframe, table_name)

            assert (
                f"Unexpected error while writing DataFrame to PostgreSQL table '{table_name}': Unexpected internal server error"
                in caplog.text
            )
            assert e.type is WriteDataFrameError

    def test_write_dataframe_to_postgres_empty_df_warning(self, caplog):
        logger = logging.getLogger("src.load")
        logger.setLevel(logging.INFO)
        caplog.set_level(logging.WARNING)

        empty_dataframe = pd.DataFrame()
        table_name = "empty_df_table"

        with patch("src.load.create_engine") as mock_create_engine:
            write_dataframe_to_postgres(empty_dataframe, table_name)

            mock_create_engine.assert_not_called()
            assert (
                f"DataFrame for table '{table_name}' is empty. No data will be written."
                in caplog.text
            )
