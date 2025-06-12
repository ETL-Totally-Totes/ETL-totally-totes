import boto3
import os
from io import BytesIO
import pandas as pd
from sqlalchemy import create_engine
import dotenv
import logging
import botocore.exceptions
from sqlalchemy.exc import SQLAlchemyError

dotenv.load_dotenv()
BUCKET = os.environ["TRANSFORM_BUCKET"]
PG_CONNECTION = os.environ["PG_CONNECTION"]

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class ReadParquetError(BaseException):
    """Raised when reading a Parquet file from S3 fails."""

    pass


class WriteDataFrameError(BaseException):
    """Raised when writing a DataFrame to the PostgreSQL database fails."""

    pass


def read_parquet_from_s3(s3_client, key, bucket=BUCKET):
    """
    Downloads a Parquet file from S3 and converts it to a pandas DataFrame.

    Args:
        s3_client (boto3.client): An active S3 client to use for fetching the file.
        key (str): The object key (i.e., file path) in the S3 bucket.
        bucket (str, optional): The name of the S3 bucket. Defaults to value from environment variable TRANSFORM_BUCKET.

    Returns:
        pd.DataFrame: A DataFrame built from the Parquet file.

    Raises:
        ReadParquetError: Raised when the file cannot be read or parsed for any reason.
    """
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        parquet_bytes = response["Body"].read()
        df = pd.read_parquet(BytesIO(parquet_bytes))  # bytes to df
        return df
    except botocore.exceptions.ClientError as e:
        logger.error(f"ClientError while accessing S3: {e}")
        raise ReadParquetError
    except Exception as e:
        logger.error(f"Unexpected exception when reading parquet from S3: {e}")
        raise ReadParquetError


def write_dataframe_to_postgres(df, table_name):
    """
    Writes a pandas DataFrame to a specified table in the PostgreSQL database.

    This function uses SQLAlchemy to handle the connection and automatically commits the transaction.
    - Data is appended to the table ('if_exists='append'').
    - An empty DataFrame will not trigger any write operation.

    Args:
        df (pd.DataFrame): The DataFrame to be written to the database.
        table_name (str): Name of the target table in PostgreSQL.

    Raises:
        WriteDataFrameError: Raised if writing to the database fails for any reason.
    """
    if df.empty:
        logger.warning(
            f"DataFrame for table '{table_name}' is empty. No data will be written."
        )
        return
    try:
        engine = create_engine(PG_CONNECTION)
        with engine.begin() as connection:
            df.to_sql(table_name, con=connection, if_exists="append", method="multi")
    except SQLAlchemyError as e:
        logger.error(
            f"SQLAlchemyError: Failed to write DataFrame to PostgreSQL table '{table_name}': {e}"
        )
        raise WriteDataFrameError
    except Exception as e:
        logger.error(
            f"Unexpected error while writing DataFrame to PostgreSQL table '{table_name}': {e}"
        )
        raise WriteDataFrameError


def load_handler(event, context):
    """
    Main handler function to load transformed data from S3 into the OLAP PostgreSQL warehouse.

    - Extracts a list of keys from Lambda event. For each of the keys present it reads the corresponding file from s3.
    - Converts the file into a pandas DataFrame.
    - Writes the data into the appropriate database table, determined by key name patterns.

    Args:
        event (dict): Event payload, expected to contain a key `'s3_keys'` with a list of file keys.
        context (object): Lambda context object (locally- pass None).

    Returns:
        dict: Summary message indicating success or lack of new data.

    Example event:
        {
    "s3_keys": [
    "2025/06/11/addr_2025-06-11 12:03:08.833644+00:00.parquet",
    "2025/06/11/coun_2025-06-11 12:03:09.513805+00:00.parquet",
    ],
    "table_names": [
    "dim_location",
    "dim_counterparty",
    ]

        }

    """
    keys = event["s3_keys"]

    if keys:
        for key in keys:
            try:
                df = read_parquet_from_s3(boto3.client("s3"), key)
                if "addr" in key:
                    table_name = "dim_location"
                    write_dataframe_to_postgres(df, table_name)
                elif "coun" in key:
                    table_name = "dim_counterparty"
                    write_dataframe_to_postgres(df, table_name)
                elif "curr" in key:
                    table_name = "currency"
                    write_dataframe_to_postgres(df, table_name)
                elif "desi" in key:
                    table_name = "dim_design"
                    write_dataframe_to_postgres(df, table_name)
                elif "sale" in key:
                    table_name = "fact_sales_order"
                    write_dataframe_to_postgres(df, table_name)
                elif "staf" in key:
                    table_name = "dim_staff"
                    write_dataframe_to_postgres(df, table_name)
                elif "date" in key:
                    table_name = "dim_date"
                    write_dataframe_to_postgres(df, table_name)
            except ReadParquetError:
                logger.error("Error occured when running read_parquet_from_s3()")
            except WriteDataFrameError:
                logger.error("Error occured when running write_dataframe_to_postgres()")
            except botocore.exceptions.ClientError as e:
                logger.error(f"ClientError while accessing S3: {e}")
            except Exception as e:
                logger.error(f"Unexpected exception when reading parquet from S3: {e}")
        return {"Success": f"Successfully loaded records!"}
    else:
        return {"Message": "No new data to append"}
