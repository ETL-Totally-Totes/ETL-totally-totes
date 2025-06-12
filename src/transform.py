import datetime
import logging
import os
import json
from pprint import pprint
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
import pandas as pd
from dotenv import load_dotenv
from src.utils.utils import facts_and_dim, read_csv_to_df, df_to_parquet


load_dotenv(override=True)

EXTRACT_BUCKET = os.environ["BUCKET"]
TRANSFORM_BUCKET = os.environ["TRANSFORM_BUCKET"]
STATUS_KEY = "transform_status_check.json"

my_config = Config(region_name="eu-west-2")

log_client = boto3.client("logs", config=my_config)
s3_client = boto3.client("s3", config=my_config)

logger = logging.getLogger()
logger.setLevel(logging.INFO)


table_list = [
    "address",
    "counterparty",
    "currency",
    "department",
    "design",
    "sales_order",
    "staff",
]


def get_logs(log_client: boto3.client, log_group_name: str) -> list[str]:
    """
    Fetches the 15 most recent log events from the latest stream in the given CloudWatch log group.

    Args:
        log_client (boto3.client): Boto3 CloudWatch Logs client.
        log_group_name (str): Name of the CloudWatch log group to query.

    Returns:
        list[str]: Raw log messages (last 15 entries from latest stream).
    """
    logs = []
    try:
        describe_stream_response = log_client.describe_log_streams(
            logGroupName=log_group_name,
            orderBy="LastEventTime",
            descending=True,
            limit=1,
        )
        log_stream = describe_stream_response["logStreams"][0]["logStreamName"]

        log_event_response = log_client.get_log_events(
            logGroupName=log_group_name, logStreamName=log_stream, startFromHead=True
        )
        logs = [record["message"] for record in log_event_response["events"][-15:]]
    except ClientError as e:
        logger.error(
            {
                "message": "an error occured with the log client",
                "error_code": e.response["Error"]["Code"],
                "details": e.response["Error"]["Message"],
            }
        )
    finally:
        return logs


def get_csv_file_keys(logs: list[str]) -> list[str]:
    """
    Parses log messages from get_logs to construct S3 keys for CSV uploads.

    Filters logs for successful export events, then reconstructs the full S3 object keys
    based on timestamp and table name.

    Args:
        logs (list[str]): Raw logs from get_logs function.

    Returns:
        list[str]: List of s3 keys for CSV files.
    """
    file_keys = []
    try:
        for log in logs:
            if not log.startswith("[INFO]") or "successfully" not in log:
                continue
            s3_uri = log.split("'")[1]
            s3_key = s3_uri.split(f"s3://{EXTRACT_BUCKET}/")[1]
            for table in table_list:
                if s3_key.startswith(table):
                    date = s3_key.split(f"{table}_")[1].split("-")
                    year = date[0]
                    month = date[1]
                    day = date[2][:2]
                    actual_s3_key = year + "/" + month + "/" + day + "/" + s3_key
                    file_keys.append(actual_s3_key)

    except Exception as e:
        logger.error(
            {
                "message": "unexpected error occured",
                "details": e,
            }
        )
    finally:
        return file_keys


def get_state(s3_client):
    """
    Gets the current state of the switch file and returns it to check whether this is the first time the data pipeline is being run.
    If the file doesn't exist, it creates one and assumes this is the first run.

    Note: this function is specific to the Transform Lambda.

    Args:
        s3_client (boto3.client): An S3 client used to access the status file.


    Returns:
        bool: True if this is the first run of the pipeline, False if it's a regular repeat run.

    """
    try:
        response = s3_client.get_object(Bucket=TRANSFORM_BUCKET, Key=STATUS_KEY)
        data = json.loads(response["Body"].read().decode("utf-8"))
        return data["is_first_run"]
    except ClientError as e:
        # if it is the first run and the file is not there
        s3_client.put_object(
            Bucket=TRANSFORM_BUCKET,
            Key=STATUS_KEY,
            Body=json.dumps({"is_first_run": True}),
        )
        return True


def change_state(s3_client, status: bool):
    """
    Changes the state (bool) of the switch file stored in s3.
       It is changed after the first run only.
       The file can be manually changed if needed later, if there is a case of data loss.

    Note: this function is specific to the Transform Lambda.

    Args:
        s3_client (boto3.client): An S3 client used to update the status file.
        status (bool): The new state value to set (True = first run, False = normal run).

    Raises:
        TypeError: If 'status' is not a boolean.
    """
    if isinstance(status, bool):
        try:
            s3_client.put_object(
                Bucket=TRANSFORM_BUCKET,
                Key=STATUS_KEY,
                Body=json.dumps({"is_first_run": status}),
            )
        except ClientError as e:
            print(e)
    else:
        raise TypeError("Only arguments of type bool accepted")


def transform_handler(event, context):
    """
    Main Lambda handler for the transform phase of the pipeline.

    - Reads logs from the latest log stream from the extract lambda.
    - Extracts CSV S3 keys from those logs.
    - Reads CSVs from S3 and applies transformations.
    - Writes transformed DataFrames as Parquet files to the 'processed' S3 bucket.

    Args:
        event (dict): returned data from the extract lambda (expects 'log_group_name' key).
        context (object):  Lambda context object (locally- pass None).

    Returns:
        dict: {
            "s3_keys": List of written Parquet file keys,
            "table_names": Corresponding table names
        }
    """
    logs = get_logs(log_client, log_group_name=event["log_group_name"])
    try:
        keys = get_csv_file_keys(logs)
        parquet_keys = []
        table_name = []
        current_state = get_state(s3_client)

        dfs = read_csv_to_df(keys, s3_client)
        address, department = None, None  # These are dataframes
        for key in keys:
            new_df = None
            curr_df = next(dfs)
            prefix = key[11:15]
            if prefix == "sale":
                df = curr_df[key]
                new_df = facts_and_dim["sales_fact"](df)
                table_name.append("fact_sales_order")
            elif prefix == "addr":
                df = curr_df[key]
                address = df
                new_df = facts_and_dim["address_dim"](address)
                table_name.append("dim_location")

            elif prefix == "coun":
                df = curr_df[key]
                new_df: pd.DataFrame = facts_and_dim["counterparty_dim"](df, address)
                # Legal_Address_id is still included. Need to look into dropping that column
                table_name.append("dim_counterparty")

            elif prefix == "curr":
                df = curr_df[key]
                new_df = facts_and_dim["currency_dim"](df)
                table_name.append("dim_currency")

            elif prefix == "depa":
                df = curr_df[key]
                department = df
                continue
            elif prefix == "desi":
                df = curr_df[key]
                new_df = facts_and_dim["design_dim"](df)
                table_name.append("dim_design")

            elif prefix == "staf":
                df = curr_df[key]
                new_df = facts_and_dim["staff_dim"](df, department)
                table_name.append("dim_staff")

            df_buffer = df_to_parquet(new_df)
            current_time = datetime.datetime.now(datetime.UTC)
            year = current_time.strftime("%Y")
            month = current_time.strftime("%m")
            day = current_time.strftime("%d")

            parquet_file_key = f"{year}/{month}/{day}/{prefix}_{current_time}.parquet"
            parquet_keys.append(parquet_file_key)
            s3_client.put_object(
                Bucket=TRANSFORM_BUCKET, Key=parquet_file_key, Body=df_buffer
            )

            logger.info(f"Data exported to {parquet_file_key} successfully.")
        if not keys:
            logger.info(
                {
                    "message": "no data was exported for any table during this execution",
                }
            )
    except ClientError as e:
        logger.error(
            {
                "message": "an error occured with s3",
                "error_code": e.response["Error"]["Code"],
                "details": e.response["Error"]["Message"],
            }
        )
    except IndexError as e:
        logger.error(
            {
                "message": "no data was exported for any table during this execution",
            }
        )
    except Exception as e:
        logger.error({"message": "unknown error occured", "details": e})
    finally:
        if current_state:
            new_df = facts_and_dim["date_dim"]()
            df_buffer = df_to_parquet(new_df)
            current_time = datetime.datetime.now(datetime.UTC)
            year = current_time.strftime("%Y")
            month = current_time.strftime("%m")
            day = current_time.strftime("%d")

            parquet_file_key = f"{year}/{month}/{day}/date_dim_{current_time}.parquet"
            parquet_keys.append(parquet_file_key)
            table_name.append("dim_date")

            s3_client.put_object(
                Bucket=TRANSFORM_BUCKET, Key=parquet_file_key, Body=df_buffer
            )

            logger.info(f"Data exported to '{parquet_file_key}' successfully.")
            change_state(s3_client, False)
        return {"s3_keys": parquet_keys, "table_names": table_name}


if __name__ == "__main__":
    x = get_logs(log_client, "/aws/lambda/extract_handler")
    file_keys = get_csv_file_keys(x)
    df = read_csv_to_df(file_keys)
    next = next(df)
    pprint(next)
