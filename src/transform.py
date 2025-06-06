import datetime
import logging
import os
import json
from pprint import pprint
import boto3
from botocore.exceptions import ClientError
import pandas as pd
from dotenv import load_dotenv
from src.utils.utils import facts_and_dim, read_csv_to_df, df_to_parquet


load_dotenv(override=True)

EXTRACT_BUCKET = os.environ["BUCKET"]
TRANSFORM_BUCKET = os.environ["TRANSFORM_BUCKET"]
STATUS_KEY = "transform_status_check.json"

log_client = boto3.client("logs")
s3_client = boto3.client("s3")

logger = logging.getLogger(__name__)


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
    """Function to extract logs

    Args:
        log_client (boto3 client): log client
        log_group_name: str

    Returns:
        list[str]: List of raw log messages
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
            logGroupName=log_group_name,
            logStreamName=log_stream,
        )
        logs = [record["message"] for record in log_event_response["events"]]
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
    """Gets CSV file keys from get_logs function return value.

    Args:
        logs (list[str]): Raw logs

    Returns:
        list[str]: List of file S3 keys
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
        Gets the current state of the switch file and returns it.
        If it is doesn't exist, it means it is the first run and the file does not exist so it is then created.
    Args:
        s3_client (boto3 client): s3 client

    Returns:
        bool: status of the run
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
    """Changes the state of the switch file. 
       It is changed after the first run only.
       Any subsequent changes will be done manually incase of any data loss.

    Args:
        s3_client (boto3 client): s3 client
        status (bool): status to be applied to the first run
    Raises:
        TypeError: Only arguments of type bool accepted
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
    Reads logs from the latest log stream from the extract lambda to get the most recent CSV file keys.
    Transform the OLTP data changes to a parquet file.
    Loads parquet files in to 'processed' S3 bucket.

    Args:
        event (JSON | dict): returned data from the extract lambda. 
        context (JSON | dict): contains information about this lambda 
    """    
    logs = get_logs(log_client, log_group_name=event["log_group_name"])
    keys = get_csv_file_keys(logs)
    try:
        if keys:
            dfs = read_csv_to_df(keys)
            address, department = None, None # These are dataframes
            print(keys)
            for key in keys:
                print(key)
                new_df = None
                prefix = key[11:15]
                if prefix == "sale":
                    df = dfs[key]
                    new_df = facts_and_dim["sales_fact"](df)
                elif prefix == "addr":
                    df = dfs[key]
                    address = df
                    new_df = facts_and_dim["address_dim"](address)
                elif prefix == "coun":
                    df = dfs[key]
                    new_df = facts_and_dim["counterparty_dim"](df, address)
                elif prefix == "curr":
                    df = dfs[key]
                    new_df = facts_and_dim["currency_dim"](df)
                elif prefix == "depa":
                    df = dfs[key]
                    department = df
                elif prefix == "desi":
                    df = dfs[key]
                    new_df = facts_and_dim["design_dim"](df)
                elif prefix == "staf":
                    df = dfs[key]
                    new_df = facts_and_dim["staff_dim"](df, department)
                else:
                    current_state = get_state(s3_client)
                    if current_state:
                        new_df = facts_and_dim["date_dim"]
                        change_state(s3_client, False)
                        
                df_buffer = df_to_parquet(new_df)
                current_time = datetime.datetime.now(datetime.UTC)
                year = current_time.strftime("%Y")
                month = current_time.strftime("%m")
                day = current_time.strftime("%d")

                parquet_file_key = (
                    f"{year}/{month}/{day}/{prefix}_{current_time}.parquet"
                )

                s3_client.put_object(
                    Bucket=TRANSFORM_BUCKET, Key=parquet_file_key, Body=df_buffer
                )

                logger.info(f"Data exported to '{parquet_file_key}' successfully.")
            else:
                logger.error(
                    {
                        "message": "no data was exported during this execution",
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
                "message": "no data was exported during this execution",
            }
        )
    except Exception as e:
        logger.error({"message": "unknown error occured", "details": e})


if __name__ == "__main__":
    x = get_logs(log_client, "/aws/lambda/extract_handler")
    file_keys = get_csv_file_keys(x)
    df = read_csv_to_df(file_keys)
    next = next(df)
    pprint(next)
    # sales_fact = create_sales_fact(next)
    # pprint(sales_fact)
    # for k, v in df.items():
    #     print(k)
    #     print(v)
