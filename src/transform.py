import datetime
import io
import json
import logging
import os
from pprint import pprint
import boto3
from botocore.exceptions import ClientError
import pandas as pd
from dotenv import load_dotenv

# Get logs from extract function
# Get key out of log message
# Get object(s) out of CSV bucket
# Convert CSV to dataframe (Saffi & Selva) 
# Merge dataframes         >> utils?
# Convert to parquet files
# Put parquet files into S3 processed bucket

# Send logs to Cloudwatch
#

load_dotenv(override=True)
log_client = boto3.client("logs")
s3_client = boto3.client("s3")

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

LOG_GROUP_ARN = os.environ["EXTRACT_LOG_GROUP"]
EXTRACT_BUCKET = os.environ['BUCKET']

table_list = [
            "address",
            "counterparty",
            "currency",
            "department",
            "design",
            "sales_order",
            "staff",
        ]

def get_logs(log_client)->list[str]:
    """Function to extract logs

    Args:
        log_client (boto3 client): log client

    Returns:
        list[str]: List of raw log messages
    """
    try:    
        describe_stream_response = log_client.describe_log_streams(
            logGroupIdentifier=LOG_GROUP_ARN,
            orderBy='LastEventTime',
            descending=True,
            limit=1
        )
        log_stream = describe_stream_response["logStreams"][0]["logStreamName"]

        log_event_response = log_client.get_log_events(
            logGroupIdentifier=LOG_GROUP_ARN,
            logStreamName=log_stream,
        )
        logs = [
            record["message"] for record in log_event_response["events"]
        ]
        return logs
    
    except ClientError as e:
        logger.error(
            {
                "message": "an error occured with the log client",
                "error_code": e.response["Error"]["Code"],
                "details": e.response["Error"]["Message"],
            }
        )

def get_csv_file_keys(logs: list[str])->list[str]:
    """Gets CSV file keys from get_logs function return value.

    Args:
        logs (list[str]): Raw logs

    Returns:
        list[str]: List of file S3 keys 
    """    
    try:
        file_keys = []
        for log in logs:
            if not log.startswith("[INFO]") or "successfully" not in log:
                continue
            s3_uri = log.split("'")[1]
            s3_key = s3_uri.split(f"s3://{EXTRACT_BUCKET}/")[1] 
            for table in table_list:
                if s3_key.startswith(table):
                    file_keys.append(s3_key)
        return file_keys

    except Exception as e:
        logger.error(
            {
                "message": "unexpected error occured",
                "details": e,
            }
        )


if __name__ == "__main__":       
    x = get_logs(log_client)
    get_csv_file_keys(x)