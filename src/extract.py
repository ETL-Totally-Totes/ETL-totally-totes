import datetime
import io
import json
import logging
from pprint import pprint
import pandas as pd
import boto3
from botocore.exceptions import ClientError
import psycopg2
from src.utils.connection import create_connection, close_connection
from dotenv import load_dotenv
import os

load_dotenv()


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

BUCKET = os.environ['BUCKET']
STATUS_KEY = "status_check.json"


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
        response = s3_client.get_object(Bucket=BUCKET, Key=STATUS_KEY)
        data = json.loads(response["Body"].read().decode("utf-8"))
        return data["is_first_run"]
    except ClientError as e:
        # if it is the first run and the file is not there
        s3_client.put_object(
            Bucket=BUCKET,
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
                Bucket=BUCKET,
                Key=STATUS_KEY,
                Body=json.dumps({"is_first_run": status}),
            )
        except ClientError as e:
            print(e)
    else:
        raise TypeError("Only arguments of type bool accepted")


def extract_handler(event, context):
    """Monitors the OLTP for any changes every 10 minutes.
       If there are any changes, it is exported to an s3 bucket and logged.
       If there are no changes, nothing happens and this is also logged.

    Args:
        event (json): an event to trigger the lambda (pass empty dict/json)
        context (Any): a context for the lambda. (pass None)
    """    
    current_state = get_state(
        boto3.client("s3")
    )  # checks if it is the first run- returns bool
    try:
        db = create_connection()

        table_list = [
            "address",
            "counterparty",
            "currency",
            "department",
            "design",
            "payment",
            "payment_type",
            "purchase_order",
            "sales_order",
            "staff",
            "transaction",
        ]
        condition = " WHERE now() - last_updated <= interval '10 mins'"
        db_cursor = db.cursor()

        for table in table_list:
            query = f"SELECT * FROM {table}" + (
                condition if current_state is False else ""
            )

            db_cursor.execute(query)

            data_frame = pd.DataFrame.from_records(db_cursor.fetchall())

            if data_frame.shape[0]:
                csv_buffer = io.StringIO()
                data_frame.to_csv(csv_buffer, index=False)
                year = datetime.datetime.now(datetime.UTC).strftime("%Y")
                month = datetime.datetime.now(datetime.UTC).strftime("%m")
                day = datetime.datetime.now(datetime.UTC).strftime("%d")
                current_time = datetime.datetime.now(datetime.UTC)
                csv_file_name_key = f"{year}/{month}/{day}/{table}_{current_time}.csv"
                s3 = boto3.client("s3")
                s3.put_object(
                    Bucket=BUCKET, Key=csv_file_name_key, Body=csv_buffer.getvalue()
                )
                #  There might be an issue, when migrating on first run could consume a lot of memory.
                # This is not for MVP its for later when we might have millions of rows to migrate to OLAP

                csv_file_name = (f"s3://{BUCKET}/{table}_{current_time}.csv")
                logger.info(f"Data exported to '{csv_file_name}' successfully.")
            else:
                logger.info(f"No data changes in the table {table}")
        return {
            "log_group_name": context.log_group_name   # <- Needs testing
        }

    except ClientError as e:
        logger.error(
            {
                "message": "an error occured with s3",
                "error_code": e.response["Error"]["Code"],
                "details": e.response["Error"]["Message"],
            }
        )

    except psycopg2.errors.OperationalError as e:
        logger.error(
            {"message": "an error occured with the psycopg2 connection", "details": e}
        )

    except pd.errors.EmptyDataError as e:
        logger.error({"message": "an error occured with pandas", "details": e})

    except Exception as e:
        logger.error({"message": "unknown error occured", "details": e})

    else:
        if current_state is True:
            change_state(
                boto3.client("s3"), False
            )  # Once first run is ending, change 'is first run' status to False.
    finally:
        close_connection(db)


if __name__ == "__main__":
    extract_handler(None, None)
