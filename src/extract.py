import datetime
import json
import logging
import pandas as pd
import boto3
from botocore.exceptions import ClientError
import psycopg2
import os
from dotenv import load_dotenv
from src.utils.connection import create_connection, close_connection


load_dotenv()


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

BUCKET = os.environ["BUCKET"]
STATUS_KEY = "status_check.json"


def get_state(s3_client):
    """
    Gets the current state of the switch file and returns it to check whether this is the first time the data pipeline is being run.
    If the file doesn't exist, it creates one and assumes this is the first run.

    Note: this function is specific to the Extract Lambda.

    Args:
        s3_client (boto3.client): An S3 client used to access the status file.


    Returns:
        bool: True if this is the first run of the pipeline, False if it's a regular repeat run.

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
    """
    Changes the state (bool) of the switch file stored in s3.
       It is changed after the first run only.
       The file can be manually changed if needed later, if there is a case of data loss.

    Note: this function is specific to the Extract Lambda.

    Args:
        s3_client (boto3.client): An S3 client used to update the status file.
        status (bool): The new state value to set (True = first run, False = normal run).

    Raises:
        TypeError: If 'status' is not a boolean.
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
    """
    Main function that connects to the transactional database (OLTP), checks for updates,
    and uploads any new or changed data to an S3 bucket in CSV format.

    Scheduled to run automatically every 10 minutes.
    - If this is the first time the pipeline is run, it extracts all data from each table.
    - On subsequent runs, it extracts only rows updated in the last 10 minutes.
    - All exported data is stored in the S3 bucket defined in the environment variable 'BUCKET' and logged.
    - If there are no chnages this is also logged.
    - Each table is saved to a dated folder structure that contains date, tablename, and timestamp.

    Args:
        event (dict): An event to trigger the Lambda- required for Lambda compatibility (pass empty dict).
        context (object): A context for the Lambda (locally- pass None).

    Returns:
        dict: Contains the log group name, useful for tracing logs.
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

            try:
                current_time = datetime.datetime.now(datetime.UTC)
                csv_path = "/tmp/new_file.csv"
                batch_size = 1000
                first_batch = True
                while True:
                    rows = db_cursor.fetchmany(batch_size)
                    if not rows:
                        break
                    df_batch = pd.DataFrame.from_records(
                        rows, columns=[desc[0] for desc in db_cursor.description]
                    )
                    if first_batch:
                        df_batch.to_csv(csv_path, mode="w", index=False)
                        first_batch = False
                    else:
                        df_batch.to_csv(csv_path, mode="a", index=False, header=False)
                year = datetime.datetime.now(datetime.UTC).strftime("%Y")
                month = datetime.datetime.now(datetime.UTC).strftime("%m")
                day = datetime.datetime.now(datetime.UTC).strftime("%d")

                csv_file_name_key = f"{year}/{month}/{day}/{table}_{current_time}.csv"
                csv_file_name = f"s3://{BUCKET}/{table}_{current_time}.csv"

                s3 = boto3.client("s3")
                s3.upload_file(csv_path, BUCKET, csv_file_name_key)
                os.remove(csv_path)
                logger.info(f"Data exported to '{csv_file_name}' successfully.")

            except Exception as e:
                logger.info(f"No data changes in the table {table}")
        return {"log_group_name": context.log_group_name}  # <- Needs testing

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

    finally:
        if current_state is True:
            change_state(
                boto3.client("s3"), False
            )  # Once first run is ending, change 'is first run' status to False.
        close_connection(db)


if __name__ == "__main__":
    extract_handler(None, None)
