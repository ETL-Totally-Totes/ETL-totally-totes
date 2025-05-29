from utils.connection import create_connection, close_connection
import pandas as pd
import datetime
#import psycopg2  # TODO: add to requirements
# import s3fs #TODO: add to requirements, remove from imports
import logging
from src.utils.utils import get_state, change_state
import boto3

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def lambda_extract_handler(event, context):
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
        current_state = get_state(boto3.client('s3')) #checks if it is the first run- returns bool
        for table in table_list:
            query = f"SELECT * FROM {table}" + (condition if current_state is False else "")
            data_frame = pd.read_sql(query, db)
            bucket = "saffi-test-bucket-test"  # TODO:make dynamic
            csv_file_name_key = f"{table}_{datetime.datetime.now(datetime.UTC)}.csv"
            csv_file_name = f"s3://{bucket}/{csv_file_name_key}"
            data_frame.to_csv(
                csv_file_name, index=False
            )  # index=False means no dataframe index written in
            print(
                f"Data exported to '{csv_file_name}' successfully."
            )  # DO NOT REMOVE, IT BREAKS
            logger.info(f"Data exported to '{csv_file_name}' successfully.")
    except Exception as e:
        logger.error({"message": e})
    finally:
        if current_state is True:
            change_state(boto3.client('s3'), False) #Once first run is ending, change 'is first run' status to False.
        close_connection(db)


lambda_extract_handler(None, None)
# exception for pandas
# exception for s3
#exception for connect
#fix empty file glitch
#get rid of credentials in connection.py