import datetime
import logging
import pandas as pd
import boto3
from botocore.exceptions import ClientError
import psycopg2
from src.utils.utils import get_state, change_state
from utils.connection import create_connection, close_connection

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

BUCKET = "abby-test-bucket-test"  #DEFINITELY CHANGE THIS!!!!


def lambda_extract_handler(event, context):
    current_state = get_state(boto3.client('s3')) #checks if it is the first run- returns bool
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
        
        for table in table_list:
            query = f"SELECT * FROM {table}" + (condition if current_state is False else "")
            db.execute(query)
            data_frame = pd.DataFrame.from_records(db.fetchall())

            if data_frame.shape[0]:
                bucket = BUCKET # TODO:make dynamic

                csv_file_name_key = f"{table}_{datetime.datetime.now(datetime.UTC)}.csv"
                csv_file_name = f"s3://{bucket}/{csv_file_name_key}"

                data_frame.to_csv(
                    csv_file_name, index=False
                )  # index=False means no dataframe index written in

                logger.info(f"Data exported to '{csv_file_name}' successfully.")
            else:
                logger.info(f"No data changes in the table {table}")
    
    except ClientError as e:
        logger.error({"message": "an error occured with s3",
                      "details": e})
        
    except psycopg2.errors.ConnectionException as e:
        logger.error({"message": "an error occured with the psycopg2 connection",
                      "details": e})
        
    except pd.errors.EmptyDataError as e:
        logger.error({"message": "an error occured with pandas",
                      "details": e})
        
    except Exception as e:
        logger.error({"message": "unknown error occured",
                      "details": e})

    else:
        if current_state is True:
            change_state(boto3.client('s3'), False) #Once first run is ending, change 'is first run' status to False.
    finally:
        close_connection(db)

