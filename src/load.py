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
BUCKET = os.environ['TRANSFORM_BUCKET']
PG_CONNECTION = os.environ['PG_CONNECTION']

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class ReadParquetError(BaseException):
    pass
class WriteDataFrameError(BaseException):
    pass


def read_parquet_from_s3(s3_client, key, bucket=BUCKET): #returns panda df
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)  # returns json
        parquet_bytes = response['Body'].read()
        df = pd.read_parquet(BytesIO(parquet_bytes))  # bytes to df
        return df
    except botocore.exceptions.ClientError as e:
        logger.error(f"ClientError while accessing S3: {e}")
        raise ReadParquetError
    except Exception as e:
        logger.error(f"Unexpected exception when reading parquet from S3: {e}")
        raise ReadParquetError

def write_dataframe_to_postgres(df, table_name):

    if df.empty:
        logger.warning(f"DataFrame for table '{table_name}' is empty. No data will be written.")
        return
    try:
        engine = create_engine(PG_CONNECTION)
        with engine.begin() as connection:
            df.to_sql(
                table_name,
                con=connection,
                if_exists='append',  
                method='multi'       # multi’: Pass multiple values in a single INSERT clause. (Faster)
            )
            # engine.begin() commits automatically
    except SQLAlchemyError as e:
        logger.error(f"SQLAlchemyError: Failed to write DataFrame to PostgreSQL table '{table_name}': {e}")
        raise WriteDataFrameError
    except Exception as e:
        logger.error(f"Unexpected error while writing DataFrame to PostgreSQL table '{table_name}': {e}")
        raise WriteDataFrameError


def load_handler(event, context):
    keys = event['s3_keys']

    if keys:
        for key in keys:
            try:
                df = read_parquet_from_s3(boto3.client("s3"), key)
                if 'addr' in key:
                    table_name =  "dim_location"
                    write_dataframe_to_postgres(df, table_name)
                elif 'coun' in key:
                    table_name =  "dim_counterparty"
                    write_dataframe_to_postgres(df, table_name)
                elif 'curr' in key:
                    table_name =  "currency"
                    write_dataframe_to_postgres(df, table_name)
                elif 'desi' in key:
                    table_name =  "dim_design"
                    write_dataframe_to_postgres(df, table_name)
                elif 'sale' in key:
                    table_name =  "fact_sales_order"
                    write_dataframe_to_postgres(df, table_name)
                elif 'staf' in key:
                    table_name =  "dim_staff"
                    write_dataframe_to_postgres(df, table_name)
                elif 'date' in key:
                    table_name =  "dim_date"
                    write_dataframe_to_postgres(df, table_name)
            except ReadParquetError:
                logger.error('Error occured when running read_parquet_from_s3()')
            except WriteDataFrameError:
                logger.error('Error occured when running write_dataframe_to_postgres()')
            except botocore.exceptions.ClientError as e:
                logger.error(f"ClientError while accessing S3: {e}")
            except Exception as e:
                logger.error(f"Unexpected exception when reading parquet from S3: {e}")         
        return {
        "Success": f'Successfully loaded records!'
    }
    else: 
        return {"Message": "No new data to append"}
        