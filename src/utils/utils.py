import pandas as pd
import boto3
from dotenv import load_dotenv
import os
import logging
from io import BytesIO
from botocore.exceptions import ClientError

load_dotenv()
BUCKET = os.environ['BUCKET']

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def read_csv_to_df(key_list, s3_client, origin_bucket = BUCKET):
    '''This function reads .csv files from the s3 bucket and transforms them to panda datasets
    so that we can run queries on the data
    Args: list of strings (keys)

    Return: pd DataFrame
    '''
    
    if not key_list:
        raise IndexError("Argument list must not be empty.")
    
    for key in key_list:
        try:
            response = s3_client.get_object(Bucket=origin_bucket, Key=key)
            df = pd.read_csv(BytesIO(response.get("Body").read()))
            yield {key : df}

        except ClientError as e:
            logger.error({"message": f"The key '{key}' does not exist in bucket '{BUCKET}'.", 
                          "details": e}
                )
            
def df_to_parquet(df):
    buffer = BytesIO()
    df.to_parquet(buffer)
    # buffer.getvalue() should retrieve whole value without having to reset position
    # but if not (in next funct) add buffer.seek(0)
    buffer_value = buffer.getvalue()  # takes value from the buffer as byte object
    return buffer_value
# we can use index=False inside df.to_parquet(buffer) if we don't want to preserve df indices?
# TODO:check with team
# TODO:add pyarrow to requirements? needed 'pip install pyarrow' to test
# if we need to need to read byte object- BytesIO(buffer_value)
# s3_client.put_object(Bucket=origin_bucket, Key=key, Body=buffer_value)
# ^^needs key, bucket and client as args
            
           
                
       