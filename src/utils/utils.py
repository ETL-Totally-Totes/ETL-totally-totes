import pandas as pd
import boto3
from dotenv import load_dotenv
import os
from io import BytesIO

load_dotenv()
BUCKET = os.environ['BUCKET']

def read_csv_to_df(key_list, s3_client, origin_bucket = BUCKET):
    '''This function reads .csv files from the s3 bucket and transforms them to panda datasets
    so that we can run queries on the data
    Args: function retrieving list of strings

    Return: pd DataFrame
    '''
    for key in key_list:
        response = s3_client.get_object(Bucket=origin_bucket, Key=key)
        # csv_content = response['Body'].read().decode('utf-8')
        df = pd.read_csv(BytesIO(response.get("Body").read()))
        yield {key : df}
    
#the next function is going to do queries and clean data from each db and transform 
# def next_function(list):
#     '''This function iterates though a list of DataFrames, cleans the data and transforms it into parquet.
#     It finally puts the parquet into a s3 bucket.

#     Arg: list of strings (s3 object keys)

#     Return: parquet files
#     '''
#     for result in read_csv_to_df(list):
#         #7 tables - needed for the MVP
#         #7 queries
#         #clean the data of these tables

#EXAMPLE FROM DOCS
# >>> df = pd.DataFrame(data={'col1': [1, 2], 'col2': [3, 4]})
# >>> df.to_parquet('df.parquet.gzip',
# ...               compression='gzip')  
# >>> pd.read_parquet('df.parquet.gzip')  
#    col1  col2
# 0     1     3
# 1     2     4


# dim_staff
# dim_location
# dim_design
# dim_date
# dim_currency
# dim_counterparty
# fact_sales_order


# loop through the list and use keys in the list to find files in the bucket
# if key is in bucket: read csv (turn into df)
# yield each df
# code includes the ingest bucket(hardcode)

# how wil be handle the main bulk of info in initial files versus the updates in newer files
# how will we hold the df in memory

#Functions our read_csv_to_df needs:
#get_logs (grabs the logs)
#get_csv_file_keys (returns the list of keys)