import pandas as pd
import boto3
def read_csv_to_df(list):
    """This function reads .csv files from the s3 bucket and transforms them to panda datasets
    so that we can run queries on the data"""
    return ''
#     for key in list:
#         df_name = f'df_{key}'
#         df = pd.read_csv(key)
#         df_name = df
#         yield df_name

# def next_function(list):
#     for key in range(len(list)):
#         #run queries


# loop through the list and use keys in the list to find files in the bucket
# if key is in bucket: read csv (turn into df)
# yield each df
# code includes the ingest bucket(hardcode)

# input is a list of keys
# output is dataframe-

# how wil be handle the main bulk of info in initial files versus the updates in newer files
# how will we hold the df in memory