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
            
           
                

def create_sales_fact(df):
    df["created_date"] = pd.to_datetime(df["created_at"]).dt.date
    df["created_time"] = pd.to_datetime(df["created_at"]).dt.time
    df.drop(["created_at"], axis="columns", inplace=True)
    df["last_updated_date"] = pd.to_datetime(df["last_updated"]).dt.date
    df["last_updated_time"] = pd.to_datetime(df["last_updated"]).dt.time
    df.drop(["last_updated"], axis="columns", inplace=True)
    return df


def create_location_dim(df):
    df.drop(["created_at", "last_updated"], axis="columns", inplace=True)
    df.rename_axis("location_id", inplace=True)
    return df


def create_design_dim(df):
    df.drop(["created_at", "last_updated"], axis="columns", inplace=True)
    return df


def create_currency_dim(df):
    df.drop(["created_at", "last_updated"], axis="columns", inplace=True)
    df["currency_name"] = ["Great British Pounds", "US Dollars", "Euros"]
    return df


def create_counterparty_dim(df_counterparty, df_address):
    # This will need the original address dataframe so assign that to a variable outside the for loop once you have it
    df_counterparty.drop(
        ["commercial_contact", "delivery_contact", "created_at", "last_updated"],
        axis="columns",
        inplace=True,
    )
    df = df_counterparty.merge(df_address, left_on="legal_address_id", right_index=True)
    df.drop(["created_at", "last_updated"], axis="columns", inplace=True)
    df.rename(
        columns={
            "legal_address_id": "counterparty_legal_address_id",
            "address_line_1": "counterparty_address_line_1",
            "address_line_2": "counterparty_address_line_2",
            "district": "counterparty_district",
            "city": "counterparty_city",
            "postal_code": "counterparty_postal_code",
            "country": "counterparty_country",
            "phone": "counterparty_phone",
        },
        inplace=True,
    )
    return df

def create_staff_dim(df_staff, df_department):
    # This will need the original department dataframe so assign that to a variable outside the for loop once you have it
    df_staff.drop(["created_at", "last_updated"], axis="columns", inplace=True)
    df_department.drop(
        ["manager", "created_at", "last_updated"], axis="columns", inplace=True
    )
    df = df_staff.merge(df_department, left_on="department_id", right_index=True)
    df.drop(["department_id"], axis="columns", inplace=True)
    return df


def create_date_dim():
    dates = pd.date_range("2022-11-01", "2025-12-31")
    df = pd.DataFrame(
        {
            "date_id": dates,
            "year": dates.year,
            "month": dates.month,
            "day": dates.day,
            "day_of_week": dates.dayofweek,
            "day_name": dates.day_name(),
            "month_name": dates.month_name(),
            "quarter": dates.quarter,
        }
    )
    df.set_index("date_id", inplace=True)
    return df


facts_and_dim = {
    "sales_fact": create_sales_fact,
    "counterparty_dim": create_counterparty_dim,
    "currency_dim": create_currency_dim,
    "date_dim": create_date_dim,
    "design_dim": create_design_dim,
    "address_dim": create_location_dim, #I know the name is different but plz don't change it
    "staff_dim": create_staff_dim
}