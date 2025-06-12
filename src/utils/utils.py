import pandas as pd
import os
import logging
from io import BytesIO
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()
BUCKET = os.environ["BUCKET"]

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def read_csv_to_df(key_list, s3_client, origin_bucket=BUCKET):
    """
    Reads CSV files from S3 and converts each into a Pandas DataFrame.

    Args:
        key_list (list): List of S3 object keys (file paths).
        s3_client (boto3.client): An active boto3 S3 client.
        origin_bucket (str): Name of the S3 bucket to read from (default is BUCKET from .env).

    Yields:
        dict: A dictionary with the key name and the corresponding DataFrame.
    """

    if not key_list:
        raise IndexError("Argument list must not be empty.")

    for key in key_list:
        try:
            response = s3_client.get_object(Bucket=origin_bucket, Key=key)
            df = pd.read_csv(BytesIO(response.get("Body").read()), index_col=0)
            yield {key: df}

        except ClientError as e:
            logger.error(
                {
                    "message": f"The key '{key}' does not exist in bucket '{BUCKET}'.",
                    "details": e,
                }
            )


def df_to_parquet(df):
    """
    Converts a DataFrame to a Parquet file in memory.

    Args:
        df (pd.DataFrame): The DataFrame to convert.

    Returns:
        buffer_value (bytes): A byte string representing the Parquet data.
    """
    buffer = BytesIO()
    df.to_parquet(buffer)
    buffer_value = buffer.getvalue()  # takes value from the buffer as byte object
    return buffer_value


def create_sales_fact(df):
    """
    Prepares the sales fact table by extracting date and time from datetime columns.

    Args:
        df (pd.DataFrame): Raw sales data as panda dataframe.

    Returns:
        pd.DataFrame: Transformed DataFrame with split datetime columns.
    """
    df["created_date"] = pd.to_datetime(df["created_at"]).dt.date
    df["created_time"] = pd.to_datetime(df["created_at"]).dt.time
    df.drop(["created_at"], axis="columns", inplace=True)
    df["last_updated_date"] = pd.to_datetime(df["last_updated"]).dt.date
    df["last_updated_time"] = pd.to_datetime(df["last_updated"]).dt.time
    df.drop(["last_updated"], axis="columns", inplace=True)
    return df


def create_location_dim(df):
    """
    Prepares the data for location dimension table.

    Args:
        df (pd.DataFrame): Raw address data.

    Returns:
        pd.DataFrame: Cleaned DataFrame with appropriate index set.
    """
    df.drop(["created_at", "last_updated"], axis="columns", inplace=True)
    df.rename_axis("location_id", inplace=True)
    return df


def create_design_dim(df):
    """
    Prepares the data for design dimensions table.

    Args:
        df (pd.DataFrame): Raw design data.

    Returns:
        pd.DataFrame: Cleaned design data.
    """
    df.drop(["created_at", "last_updated"], axis="columns", inplace=True)
    return df


def create_currency_dim(df):
    """
    Prepares the currency dimension and adds readable currency names

    Args: df (pd.DataFrame): Raw currency data

    Returns: pd.Dataframe: Cleaned and enriched currency data
    """
    df.drop(["created_at", "last_updated"], axis="columns", inplace=True)
    df["currency_name"] = ["Great British Pounds", "US Dollars", "Euros"]
    return df


def create_counterparty_dim(df_counterparty, df_address):
    """
    Merges counterparty and address data into a single dataframe.

    Args:
        df_counterparty (pd.DataFrame): Raw counterparty data.
        df_address (pd.DataFrame): Cleaned location data.

    Returns:
        pd.DataFrame: Combined counterparty datafame.
    """
    # This will need the original address dataframe so assign that to a variable outside the for loop once you have it
    df_counterparty.drop(
        ["commercial_contact", "delivery_contact", "created_at", "last_updated"],
        axis="columns",
        inplace=True,
    )
    df = df_counterparty.merge(df_address, left_on="legal_address_id", right_index=True)
    # df.drop(["created_at", "last_updated"], axis="columns", inplace=True)
    # df.drop("legal_address_id", inplace=True)
    df.rename(
        columns={
            # "legal_address_id": "counterparty_legal_address_id",
            "address_line_1": "counterparty_legal_address_line_1",
            "address_line_2": "counterparty_legal_address_line_2",
            "district": "counterparty_legal_district",
            "city": "counterparty_legal_city",
            "postal_code": "counterparty_postal_code",
            "country": "counterparty_legal_country",
            "phone": "counterparty_legal_phone_number",
        },
        inplace=True,
    )

    return df


def create_staff_dim(df_staff, df_department):
    """
    Joins staff and department data to build data for the staff dimension.

    Args:
        df_staff (pd.DataFrame): Raw staff data.
        df_department (pd.DataFrame): Cleaned department data.

    Returns:
        pd.DataFrame: Staff data with department info merged in.
    """
    df_staff.drop(["created_at", "last_updated"], axis="columns", inplace=True)
    df_department.drop(
        ["manager", "created_at", "last_updated"], axis="columns", inplace=True
    )
    df = df_staff.merge(df_department, left_on="department_id", right_index=True)
    df.drop(["department_id"], axis="columns", inplace=True)
    return df


def create_date_dim():
    """
    Creates a static date dimension covering Nov 2022 to Dec 2025.

    Returns:
        pd.DataFrame: Date dimension with fields for year, month, weekday, etc.
    """
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
    "address_dim": create_location_dim,  # I know the name is different but plz don't change it
    "staff_dim": create_staff_dim,
}
