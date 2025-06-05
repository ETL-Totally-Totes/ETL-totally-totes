import pandas as pd

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
    "location_dim": create_location_dim,
    "staff_dim": create_staff_dim
}