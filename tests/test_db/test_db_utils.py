from pg8000.native import identifier
from decimal import Decimal
import json
from datetime import datetime
from src.utils.connection import pg8000_connect_to_oltp, close_connection


def convert_datetime(obj):
    if isinstance(obj, dict):
        return {k: convert_datetime(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_datetime(elem) for elem in obj]
    elif isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, Decimal):
        return float(obj)  # or str(obj) if you prefer
    return obj


def format_response(columns, data, label):
    if len(data) > 1:
        formatted_data = [dict(zip(columns, row)) for row in data]
    else:
        formatted_data = dict(zip(columns, data[0]))
    return {label: formatted_data}


def extract_tables():
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
    conn = pg8000_connect_to_oltp()

    for table in table_list:
        query = f"""
        SELECT * FROM {identifier(table)}
        LIMIT 5
        """
        response = conn.run(query)
        columns = [column["name"] for column in conn.columns]
        response = format_response(columns, response, f"{table}")
        converted_response = convert_datetime(response)

        with open(f"tests/test_db/data/{table}.json", "w") as write_file:
            json.dump(converted_response, write_file, indent=4, default=str)
    close_connection(conn)


extract_tables()
