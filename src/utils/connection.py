import psycopg2 
from psycopg2.extras import RealDictCursor
from pg8000.native import Connection
from dotenv import load_dotenv
import os

load_dotenv(override=True)

database = os.environ["PG_DATABASE"]
username = os.environ["PG_USERNAME"]
password = os.environ["PG_PASSWORD"]
host = os.environ["PG_HOST"]

def create_connection():
    db = psycopg2.connect(
            database=database,
            user=username,
            password=password,
            host=host,
            cursor_factory=RealDictCursor
        )
    return db

def create_connection_to_local():
    db = psycopg2.connect(
            user=os.getenv("TEST_PG_USERNAME"),
            database=os.getenv("TEST_PG_DATABASE"),
            password=os.getenv("TEST_PG_PASSWORD"),
            # host=os.getenv("TEST_PG_HOST"),
            port=int(os.getenv("TEST_PG_PORT")),
            cursor_factory=RealDictCursor
        )
    return db



def pg8000_connect_to_oltp():
    load_dotenv(override=True)
    return Connection(
        user=username,
        database=database,
        password=password,
        host=host,
    )

def pg8000_connect_to_local():
    load_dotenv(override=True)
    return Connection(
        user=os.getenv("TEST_PG_USERNAME"),
        database=os.getenv("TEST_PG_DATABASE"),
        password=os.getenv("TEST_PG_PASSWORD"),
        # host=os.getenv("TEST_PG_HOST"),
        port=int(os.getenv("TEST_PG_PORT"))
    )



def close_connection(db):
    db.close()
