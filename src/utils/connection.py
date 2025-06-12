import psycopg2
from pg8000.native import Connection
import os
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv(override=True)

database = os.environ["PG_DATABASE"]
username = os.environ["PG_USERNAME"]
password = os.environ["PG_PASSWORD"]
host = os.environ["PG_HOST"]


def create_connection():
    """
    Establishes a connection to the main PostgreSQL database using psycopg2.

    Returns:
       A live psycopg2.extensions.connection.
    """
    db = psycopg2.connect(
        database=database,
        user=username,
        password=password,
        host=host,
        cursor_factory=psycopg2.extras.RealDictCursor,
    )
    return db


def create_connection_to_local():
    """
    Establishes a connection to a test PostgreSQL database using 'test' environment variables.

    Returns:
        psycopg2.extensions.connection: A connection object for the local test DB.
    """
    db = psycopg2.connect(
        user=os.getenv("TEST_PG_USERNAME"),
        database=os.getenv("TEST_PG_DATABASE"),
        password=os.getenv("TEST_PG_PASSWORD"),
        # host=os.getenv("TEST_PG_HOST"),
        port=int(os.getenv("TEST_PG_PORT")),
        cursor_factory=psycopg2.extras.RealDictCursor,
    )
    return db


def pg8000_connect_to_oltp():
    """
    Connects to the main PostgreSQL database using pg8000.

    Returns:
        pg8000.native.Connection: A pg8000 connection instance to the main DB.
    """
    load_dotenv(override=True)
    return Connection(
        user=username,
        database=database,
        password=password,
        host=host,
    )


def pg8000_connect_to_local():
    """
    Connects to the test PostgreSQL database using pg8000.

    Returns:
        pg8000.native.Connection: A pg8000 connection instance to the test/local DB.
    """
    load_dotenv(override=True)
    return Connection(
        user=os.getenv("TEST_PG_USERNAME"),
        database=os.getenv("TEST_PG_DATABASE"),
        password=os.getenv("TEST_PG_PASSWORD"),
        # host=os.getenv("TEST_PG_HOST"),
        port=int(os.getenv("TEST_PG_PORT")),
    )


def close_connection(db):
    """
    Closes the given database connection.

    Args:
        db: An open database connection object.
    """
    db.close()
