import psycopg2 
from psycopg2.extras import RealDictCursor
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
    return db.cursor()

def close_connection(db):
    db.close()

