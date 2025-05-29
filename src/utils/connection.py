from pg8000.native import Connection
from dotenv import load_dotenv
import os

load_dotenv()

def create_connection():

    database = os.environ["PG_DATABASE"]
    username = os.environ["PG_USERNAME"]
    password = os.environ["PG_PASSWORD"]
    host = os.environ["PG_HOST"]

    con = Connection(
        username,
        database=database,
        password=password,
        host=host,
        port=5432
    )
    return con

def close_connection(con):
    con.close()
