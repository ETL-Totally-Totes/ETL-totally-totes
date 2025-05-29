import psycopg2 
from dotenv import load_dotenv
import os

load_dotenv()

def create_connection():

    database = os.environ["PG_DATABASE"]
    username = os.environ["PG_USERNAME"]
    password = os.environ["PG_PASSWORD"]
    host = os.environ["PG_HOST"]

    db = psycopg2.connect(
            database="totesys",
            user="project_team_06",
            password="fZUyorR8LdL8uBU",
            host="nc-data-eng-totesys-production.chpsczt8h1nu.eu-west-2.rds.amazonaws.com",
        )
    return db

def close_connection(db):
    db.close()

