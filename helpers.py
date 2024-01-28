""" Module to connect to PostgreSQL database"""

import os
import psycopg2
from sqlalchemy import create_engine
from dotenv import load_dotenv
load_dotenv()



def get_database_engine(local_dev=True):
    """ Generate sqlalchemy db connection for use with Pandas"""
    if local_dev:
        dburl = os.getenv("LOCAL_DATABASE_URL")
    else:
        dburl = os.getenv("DATABASE_URL_NEW")

    if dburl.startswith("postgres:"):
        dburl = "postgresql" + dburl[len("postgres"):]

    engine = create_engine(dburl)
    return engine
