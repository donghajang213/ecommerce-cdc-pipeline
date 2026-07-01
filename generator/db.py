import os
import time

import psycopg2

DB_HOST = os.environ.get("DB_HOST", "postgres")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_NAME = os.environ.get("DB_NAME", "shop")
DB_USER = os.environ.get("DB_USER", "appuser")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "apppass")


def get_connection(retries: int = 20, delay: float = 3.0):
    last_err = None
    for _ in range(retries):
        try:
            conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
            )
            conn.autocommit = True
            return conn
        except psycopg2.OperationalError as err:
            last_err = err
            time.sleep(delay)
    raise RuntimeError(f"Could not connect to Postgres after {retries} retries") from last_err
