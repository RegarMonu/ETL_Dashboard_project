from dotenv import load_dotenv
import psycopg2
import os

load_dotenv()

DB_CONFIG = {
    'dbname': os.getenv("DB_NAME"),
    'user': os.getenv("DB_USER"),
    'password': os.getenv("DB_PASSWD"),
    'host': os.getenv("DB_HOST"),
    'port': os.getenv("DB_PORT")
}

# Step 2: Connect to PostgreSQL
def get_connection():
    return psycopg2.connect(**DB_CONFIG)