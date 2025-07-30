from dotenv import load_dotenv
import psycopg2
import os
from src.utils.logger_config import AppLogger

# Load environment variables
load_dotenv()

# Set up logger
logger = AppLogger().get_logger()

# Prepare DB config from .env
DB_CONFIG = {
    'dbname': os.getenv("DB_NAME"),
    'user': os.getenv("DB_USER"),
    'password': os.getenv("DB_PASSWD"),
    'host': os.getenv("DB_HOST"),
    'port': os.getenv("DB_PORT")
}


def get_connection():
    """Establish and return a PostgreSQL DB connection."""
    try:
        logger.info(f"Attempting database connection to host={DB_CONFIG['host']} port={DB_CONFIG['port']} dbname={DB_CONFIG['dbname']}")
        conn = psycopg2.connect(**DB_CONFIG)
        logger.info("Database connection established successfully")
        return conn
    except psycopg2.OperationalError as e:
        logger.exception("Operational error while connecting to the database")
        raise
    except Exception as e:
        logger.exception("Unexpected error during database connection")
        raise
