import psycopg2
from psycopg2.extras import execute_batch
import pandas as pd
from src.utils.db_connection import get_connection
from src.utils.logger_config import AppLogger  # adjust if path differs

logger = AppLogger().get_logger()

def upload(df: pd.DataFrame):
    if df.empty:
        logger.warning("Provided DataFrame is empty. No records to UPSERT.")
        return

    columns = df.columns.tolist()
    placeholders = ", ".join(["%s"] * len(columns))

    update_stmt = ", ".join([
        f"{col} = EXCLUDED.{col}" for col in columns if col != 'file_id'
    ])

    sql = f"""
        INSERT INTO file_tracker ({", ".join(columns)})
        VALUES ({placeholders})
        ON CONFLICT (file_id)
        DO UPDATE SET {update_stmt}
        WHERE 
            (
                EXCLUDED.status IS DISTINCT FROM file_tracker.status
            );
    """

    # Convert DataFrame to list of tuples
    data = [tuple(row[col] for col in columns) for _, row in df.iterrows()]

    conn = get_connection()

    try:
        with conn:
            with conn.cursor() as cur:
                execute_batch(cur, sql, data)
        logger.info(f"UPSERT completed successfully for folder data. Rows: {len(data)}")
    
    except Exception as e:
        logger.exception("UPSERT failed for file_tracker table.")

    finally:
        conn.close()
        logger.debug("Database connection closed after UPSERT.")
