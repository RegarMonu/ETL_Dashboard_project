import psycopg2
from psycopg2.extras import execute_batch
import pandas as pd
from src import db_connection


def upload(df):
    columns = df.columns.tolist()
    
    placeholders = ", ".join(["%s"] * len(columns))

    update_stmt = ", ".join([
        f"{col} = EXCLUDED.{col}" for col in columns
    ])

    # ✅ Corrected SQL
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

    # Execute
    conn = db_connection.get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                execute_batch(cur, sql, data)
        print("✅ UPSERT completed successfully For Folder Data.")
    except Exception as e:
        print("UPSERT failed:", e)
    finally:
        conn.close()
