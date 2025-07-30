from psycopg2.extras import execute_values
from src.utils.logger_config import AppLogger
import pandas as pd
import time
import os
from dotenv import load_dotenv
from src.utils.db_connection import get_connection

logger = AppLogger().get_logger()

load_dotenv()
def chunked(iterable, size):
    for i in range(0, len(iterable), size):
        yield iterable[i:i + size]


def upsert_with_filter(conn, df, table_name, conflict_key='ticket_id', batch_size=1500):
    if df.empty:
        logger.info(f"No rows to UPSERT into '{table_name}'")
        return 0

    columns = df.columns.tolist()
    update_stmt = ", ".join([f"{col} = EXCLUDED.{col}" for col in columns if col != conflict_key])
    update_stmt += ", insert_date = NOW()"

    sql = f"""
        INSERT INTO {table_name} ({", ".join(columns)})
        VALUES %s
        ON CONFLICT ({conflict_key})
        DO UPDATE SET {update_stmt}
        WHERE
            {table_name}.ticket_status IS DISTINCT FROM EXCLUDED.ticket_status OR
            ({table_name}.status_s IS DISTINCT FROM EXCLUDED.status_s AND {table_name}.closed_dt_s IS DISTINCT FROM EXCLUDED.closed_dt_s) OR
            ({table_name}.status_c IS DISTINCT FROM EXCLUDED.status_c AND {table_name}.closed_dt_c IS DISTINCT FROM EXCLUDED.closed_dt_c) OR
            ({table_name}.status_qc IS DISTINCT FROM EXCLUDED.status_qc AND {table_name}.closed_date_qc IS DISTINCT FROM EXCLUDED.closed_date_qc) OR
            ({table_name}.status_u IS DISTINCT FROM EXCLUDED.status_u AND {table_name}.uploaded_date_u IS DISTINCT FROM EXCLUDED.uploaded_date_u)
        ;
    """

    total_rows = 0
    try:
        with conn.cursor() as cur:
            for batch in chunked(list(df.to_records(index=False)), batch_size):
                execute_values(cur, sql, batch)
                total_rows += len(batch)
                logger.debug(f"Upserted batch of {len(batch)} rows into '{table_name}'")
        return total_rows
    except Exception as e:
        logger.exception(f"Failed during UPSERT into '{table_name}'")
        raise


def uploader(df: pd.DataFrame):
    if df.empty:
        logger.warning("DataFrame is empty. No rows to process.")
        return

    start_time = time.time()
    logger.info("Starting data sync process...")

    # Clean ticket_id
    df['ticket_id'] = df['ticket_id'].astype(str).str.strip()
    df = df[df['ticket_id'].notnull() & (df['ticket_id'] != '')].copy()
    df = df.drop_duplicates(subset=['ticket_id'], keep='last')
    df = df.replace('', None)
    logger.debug(f"Cleaned DataFrame, remaining rows: {len(df)}")

    csv_path = os.getenv("CSV_PATH")
    os.makedirs(csv_path, exist_ok=True)
        
    conn = get_connection()
    try:
        with conn:
            # Split into in-progress and completed
            df_in_progress = df[df['ticket_status'] != 'Completed'].copy()
            df_completed = df[df['ticket_status'] == 'Completed'].copy()
            logger.info(f"Rows: In-progress = {len(df_in_progress)}, Completed = {len(df_completed)}")

            df_in_progress.to_csv(os.path.join(csv_path, 'work_in_progress.csv'), index=False)
            df_completed.to_csv(os.path.join(csv_path, 'work_completed.csv'), index=False)

            # UPSERT in-progress
            if not df_in_progress.empty:
                rows_upserted = upsert_with_filter(conn, df_in_progress, 'work_in_progress', 'ticket_id')
                logger.info(f"Upserted {rows_upserted} rows into 'work_in_progress'")

            # UPSERT completed
            if not df_completed.empty:
                rows_upserted = upsert_with_filter(conn, df_completed, 'work_completed', 'ticket_id')
                logger.info(f"Upserted {rows_upserted} rows into 'work_completed'")

                # DELETE completed from work_in_progress
                ticket_ids = tuple(df_completed['ticket_id'].tolist())
                if ticket_ids:
                    delete_query = "DELETE FROM work_in_progress WHERE ticket_id IN %s;"
                    with conn.cursor() as cur:
                        for chunk in chunked(ticket_ids, 1500):
                            cur.execute(delete_query, (tuple(chunk),))
                            logger.debug(f"Deleted batch of {len(chunk)} tickets from 'work_in_progress'")
                    logger.info(f"Deleted {len(ticket_ids)} completed tickets from 'work_in_progress'")

        elapsed = round(time.time() - start_time, 2)
        logger.info(f"Full sync completed in {elapsed} seconds.")

    except Exception as e:
        logger.exception("Sync process failed")
    finally:
        conn.close()
        logger.info("Database connection closed.")
