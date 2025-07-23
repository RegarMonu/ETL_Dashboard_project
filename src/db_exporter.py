from src import db_connection
from psycopg2.extras import execute_values
import pandas as pd
import time


def chunked(iterable, size):
    for i in range(0, len(iterable), size):
        yield iterable[i:i + size]


def upsert_with_filter(conn, df, table_name, conflict_key='ticket_id', batch_size=1500):
    if df.empty:
        print(f"No rows to UPSERT into {table_name}")
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
    with conn.cursor() as cur:
        for batch in chunked(list(df.to_records(index=False)), batch_size):
            execute_values(cur, sql, batch)
            total_rows += len(batch)

    return total_rows


def uploader(df: pd.DataFrame):
    if df.empty:
        print("DataFrame is empty. No rows to process.")
        return

    start_time = time.time()
    # Clean the ticket_id column
    df['ticket_id'] = df['ticket_id'].astype(str).str.strip()
    df = df[df['ticket_id'].notnull() & (df['ticket_id'] != '')].copy()
    df = df.drop_duplicates(subset=['ticket_id'], keep='last')
    df = df.replace('', None)


    conn = db_connection.get_connection()
    try:
        with conn:
            # Split DataFrame
            df_in_progress = df[df['ticket_status'] != 'Completed'].copy()
            df_completed = df[df['ticket_status'] == 'Completed'].copy()

            df_in_progress.to_csv('work_in_progress.csv')
            df_completed.to_csv('work_completed.csv')

            # UPSERT work_in_progress
            if not df_in_progress.empty:
                rows_upserted = upsert_with_filter(conn, df_in_progress, 'work_in_progress', 'ticket_id')
                print(f"UPSERTED {rows_upserted} rows to work_in_progress (out of {len(df_in_progress)}).")

            # UPSERT work_completed
            if not df_completed.empty:
                rows_upserted = upsert_with_filter(conn, df_completed, 'work_completed', 'ticket_id')
                print(f"UPSERTED {rows_upserted} rows to work_completed (out of {len(df_completed)}).")

                # DELETE completed from work_in_progress
                ticket_ids = tuple(df_completed['ticket_id'].tolist())
                if ticket_ids:
                    delete_query = """
                        DELETE FROM work_in_progress WHERE ticket_id IN %s;
                    """
                    with conn.cursor() as cur:
                        for chunk in chunked(ticket_ids, 1500):
                            cur.execute(delete_query, (tuple(chunk),))
                    print(f"🗑️ Removed {len(ticket_ids)} completed tickets from work_in_progress.")

        elapsed = round(time.time() - start_time, 2)
        print(f"Full sync complete in {elapsed} seconds.")

    except Exception as e:
        print("Sync failed:", e)
    finally:
        conn.close()
