import psycopg2
from datetime import datetime
from src.utils.db_connection import get_connection
from src.utils.logger_config import AppLogger
from src.utils.etl_updater import get_etl_metadata, update_etl_metadata

logger = AppLogger().get_logger()

def run_delta_etl_fact_catalog_activity(conn, source_table):
    try:
        with conn.cursor() as cur:
            logger.info(f"Fetching last_loaded_at from etl_metadata for '{source_table}'...")
            
            last_loaded_at = get_etl_metadata(cur, source_table=source_table)

            logger.info(f"Running delta insert from {source_table}...")

            delta_sql = f"""
                INSERT INTO fact_catalog_activity (
                    ticket_id, vendor_name, client_id, associate_id, stage_order, stage_status, 
                    ticstatus_id, start_date, closed_date, 
                    no_of_products, no_of_categories, duration_hrs, last_updated_at
                )
                SELECT * FROM (
                    -- Stage S
                    SELECT
                        wc.ticket_id,
                        wc.vendor_name,
                        c.client_id,
                        ca.associate_id,
                        s.stage_order,
                        INITCAP(wc.status_s),
                        ts.ticket_status_id,
                        wc.start_dt_s,
                        dd.date_id,
                        wc.no_of_products_s,
                        wc.no_of_categories_s,
                        EXTRACT(EPOCH FROM (wc.closed_dt_s::timestamp - wc.start_dt_s::timestamp)) / 3600.0,
                        NOW()
                    FROM {source_table} wc
                    LEFT JOIN dim_clients c ON wc.client = c.client_name
                    LEFT JOIN dim_catalog_associates ca 
                        ON COALESCE(NULLIF(TRIM(wc.catalogue_associate), ''), 'Unassigned') = ca.associate_name
                    LEFT JOIN dim_stages s ON s.stage = 'S'
                    LEFT JOIN dim_ticket_status ts ON ts.ticket_status = wc.ticket_status
                    LEFT JOIN dim_dates dd ON dd.date_id = wc.closed_dt_s
                    WHERE wc.insert_date > %s

                    UNION ALL

                    -- Stage C
                    SELECT
                        wc.ticket_id,
                        wc.vendor_name,
                        c.client_id,
                        ca.associate_id,
                        s.stage_order,
                        INITCAP(wc.status_c),
                        ts.ticket_status_id,
                        wc.start_dt_c,
                        dd.date_id,
                        wc.no_of_products_c,
                        wc.no_of_categories_c,
                        EXTRACT(EPOCH FROM (wc.closed_dt_c::timestamp - wc.start_dt_c::timestamp)) / 3600.0,
                        NOW()
                    FROM {source_table} wc
                    LEFT JOIN dim_clients c ON wc.client = c.client_name
                    LEFT JOIN dim_catalog_associates ca 
                        ON COALESCE(NULLIF(TRIM(wc.assignee_c), ''), 'Unassigned') = ca.associate_name
                    LEFT JOIN dim_stages s ON s.stage = 'C'
                    LEFT JOIN dim_ticket_status ts ON ts.ticket_status = wc.ticket_status
                    LEFT JOIN dim_dates dd ON dd.date_id = wc.closed_dt_c
                    WHERE wc.insert_date > %s

                    UNION ALL

                    -- Stage QC
                    SELECT
                        wc.ticket_id,
                        wc.vendor_name,
                        c.client_id,
                        ca.associate_id,
                        s.stage_order,
                        INITCAP(wc.status_qc),
                        ts.ticket_status_id,
                        wc.start_dt_qc,
                        dd.date_id,
                        wc.no_of_products_u,
                        wc.no_of_categories_u,
                        EXTRACT(EPOCH FROM (wc.closed_date_qc::timestamp - wc.start_dt_qc::timestamp)) / 3600.0,
                        NOW()
                    FROM {source_table} wc
                    LEFT JOIN dim_clients c ON wc.client = c.client_name
                    LEFT JOIN dim_catalog_associates ca 
                        ON COALESCE(NULLIF(TRIM(wc.assignee_qc), ''), 'Unassigned') = ca.associate_name
                    LEFT JOIN dim_stages s ON s.stage = 'QC'
                    LEFT JOIN dim_ticket_status ts ON ts.ticket_status = wc.ticket_status
                    LEFT JOIN dim_dates dd ON dd.date_id = wc.closed_date_qc
                    WHERE wc.insert_date > %s

                    UNION ALL

                    -- Stage U
                    SELECT
                        wc.ticket_id,
                        wc.vendor_name,
                        c.client_id,
                        ca.associate_id,
                        s.stage_order,
                        INITCAP(wc.status_u),
                        ts.ticket_status_id,
                        wc.start_dt_u,
                        dd.date_id,
                        wc.no_of_products_u,
                        wc.no_of_categories_u,
                        EXTRACT(EPOCH FROM (wc.uploaded_date_u::timestamp - wc.start_dt_u::timestamp)) / 3600.0,
                        NOW()
                    FROM {source_table} wc
                    LEFT JOIN dim_clients c ON wc.client = c.client_name
                    LEFT JOIN dim_catalog_associates ca 
                        ON COALESCE(NULLIF(TRIM(wc.assignee_u), ''), 'Unassigned') = ca.associate_name
                    LEFT JOIN dim_stages s ON s.stage = 'U'
                    LEFT JOIN dim_ticket_status ts ON ts.ticket_status = wc.ticket_status
                    LEFT JOIN dim_dates dd ON dd.date_id = wc.uploaded_date_u
                    WHERE wc.insert_date > %s
                ) AS subquery

                ON CONFLICT (ticket_id, stage_order) DO UPDATE SET
                    client_id = EXCLUDED.client_id,
                    associate_id = EXCLUDED.associate_id,
                    stage_status = EXCLUDED.stage_status,
                    ticstatus_id = EXCLUDED.ticstatus_id,
                    start_date = EXCLUDED.start_date,
                    closed_date = EXCLUDED.closed_date,
                    no_of_products = EXCLUDED.no_of_products,
                    no_of_categories = EXCLUDED.no_of_categories,
                    duration_hrs = EXCLUDED.duration_hrs,
                    last_updated_at = NOW();
            """

            cur.execute(delta_sql, (last_loaded_at,) * 4)
            
            logger.info(f"{cur.rowcount} rows inserted/updated from {source_table}")

            # Step 3: Update metadata
            update_etl_metadata(cur, source_table=source_table)

            conn.commit()
            logger.info(f"Metadata updated for '{source_table}'. ETL completed.\n")

    except Exception as e:
        conn.rollback()
        logger.exception(f"ETL failed for '{source_table}': {e}")

def sync_data_with_bucket_data(conn):
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE fact_catalog_activity f
                SET ticstatus_id = 5
                FROM file_tracker ft
                WHERE f.vendor_name = ft.filename
                AND ft.status = 'Success'
                AND f.ticstatus_id != 5;
            """)
        conn.commit()
        logger.info("Syncing is completed.............")
    except Exception as e:
        logger.exception("Error syncing data with bucket data: %s", e)


def update_fact_table():
    conn = get_connection()
    try:
        for table in ('work_completed', 'work_in_progress'):
            run_delta_etl_fact_catalog_activity(conn, table)
        logger.info("Trying to Sync Data with Bucket Data")
        sync_data_with_bucket_data(conn)
    finally:
        conn.close()
