from src.utils.db_connection import get_connection
from src.utils.logger_config import AppLogger

logger = AppLogger().get_logger()

def materialized_view_refresh():
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            logger.info("Refreshing materialized view: kpi_table")
            cur.execute("""
                REFRESH MATERIALIZED VIEW kpi_table;
            """)
        conn.commit()
        logger.info("Materialized view refreshed successfully.")
    except Exception as e:
        logger.exception("Failed to refresh materialized view")
    finally:
        conn.close()
