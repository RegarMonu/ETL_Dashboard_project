from src.utils.db_connection import get_connection
from src.utils.logger_config import AppLogger

logger = AppLogger().get_logger()

def materialized_view_refresh():
    """Refreshes the materialized views in the database."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            logger.info("Refreshing materialized view: kpi_table")
            cur.execute("REFRESH MATERIALIZED VIEW kpi_table;")

            logger.info("Refreshing materialized view: kpi_table2")
            cur.execute("REFRESH MATERIALIZED VIEW kpi_table2;")

        conn.commit()
        logger.info("Materialized views refreshed successfully.")
    except Exception as e:
        logger.exception("Failed to refresh materialized views: %s", e)
    finally:
        conn.close()
