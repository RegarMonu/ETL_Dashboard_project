import psycopg2
from src.utils.db_connection import get_connection
from src.utils.logger_config import AppLogger  # Adjust path as per your project structure

logger = AppLogger().get_logger()


'''
"query": """
                INSERT INTO dim_catalog_associates (associate_name)
                SELECT DISTINCT associate_name
                FROM (
                    SELECT catalogue_associate AS associate_name FROM work_completed WHERE catalogue_associate IS NOT NULL AND catalogue_associate ~ '[A-Za-z]'
                    UNION
                    SELECT assignee_s FROM work_completed WHERE assignee_s IS NOT NULL AND assignee_s ~ '[A-Za-z]'
                    UNION
                    SELECT assignee_c FROM work_completed WHERE assignee_c IS NOT NULL AND assignee_c ~ '[A-Za-z]'
                    UNION
                    SELECT assignee_u FROM work_completed WHERE assignee_u IS NOT NULL AND assignee_u ~ '[A-Za-z]'
                    UNION
                    SELECT assignee_qc FROM work_completed WHERE assignee_qc IS NOT NULL AND assignee_qc ~ '[A-Za-z]'
                    UNION
                    SELECT catalogue_associate FROM work_in_progress WHERE catalogue_associate IS NOT NULL AND catalogue_associate ~ '[A-Za-z]'
                    UNION
                    SELECT assignee_s FROM work_in_progress WHERE assignee_s IS NOT NULL AND assignee_s ~ '[A-Za-z]'
                    UNION
                    SELECT assignee_c FROM work_in_progress WHERE assignee_c IS NOT NULL AND assignee_c ~ '[A-Za-z]'
                    UNION
                    SELECT assignee_u FROM work_in_progress WHERE assignee_u IS NOT NULL AND assignee_u ~ '[A-Za-z]'
                    UNION
                    SELECT assignee_qc FROM work_in_progress WHERE assignee_qc IS NOT NULL AND assignee_qc ~ '[A-Za-z]'
                ) AS combined_associates
                ON CONFLICT (associate_name) DO NOTHING;
            """
        },
'''
def update_client_associate_data():
    conn = get_connection()

    sql_queries = [
        {
            "description": "Insert catalog associates",
            "query": """
                INSERT INTO dim_catalog_associates (associate_name)
                SELECT DISTINCT COALESCE(associate_name, 'Unassigned') AS associate_name
                FROM (
                    SELECT catalogue_associate AS associate_name FROM work_completed WHERE catalogue_associate ~ '[A-Za-z]'
                    UNION
                    SELECT assignee_s FROM work_completed WHERE assignee_s ~ '[A-Za-z]'
                    UNION
                    SELECT assignee_c FROM work_completed WHERE assignee_c ~ '[A-Za-z]'
                    UNION
                    SELECT assignee_u FROM work_completed WHERE assignee_u ~ '[A-Za-z]'
                    UNION
                    SELECT assignee_qc FROM work_completed WHERE assignee_qc ~ '[A-Za-z]'
                    UNION
                    SELECT catalogue_associate FROM work_in_progress WHEREcatalogue_associate ~ '[A-Za-z]'
                    UNION
                    SELECT assignee_s FROM work_in_progress WHERE assignee_s ~ '[A-Za-z]'
                    UNION
                    SELECT assignee_c FROM work_in_progress WHERE assignee_c ~ '[A-Za-z]'
                    UNION
                    SELECT assignee_u FROM work_in_progress WHERE assignee_u ~ '[A-Za-z]'
                    UNION
                    SELECT assignee_qc FROM work_in_progress WHERE assignee_qc ~ '[A-Za-z]'
                ) AS combined_associates
                ON CONFLICT (associate_name) DO NOTHING;
            """
        },
        {
            "description": "Insert distinct clients",
            "query": """
                INSERT INTO dim_clients (client_name)
                SELECT DISTINCT client
                FROM (
                    SELECT client FROM work_in_progress
                    UNION
                    SELECT client FROM work_completed
                ) AS combined_clients
                ON CONFLICT (client_name) DO NOTHING;
            """
        },
        {
            "description": "Insert team",
            "query": """
                INSERT INTO dim_teams (team_name, team_lead)
                VALUES ('vipani team A', 'Arun')
                ON CONFLICT (team_name) DO NOTHING;
            """
        },
        {
            "description": "Map associates to team",
            "query": """
                INSERT INTO associate_team_map (associate_name, team_name)
                VALUES 
                    ('Reshma', 'vipani team A'),
                    ('Vyshnavi', 'vipani team A'),
                    ('Dinesh', 'vipani team A'),
                    ('Akshay', 'vipani team A'),
                    ('Arun', 'vipani team A'),
                    ('Naresh', 'vipani team A')
                ON CONFLICT (associate_name) DO NOTHING;
            """
        }
    ]

    try:
        with conn:
            with conn.cursor() as cur:
                for item in sql_queries:
                    logger.info(f"Executing: {item['description']}...")
                    cur.execute(item["query"])
                    logger.info(f"{item['description']} completed.")
        logger.info("All metadata insertions completed successfully.")

    except Exception as e:
        logger.exception("Error during metadata update.")
        conn.rollback()

    finally:
        conn.close()
        logger.info("Database connection closed.")
