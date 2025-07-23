import psycopg2
from src import db_connection

def update_client_associate_data():
    conn = db_connection.get_connection()
    
    sql_queries = [
        """
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
        """,
        """
        INSERT INTO dim_clients (client_name)
        SELECT DISTINCT client
        FROM (
            SELECT client FROM work_in_progress
            UNION
            SELECT client FROM work_completed
        ) AS combined_clients
        ON CONFLICT (client_name) DO NOTHING;
        """,
        """
            -- Insert team
        INSERT INTO dim_teams (team_name, team_lead)
        VALUES 
            ('vipani team A', 'Arun')
        ON CONFLICT (team_name) DO NOTHING;
        """,
        """
            -- Map associates to team
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
    ]

    try:
        cur = conn.cursor()
        for query in sql_queries:
            cur.execute(query)
        conn.commit()
        print("Data inserted successfully.")
    except Exception as e:
        print("Error:", e)
    finally:
        conn.close()
