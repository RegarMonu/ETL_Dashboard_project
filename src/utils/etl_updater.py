import psycopg2

def update_etl_metadata(cur, source_table):
    """
    Updates or inserts the last_loaded_at timestamp for a given source_table.
    """
    cur.execute("""
        INSERT INTO etl_metadata (table_name, last_loaded_at)
        VALUES (%s, CURRENT_TIMESTAMP)
        ON CONFLICT (table_name)
        DO UPDATE SET last_loaded_at = EXCLUDED.last_loaded_at;
    """, (source_table,))

def get_etl_metadata(cur, source_table):
    """
    Fetches the last_loaded_at timestamp for a given source_table.
    Raises an exception if no metadata is found.
    """
    cur.execute("""
        SELECT last_loaded_at FROM etl_metadata WHERE table_name = %s;
    """, (source_table,))
    
    result = cur.fetchone()
    if result is None:
        raise Exception(f"No entry in etl_metadata for '{source_table}'")
    
    return result[0]
