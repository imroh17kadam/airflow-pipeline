# src/load.py
import psycopg2
import logging

def load_data(data: list, db_config: dict):
    logging.info("Starting data load")

    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    create_table_query = """
    CREATE TABLE IF NOT EXISTS tasks (
        id INT PRIMARY KEY,
        title TEXT,
        completed BOOLEAN
    )
    """
    cursor.execute(create_table_query)

    insert_query = """
    INSERT INTO tasks (id, title, completed)
    VALUES (%s, %s, %s)
    ON CONFLICT (id) DO NOTHING
    """

    for row in data:
        cursor.execute(
            insert_query,
            (row["id"], row["title"], row["completed"])
        )

    conn.commit()
    cursor.close()
    conn.close()

    logging.info("Data loaded successfully")