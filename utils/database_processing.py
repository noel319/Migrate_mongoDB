import sqlite3
import pandas as pd

def get_tables(file_path):
    """
    Connect to SQLite and get a list of tables.
    """
    conn = sqlite3.connect(file_path)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    conn.close()
    return [table[0] for table in tables]

def read_table(file_path, table_name, chunksize=10000):
    """
    Read a table from SQLite in chunks.
    """
    conn = sqlite3.connect(file_path)
    chunks = pd.read_sql_query(f"SELECT * FROM {table_name}", conn, chunksize=chunksize)
    conn.close()
    return chunks
