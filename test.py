from multiprocessing import Pool, cpu_count
import os, re
import sqlite3
from utils.test_generation import generate_name, analyze, col_type
import pandas as pd
from pymongo import MongoClient
sqlite_folder = 'db/'  
mongo_uri = 'mongodb://localhost:27017/'


def start_migrate(sqlite_folder):
    sqlite_files = [os.path.join(sqlite_folder, file) for file in os.listdir(sqlite_folder) if file.endswith('.db')]
    # pool = Pool(processes=cpu_count())
    
    for file_path in sqlite_files:
        print(f"Queuing migration for {file_path}")
        migrate_db(file_path)
        # pool.apply_async(migrate_db, (file_path,))    
    # pool.close()
    # pool.join()
    print("Migration completed for all files.")
def migrate_db(file_path):
    global col_type
    ### Connect Sqlite DB and Get Table List

    db_name = os.path.basename(file_path).replace('.db','')
    conn = sqlite3.connect(file_path)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    client = MongoClient(mongo_uri)
    mongo_db_name = get_db_name(db_name)
    mongo_db = client[mongo_db_name]
    
    ### PreRead Table Column data And Make meaningful column name by using AI model

    for table in tables:
        collection = mongo_db[table[0]]
        query = f"SELECT * FROM {table[0]} LIMIT 100"
        df = pd.read_sql(query, conn)
        if table[0] == "main":
            column_name = generate_name(df)
        for chunk in pd.read_sql_query(f"SELECT *FROM {table[0]}", conn, chunksize=500):
            # Clean and prepare data if necessary
            if table[0] == "main":
                chunk.columns = column_name
                for col, dtype in zip(chunk.columns, col_type):
                    chunk[col] = chunk[col].astype(dtype)
                col_type = []
                result = analyze(chunk)                
            # cleanned_data = analyze(chunk)            
                json_data = result.to_dict(orient = 'records')
            else:
                json_data = chunk.to_dict(orient = 'records')
            collection.insert_many(json_data)
                    
    conn.close()    

def get_db_name(db_name):
    db_name = re.sub(r'[^a-zA-Zа-яА-ЯіїІЇєЄ0-9]', '_', db_name)
    db_name = db_name.replace(' ', '_')
    db_name = db_name[:30]
    return db_name
if __name__ == "__main__":
    start_migrate(sqlite_folder)


