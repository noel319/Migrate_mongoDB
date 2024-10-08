from  utils.ai_generation import generate_name
from  utils.ai_generation import analyze
import pandas as pd
from pymongo import MongoClient
import sqlite3
import os, re

mongo_uri = 'mongodb://localhost:27017/'  

def migrate_db(file_path):

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
        query = f"SELECT *FROM {table[0]} LIMIT 500"
        df = pd.read_sql(query, conn)
        column_data = {}
        column_name = []
        for column in df.columns:
            column_data[column] = df[column].tolist()
            column_name.append(generate_name(column_data))
        
        # Migrate mogoDB
        print(column_name)
        for chunk in pd.read_sql_query(f"SELECT *FROM {table[0]}", conn, chunksize=1000):
            # Clean and prepare data if necessary
            chunk.columns = column_name
            # cleanned_data = analyze(chunk)
            json_data = chunk.to_dict(orient = 'records')
            collection.insert_many(json_data)   
    conn.close()    
    print(f"Completed processing for {file_path}")

def get_db_name(db_name):
    db_name = re.sub(r'[^a-zA-Zа-яА-ЯіїІЇєЄ0-9]', '_', db_name)
    db_name = db_name.replace(' ', '_')
    db_name = db_name[:30]
    return db_name
