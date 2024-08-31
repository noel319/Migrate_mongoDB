import os
from pymongo import MongoClient
from multiprocessing import Pool, cpu_count
from utils.data_clean import clean_data
from utils.make_name import load_model
from utils.database_processing import get_tables, read_table

### Process a single SQLite file and migrate its data to MongoDB.

def process_single_file(file_path, mongo_uri, ai_model):  
    db_name = os.path.splitext(os.path.basename(file_path))[0]  # Use the SQLite file name as the MongoDB database name
    client = MongoClient(mongo_uri)
    db = client[db_name]
    
    tables = get_tables(file_path)
    
    for table_name in tables:
        collection = db[table_name]
        for chunk in read_table(file_path, table_name):
            cleaned_data = clean_data(chunk, ai_model)
            json_data = cleaned_data.to_dict(orient='records')
            collection.insert_many(json_data)
    
    print(f"Completed processing for {file_path}")

### Migrate data from SQLite files in the folder to MongoDB.

def migrate_mongo(sqlite_folder_path, mongo_uri):   
    ai_model = load_model()
    sqlite_files = [os.path.join(sqlite_folder_path, file) for file in os.listdir(sqlite_folder_path) if file.endswith('.sqlite') or file.endswith('.db')]
    pool = Pool(processes=cpu_count())
    
    for file_path in sqlite_files:
        pool.apply_async(process_single_file, (file_path, mongo_uri, ai_model))    
    pool.close()
    pool.join()
    print("Migration completed for all files.")
