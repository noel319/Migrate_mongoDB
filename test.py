from aiomultiprocess import Pool
import os, re, asyncio
import sqlite3
from utils.test_generation import generate_name, analyze
import pandas as pd
import motor.motor_asyncio
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# sqlite_folder = '../../db/' 
sqlite_folder = 'db/' 
# mongo_uri = 'mongodb://twuser:moniThmaRtio@192.168.20.75:27017/admin'
mongo_uri = 'mongodb://localhost:27017'

# Function to start the migration process
async def start_migrate(sqlite_folder):
    
    sqlite_files = [os.path.join(sqlite_folder, file) for file in os.listdir(sqlite_folder) if file.endswith('.db')]
    
    async with Pool() as pool:
        await pool.map(run_migration_sync, sqlite_files)      
    
    logging.info("Migration completed for all files.")
    
# Function to run the migration synchronously
async def run_migration_sync(file_path):
    try:
        logging.info(f"Queuing migration for {file_path}")
        await migrate_db(file_path)
    except Exception as e:
        logging.error(f"Error in migrating {file_path}: {e}")

# Function to migrate the database asynchronously
async def migrate_db(file_path):
    logging.info(f"Starting Migration for {file_path}")
    # Create a new Motor client for each process     
    client = motor.motor_asyncio.AsyncIOMotorClient(mongo_uri)    
    
    try:
        db_name = os.path.basename(file_path).replace('.db', '')
        conn = sqlite3.connect(file_path)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        logging.info(f"The database includes {len(tables)} tables: {tables}")

        mongo_db_name = get_db_name(db_name)
        mongo_db = client[mongo_db_name]
        column_type = []
        # Iterate through each table
        for table in tables:
            logging.info(f"Starting migration of table {table[0]}")
            if table[0] in ["main_config", "main_content"]:
                continue

            collection = mongo_db[table[0]]
            query = f"SELECT * FROM {table[0]} LIMIT 100"
            try:
                df = pd.read_sql(query, conn)
            except Exception as e:
                logging.error(f"Failed to read table {table[0]} from {file_path}: {e}")
                continue

            if table[0] == "main":
                try:
                    column_name, column_type = await generate_name(df)
                    logging.info(f"Finished generating column names for {table[0]}: {column_name}:date tpye:{column_type}")
                except Exception as e:
                    logging.error(f"Error generating column names for {table[0]}: {e}")
                    continue

            # Read table in chunks and migrate
            try:
                for chunk in pd.read_sql_query(f"SELECT * FROM {table[0]}", conn, chunksize=1000):
                    
                    # Clean and set datatype if necessary
                    if table[0] == "main":
                        chunk.columns = column_name
                        try:
                            result = analyze(chunk, column_type)
                            json_data = result.to_dict(orient='records')
                        except Exception as e:
                            logging.error(f"Error analyzing data for table {table[0]}: {e}")
                            continue
                    else:
                        json_data = chunk.to_dict(orient='records')

                    # Insert into MongoDB
                    try:
                        await collection.insert_many(json_data)                        
                    except Exception as e:
                        logging.error(f"Error inserting data into {table[0]} collection: {e}")
                column_type = []
            except Exception as e:
                logging.error(f"Error reading data in chunks from table {table[0]}: {e}")

        conn.close()
    except Exception as e:
        logging.error(f"Error in processing {file_path}: {e}")
        
    finally:
        # Ensure the MongoDB client is closed after migration        
        logging.info(f"MongoDB client closed for {file_path}")
        client.close()

# Function to sanitize database names
def get_db_name(db_name):
    db_name = re.sub(r'[^a-zA-Zа-яА-ЯіїІЇєЄ0-9]', '_', db_name)
    db_name = db_name.replace(' ', '_')
    db_name = db_name[:30]
    return db_name

if __name__ == "__main__":
    asyncio.run(start_migrate(sqlite_folder))
