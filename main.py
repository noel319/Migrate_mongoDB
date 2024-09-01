from multiprocessing import Pool, cpu_count
from utils.migrate_mongo import migrate_db
import os

sqlite_folder = '/db/'  
mongo_uri = 'mongodb://localhost:27017/'


def start_migrate(sqlite_folder):
    sqlite_files = [os.path.join(sqlite_folder, file) for file in os.listdir(sqlite_folder) if file.endswith('.db')]
    pool = Pool(processes=cpu_count())
    
    for file_path in sqlite_files:
        pool.apply_async(migrate_db, (file_path))    
    pool.close()
    pool.join()
    print("Migration completed for all files.")

if __name__ == "__main__":
    start_migrate(sqlite_folder)


