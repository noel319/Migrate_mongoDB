from utils.migration import migrate_mongo
import os

sqlite_folder_path = '/db/'  
mongo_uri = 'mongodb://localhost:27017/'  

sqlite_files = [os.path.join(sqlite_folder_path, file) for file in os.listdir(sqlite_folder_path) if file.endswith('.db')]


if __name__ == "__main__":
    migrate_mongo(sqlite_folder_path, mongo_uri)
