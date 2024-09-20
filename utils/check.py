import motor.motor_asyncio
from pymongo.errors import ServerSelectionTimeoutError
import asyncio

async def check_db(uri, db_name):
    # Create an async client
    client = motor.motor_asyncio.AsyncIOMotorClient(uri)
    
    try:
        # List databases
        dbs = await client.list_database_names()
        if db_name in dbs:
            print(f"Database '{db_name}' exists.")
            
            # List collections in the given database
            db = client[db_name]
            collections = await db.list_collection_names()
            if "main" in collections:
                print(f"Collection main collection exists in database '{db_name}'.")
                return False
            else:
                print(f"Collection main collection does not exist in database '{db_name}'.")
                return True
        else:
            print(f"Database '{db_name}' does not exist.")
            return True
    
    except ServerSelectionTimeoutError:
        print("Unable to connect to MongoDB server. Check your connection or URI.")

# Example usage
uri = "mongodb://localhost:27017"  # MongoDB URI
db_name = "example_db"
collection_name = "main_collection"

# Run the async function
asyncio.run(check_db(uri, db_name, collection_name))
