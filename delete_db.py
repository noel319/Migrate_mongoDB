import motor.motor_asyncio
from pymongo.errors import ServerSelectionTimeoutError, OperationFailure
import asyncio
from aiomultiprocess import Pool

async def delete_databases_with_special_field_name(uri):
    # Create an async MongoDB client
    client = motor.motor_asyncio.AsyncIOMotorClient(uri)

    try:
        # Step 1: List all databases
        db_names = await client.list_database_names()
        print(f"Found {len(db_names)} databases.")

        # Step 2: Iterate over each database
        for db_name in db_names:
            db = client[db_name]
            
            # Step 3: Check if the 'main' collection exists in the database
            collections = await db.list_collection_names()
            if 'main' in collections:
                main_collection = db['main']
                
                try:
                    # Step 4: Find documents in the 'main' collection with a field name that includes a single quote (') or backtick (`).
                    async for document in main_collection.find():
                        for field in document.keys():
                            if "'" in field or "`" in field:  # Corrected logic
                                print(f"Database '{db_name}' contains a document with a field name that includes a special character: {field}")
                                
                                # Step 5: Delete the database if such a field is found
                                await client.drop_database(db_name)
                                print(f"Deleted database: {db_name}")
                                break
                except OperationFailure as e:
                    print(f"Skipping database '{db_name}' due to error: {e}")
            else:
                print(f"No 'main' collection found in database '{db_name}'.")
    
    except ServerSelectionTimeoutError:
        print("Unable to connect to MongoDB server. Check your connection or URI.")
    finally:
        # Close the connection asynchronously
        await client.close()

async def start_delete(uri):
    # Create a pool for parallel execution
    async with Pool() as pool:
        await pool.apply(delete_databases_with_special_field_name, (uri,))

# Example usage
if __name__ == "__main__":
    uri = 'mongodb://twuser:moniThmaRtio@192.168.20.75:27017/admin'  # MongoDB URI
    # Run the async function
    asyncio.run(start_delete(uri))
