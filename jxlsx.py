import asyncio

import pandas as pd
from bson import json_util
# from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import MongoClient


async def main():

    mongo_client = MongoClient('mongodb://localhost:27017/')
    db = mongo_client['task']  # Replace 'your_database' with your actual database name
    collection = db['user']  # Replace 'your_collection' with your actual collection name

    data = collection.find()
    # datas = await data.to_list(length=100) 

    # Create a DataFrame from the JSON data
    df = pd.DataFrame(data)

    # Specify the output Excel file path
    excel_file_path = 'output.xlsx'

    # Export the DataFrame to Excel
    df.to_excel(excel_file_path, index=False, engine='openpyxl')

    # Close MongoDB connection
    mongo_client.close()

if __name__ == "__main__":
    asyncio.run(main())
