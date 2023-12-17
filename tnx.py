import asyncio
import random
from datetime import datetime
from decimal import Decimal

import pandas as pd
from faker import Faker
from motor.motor_asyncio import AsyncIOMotorClient


async def insert_data_to_mongodb(data, collection):
    # Insert data into MongoDB
    await collection.insert_many(data)

async def generate_fake_data(num_records, start_index):
    fake = Faker()
    data = []
    for _ in range(start_index, num_records+start_index):
        record = {
            "_id": _,
            'user_id': fake.random_int(1, 10),
            'amount': fake.random_int(0, 600),
            'time': fake.date_time_between(start_date=datetime(2023, 1, 1), end_date=datetime(2023, 12, 31)),
            'status': fake.random_element(elements=('success', 'failed', 'pending')),
            'type': fake.random_element(elements=('d', 'c')),
            'balance': fake.random_int(0, 1000)
        }
        data.append(record)
    return data

async def main():

    # Generate additional fake data
    num_additional_records = int(input("Numbers of records: "))
    fake_data = await generate_fake_data(num_additional_records, start_index=int(input("Start index: ")))

    # Convert DataFrame to a list of dictionaries
    data_to_insert = pd.DataFrame(fake_data).to_dict(orient='records')

    # Connect to MongoDB
    mongo_client = AsyncIOMotorClient('mongodb://localhost:27017/')
    db = mongo_client['task1']  # Replace 'your_database' with your actual database name
    collection = db['transaction']  # Replace 'your_collection' with your actual collection name

    # Insert data into MongoDB asynchronously
    await insert_data_to_mongodb(data_to_insert, collection)

    # Close MongoDB connection
    mongo_client.close()

if __name__ == "__main__":
    asyncio.run(main())
