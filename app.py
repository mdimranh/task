import asyncio
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
            'id': _,
            'name': fake.name(),
            'age': fake.random_int(18, 60),
            'phone': fake.phone_number(),
            'city': fake.city(),
            'gender': fake.random_element(elements=('male', 'female')),
            'word': fake.sentence(),
            "location": {
                "type": "Point",
                "coordinates": [float(fake.latitude()), float(fake.longitude())]
            },
            'zip': fake.zipcode(),
            'balance': fake.random_int(0, 1000)
        }
        data.append(record)
    return data

async def main():
    # Load existing data from XLSX file
    xlsx_file_path = 'user.xlsx'
    df_existing = pd.read_excel(xlsx_file_path)

    # Generate additional fake data
    num_additional_records = int(input("Numbers of records: "))
    fake_data = await generate_fake_data(num_additional_records, start_index=int(input("Start index: ")))

    # Combine existing data with fake data
    df_combined = pd.concat([df_existing, pd.DataFrame(fake_data)])

    # Convert DataFrame to a list of dictionaries
    data_to_insert = pd.DataFrame(fake_data).to_dict(orient='records')

    # # Convert Decimal values to float
    # for record in data_to_insert:
    #     record["location"]={
    #         "type": "Point",
    #         "coordinates": [float(record['lat']), float(record['lng'])]
    #     }

    # Connect to MongoDB
    mongo_client = AsyncIOMotorClient('mongodb://localhost:27017/')
    db = mongo_client['task1']  # Replace 'your_database' with your actual database name
    collection = db['user']  # Replace 'your_collection' with your actual collection name

    # Insert data into MongoDB asynchronously
    await insert_data_to_mongodb(data_to_insert, collection)

    # Close MongoDB connection
    mongo_client.close()

if __name__ == "__main__":
    asyncio.run(main())
