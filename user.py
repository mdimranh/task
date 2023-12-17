from functools import partial
from multiprocessing import Pool, current_process

from faker import Faker
from pymongo import MongoClient

fake = Faker()

def generate_data_chunk(chunk_size, collection):
    print(f"Generating {chunk_size} in Process {current_process().name}")

    # Create a new Faker instance for each process
    fake_local = Faker()
    fake_local.seed(current_process().name)

    records = []
    for _ in range(chunk_size):
        record = {
            'name': fake_local.name(),
            'age': fake_local.random_int(18, 60),
            'phone': fake_local.phone_number(),
            'city': fake_local.city(),
            'gender': fake_local.random_element(elements=('male', 'female')),
            'word': fake_local.sentence(),
            "location": {
                "type": "Point",
                "coordinates": [float(fake_local.latitude()), float(fake_local.longitude())]
            },
            'zip': fake_local.zipcode(),
            'balance': fake_local.random_int(0, 1000)
        }
        records.append(record)
    return records

def insert_data_chunk(records, collection):
    print(f"Inserting {len(records)} in Process {current_process().name}")
    collection.insert_many(records)
    print(f"Inserted {len(records)} in Process {current_process().name}")

if __name__ == "__main__":
    chunk_size = 1000
    num_processes = 4  # Adjust based on your system's capacity

    with MongoClient("mongodb://localhost:27017") as mongo_client:
        db = mongo_client["task"]
        collection = db["user1"]

        # Create partial functions with the collection argument
        generate_data_partial = partial(generate_data_chunk, chunk_size, collection)
        insert_data_partial = partial(insert_data_chunk, collection=collection)

        with Pool(num_processes) as pool:
            data_chunks = pool.map(generate_data_partial, [None] * num_processes)
            pool.map(insert_data_partial, data_chunks)
