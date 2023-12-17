import asyncio
import time
from functools import lru_cache

import numpy as np
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import MongoClient


@lru_cache(maxsize=None)
async def accuracy(value, _min, _max, target):
    return np.round((1 - (np.abs(target - value) / (_max - _min))) * 100, 2)

async def get_data_async():
    client = AsyncIOMotorClient('mongodb://localhost:27017/')
    db = client['task']
    collection = db['user']
    target_balance = 700 # int(input("Balance: "))
    age = 43 # int(input("Age: "))
    city = "New Deborah" # input("City: ")
    _min = target_balance - 100
    _max = target_balance + 100
    start = time.time()

    cursor = collection.find(
        {
            "balance": {"$gt": _min, "$lt": _max},
            "age": age,
            "city": city,
            # # "$text": {
            # #     "$search": city
            # # }
        },
        projection={"_id": 1, "name": 1, "balance": 1}
    ).limit(2)

    result = await cursor.to_list(length=None)

    # Extract 'balance' values into a NumPy array
    # balances = np.array([item['balance'] for item in result])

    # Use NumPy vectorized operation for element-wise accuracy calculation
    # accuracies = await accuracy(balances, _min, _max, target_balance)

    # Update 'accuracy' field in the result
    # for i, item in enumerate(result):
    for item in result:
        # item['accuracy'] = accuracies[i]
        item['accuracy'] = await accuracy(item['balance'], _min, _max, target_balance)

    # Sort the result based on 'accuracy'
    result.sort(key=lambda x: x['accuracy'])

    print("Time taken:", time.time() - start)
    print(len(result))
    print(result)

if __name__ == "__main__":
    asyncio.run(get_data_async())
