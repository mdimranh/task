import asyncio
import os

import pandas as pd
from motor.motor_asyncio import AsyncIOMotorClient


class Application:
    def __init__(self):
        self.host = "localhost"
        self.port = "27017"
        self.client = AsyncIOMotorClient(f'mongodb://{self.host}:{self.port}/')
        self.doc_path = None

    def get_collection(self, name):
        return self.client[name]

    def set_doc_path(self, path):
        self.doc_path = os.path.join(os.getcwd(), path)

    def insert_user(self, coll_name="user", data=None):
        user = self.get_collection(coll_name)
        user.insert_many(data)

    def __is_number(self, value):
        try:
            val = float(value)
            return int(val)
        except ValueError:
            return False

    def check_user_data(self, coll_name="user"):
        user = self.get_collection(coll_name)
        users = db.user.find({
            "age": {
                "$not": {
                    "$or": [
                        { "$type": "int" },
                        { "$type": "double" }
                    ]
                }
            }
        }, {"age": 1})
        formatted_ages = {}
        for _user in users:
            age = self.__is_number(str(_user.get("age", 0)))
            if age:
                formatted_ages.append({
                    "_id": user['_id'],
                    "age": age
                })