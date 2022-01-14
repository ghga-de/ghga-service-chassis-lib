# Copyright 2021 - 2022 Universität Tübingen, DKFZ and EMBL
# for the German Human Genome-Phenome Archive (GHGA)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


"""
Example to show, how we can establish a connection and insert data to the database
"""

import asyncio

from ghga_service_chassis_lib.mongo_connect import DBConnect

DB_URL = "mongodb://mongo_db:27017"
DB_NAME = "example"


async def main():

    """Small example of how to connect to a mongodb database"""
    db_connect = DBConnect(DB_URL, DB_NAME)
    collection = await db_connect.get_collection("example_collection")
    await collection.delete_many({})
    await collection.insert_one({"id": "key", "value": 0})
    key_value = await collection.count_documents({})

    # This should be one
    print(f"Insterted {key_value} documents into the database.")
    await collection.delete_many({})
    await db_connect.close_db()


if __name__ == "__main__":
    asyncio.run(main())
