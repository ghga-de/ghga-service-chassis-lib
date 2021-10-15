# Copyright 2021 Universität Tübingen, DKFZ and EMBL
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
Test mongodb connection module
"""
import pytest

from ghga_service_chassis_lib.mongo_connect import DBConnect

DB_URL = "mongodb://db:27017"
DB_NAME = "test"


@pytest.mark.asyncio
async def test_get_collection():

    """
    Test, if we can establish a connection and insert data to the database
    """

    db_connect = DBConnect(DB_URL, DB_NAME)
    collection = await db_connect.get_collection("test_collection")
    # await collection.delete_many({})
    await collection.insert_one({"id": "key", "value": 0})  # type: ignore
    key_value = await collection.count_documents({})  # type: ignore
    assert key_value == 1


@pytest.mark.asyncio
async def test_close_db():

    """
    Test, if close_db actually closes the connection
    """

    db_connect = DBConnect(DB_URL, DB_NAME)
    assert await db_connect.close_db() is None
