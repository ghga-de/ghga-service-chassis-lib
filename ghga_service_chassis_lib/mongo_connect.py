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
This module contains the DBConnect class and its related methods
that are relevant for connecting to an underlying MongoDB store.
"""

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection


class DBConnect:
    """
    Class that handles connections to a MongoDB store.
    """

    def __init__(self, db_url, db_name):
        self.db_url = db_url
        self.db_name = db_name
        self.client = AsyncIOMotorClient(self.db_url)

    async def get_db(self) -> AsyncIOMotorClient:
        """
        Return database client instance.
        Returns:
            An instance of AsyncIOMotorClient
        """
        self.client = AsyncIOMotorClient(self.db_url)
        return self.client

    async def close_db(self) -> None:
        """
        Close database connection.
        """
        self.client.close()

    async def get_collection(self, name: str) -> AsyncIOMotorCollection:
        """
        Get a collection from the database.
        Args:
            name: Name of the collection to fetch
        Returns:
            An instance of AsyncIOMotorCollection
        """
        client = await self.get_db()
        collection = client[self.db_name][name]
        return collection
