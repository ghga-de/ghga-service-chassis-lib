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

"""db boilerplate"""

from typing import List

from sqlalchemy.future import select

from ghga_service_chassis_lib.postgresql import AsyncPostgresqlConnector
from ghga_service_chassis_lib.utils import DaoGenericBase

from .. import models
from ..config import config
from . import db_models

psql_connector = AsyncPostgresqlConnector(config)

# Since this is just a DAO stub without implementation,
# following pylint error are expected:
# pylint: disable=unused-argument,no-self-use
class DatabaseDao(DaoGenericBase):
    """
    A DAO base class for interacting with the database.
    """

    async def add_todo(self, item: models.ToDoItem) -> None:
        """add a todo item"""
        ...

    async def get_all_todos(self) -> List[models.ToDoItem]:
        """get all todo items"""
        ...


class PostgresDatabase(DatabaseDao):
    """
    An implementation of the  DatabaseDao interface using a PostgreSQL backend.
    """

    async def add_todo(self, item: models.ToDoItem) -> None:
        """add a todo item"""

        orm_item = db_models.ToDoItem(**item.dict())
        async with psql_connector.transactional_session() as session:
            session.add(orm_item)

    async def get_all_todos(self) -> List[models.ToDoItem]:
        """get all todo items"""

        # query all todo items:
        async with psql_connector.transactional_session() as session:
            query = await session.execute(
                select(db_models.ToDoItem).order_by(db_models.ToDoItem.id)
            )

        # translate orm_items to business-logic data models:
        items = [
            models.ToDoItem(
                title=orm_item.title,
                description=orm_item.description,
                due_date=orm_item.due_date,
            )
            for orm_item in query.scalars().all()
        ]

        return items


# here you choose the DatabaseDAO to use:
Database = PostgresDatabase
