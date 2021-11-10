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

from datetime import datetime, timedelta

from sqlalchemy.future import select

from ghga_service_chassis_lib.postgresql import PostgresqlConnector

from .config import config
from .db_models import ToDoItem

postgres = PostgresqlConnector(config)


async def add_todos():
    """add some todo items"""
    tomorrow = datetime.now() + timedelta(1)
    my_todos = [
        ToDoItem(title="laundry", description="Do the laundry", due_date=tomorrow),
        ToDoItem(
            title="groceries", description="Buy eggs and bacon", due_date=tomorrow
        ),
    ]
    async with postgres.transactional_session() as session:
        for my_todo in my_todos:
            session.add(my_todo)


async def print_all_todos():
    """print all todo items"""
    print("ToDo List:")
    async with postgres.transactional_session() as session:
        query = await session.execute(select(ToDoItem).order_by(ToDoItem.id))
    for item in query.scalars().all():
        print(f" - {item.title}: {item.description} until {item.due_date}")
