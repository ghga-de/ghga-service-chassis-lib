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

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.future import select
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import create_database, database_exists

from .db_models import Base, ToDoItem

DB_URL = "postgresql+asyncpg://postgres:postgres@postgresql/todo"
DB_URL_ = "postgresql://postgres:postgres@postgresql/todo"

if not database_exists(DB_URL_):
    create_database(DB_URL_)

engine = create_async_engine(DB_URL)

async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def reset_db():
    """Drop and re-create all tables."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)


async def add_todos():
    """add some todo items"""
    tomorrow = datetime.now() + timedelta(1)
    my_todos = [
        ToDoItem(title="laundry", description="Do the laundry", due_date=tomorrow),
        ToDoItem(
            title="groceries", description="Buy eggs and bacon", due_date=tomorrow
        ),
    ]

    async with async_session() as session:
        for my_todo in my_todos:
            session.add(my_todo)
        await session.commit()


async def print_all_todos():
    """print all todo items"""
    async with async_session() as session:
        query = await session.execute(select(ToDoItem).order_by(ToDoItem.id))
    for item in query.scalars().all():
        print(f" - {item.title}: {item.description} until {item.due_date}")
