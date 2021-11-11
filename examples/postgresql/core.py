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

"""This module contains the main business logic of the application."""

from datetime import datetime, timedelta

import typer

from . import models
from .dao import Database


async def add_some_todos():
    """Add some todo items."""
    tomorrow = datetime.now() + timedelta(1)
    yesterday = datetime.now() - timedelta(1)

    some_todos = [
        models.ToDoItem(
            title="laundry", description="Do the laundry", due_date=tomorrow
        ),
        # over-due:
        models.ToDoItem(
            title="groceries", description="Buy eggs and bacon", due_date=yesterday
        ),
        models.ToDoItem(
            title="clean bathroom",
            description="clean the bathroom",
            due_date=yesterday,
        ),
    ]
    async with Database() as database:
        for todo in some_todos:
            await database.add_todo(todo)


async def print_all_todos():
    """Print all todo items and highlight items that are overdue"""
    now = datetime.now()

    typer.echo("My ToDo list:")

    async with Database() as database:
        all_todos = await database.get_all_todos()
        for todo in all_todos:
            message = f" - {todo.title}: {todo.description} until {todo.due_date}"
            color = typer.colors.GREEN if todo.due_date >= now else typer.colors.RED
            typer.secho(message, fg=color)
