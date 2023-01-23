# Copyright 2021 - 2023 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
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
This is just a utility for setup/reset of the database. In production a
migration tool like alembic should be used instead.
"""

from sqlalchemy import create_engine
from sqlalchemy_utils import create_database, database_exists

from .dao.db_models import Base


def create_db(db_url: str):
    "creates the database if it doesn't exist"
    if not database_exists(db_url):
        create_database(db_url)


def reset_db(db_url: str):
    """Drop and re-create all tables."""
    engine = create_engine(db_url)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)


def setup_db(db_url: str):
    "Setup/reset DB"
    create_db(db_url)
    reset_db(db_url)
