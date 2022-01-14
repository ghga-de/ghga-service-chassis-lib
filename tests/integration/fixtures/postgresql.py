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

"""Fixtures for testing the PostgreSQL functionalities"""


from collections import namedtuple

from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.decl_api import DeclarativeMeta

Base: DeclarativeMeta = declarative_base()


class TestModel(Base):
    """
    A test model.
    """

    __tablename__ = "testentries"
    id = Column(Integer, primary_key=True)
    some_string = Column(String, nullable=False, unique=True)
    some_number = Column(Integer, nullable=False)


TestFixtureModel = namedtuple("TestFixtureModel", ["some_string", "some_number"])


PREPOPULATED_TEST_DATA = [
    TestFixtureModel(some_string="foo", some_number=1),
    TestFixtureModel(some_string="bar", some_number=2),
]


ADDITIONAL_TEST_DATA = [
    TestFixtureModel(some_string="turtle", some_number=3),
    TestFixtureModel(some_string="quail", some_number=4),
]


def fixture_to_orm_model(entry: TestFixtureModel) -> TestModel:
    """Converts a TestFixtureModel into an ORM model"""
    return TestModel(some_string=entry.some_string, some_number=entry.some_number)


def populate_db(db_url: str):
    """Create and populates the DB"""

    # setup database and tables:
    engine = create_engine(db_url)
    Base.metadata.create_all(engine)

    # populate with test data:
    session_factor = sessionmaker(engine)
    with session_factor() as session:
        for entry in PREPOPULATED_TEST_DATA:
            orm_entry = fixture_to_orm_model(entry)
            session.add(orm_entry)
        session.commit()
