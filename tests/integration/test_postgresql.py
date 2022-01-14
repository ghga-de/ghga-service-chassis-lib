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
Test the postgresql functionalities
"""

import pytest
from sqlalchemy.future import select
from testcontainers.postgres import PostgresContainer

from ghga_service_chassis_lib.postgresql import (
    AsyncPostgresqlConnector,
    SyncPostgresqlConnector,
)
from ghga_service_chassis_lib.postgresql_testing import config_from_psql_container

from .fixtures.postgresql import (
    ADDITIONAL_TEST_DATA,
    PREPOPULATED_TEST_DATA,
    TestModel,
    fixture_to_orm_model,
    populate_db,
)


def test_sync_connector_query():
    """Tests the SyncPostgresqlConnector"""

    with PostgresContainer() as postgres:
        config = config_from_psql_container(postgres)
        populate_db(config.db_url)

        psql_connector = SyncPostgresqlConnector(config)

        # query existing entries:
        with psql_connector.transactional_session() as session:
            query = session.execute(select(TestModel).order_by(TestModel.some_number))
            first_entry = query.scalars().first()
            expected_first_entry = PREPOPULATED_TEST_DATA[0]
        assert first_entry.some_string == expected_first_entry.some_string


def test_sync_connector_commit():
    """Tests the SyncPostgresqlConnector"""

    with PostgresContainer() as postgres:
        entry = ADDITIONAL_TEST_DATA[0]

        config = config_from_psql_container(postgres)
        populate_db(config.db_url)

        psql_connector = SyncPostgresqlConnector(config)

        # commit additional entry:
        with psql_connector.transactional_session() as session:
            orm_entry = fixture_to_orm_model(entry)
            session.add(orm_entry)

        # query for the newly added entry:
        with psql_connector.transactional_session() as session:
            query = session.execute(
                select(TestModel).where(TestModel.some_string == entry.some_string)
            )
            first_entry = query.scalars().first()
        assert first_entry.some_string == entry.some_string


@pytest.mark.asyncio
async def test_async_connector_query():
    """Tests the AsyncPostgresqlConnector"""

    with PostgresContainer() as postgres:
        config = config_from_psql_container(postgres)
        populate_db(config.db_url)

        psql_connector = AsyncPostgresqlConnector(config)

        # query existing entries:
        async with psql_connector.transactional_session() as session:
            query = await session.execute(
                select(TestModel).order_by(TestModel.some_number)
            )
            first_entry = query.scalars().first()
            expected_first_entry = PREPOPULATED_TEST_DATA[0]
        assert first_entry.some_string == expected_first_entry.some_string


@pytest.mark.asyncio
async def test_async_connector_commit():
    """Tests the AsyncPostgresqlConnector"""

    with PostgresContainer() as postgres:
        entry = ADDITIONAL_TEST_DATA[0]

        config = config_from_psql_container(postgres)
        populate_db(config.db_url)

        psql_connector = AsyncPostgresqlConnector(config)

        # commit additional entry:
        async with psql_connector.transactional_session() as session:
            orm_entry = fixture_to_orm_model(entry)
            session.add(orm_entry)

        # query for the newly added entry:
        async with psql_connector.transactional_session() as session:
            query = await session.execute(
                select(TestModel).where(TestModel.some_string == entry.some_string)
            )
            first_entry = query.scalars().first()
        assert first_entry.some_string == entry.some_string
