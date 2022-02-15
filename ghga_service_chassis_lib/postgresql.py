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
This module contains functionalities that simplifies the connection to SQL databases.
"""

from collections.abc import AsyncGenerator, Generator
from contextlib import asynccontextmanager, contextmanager

from pydantic import BaseSettings, Field, validator
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.session import Session


class PostgresqlConfigBase(BaseSettings):
    """A base class with Postrgesql-specific config params.
    Inherit your config class from this class if you need
    PostgreSQL in the backend."""

    db_url: str = Field(
        ...,
        example="postgresql://user:password@mydbserver/dbname",
        description="A URL to a PostgreSQL database.",
    )
    db_print_logs: bool = Field(
        False,
        description="Print DB/ORM logs.",
    )

    # pylint: disable=no-self-argument,no-self-use
    @validator("db_url")
    def db_url_prefix(cls, value: str):
        """Checks if db_url is a valid postgres URL."""
        prefix = "postgresql://"
        if not value.startswith("postgresql://"):
            raise ValueError(f"must start with '{prefix}'")
        return value


class PostgresqlConnectorBase:
    """A base class used to implement a handler for Connections to
    PostgreSQL databases."""

    def __init__(self, config: PostgresqlConfigBase):
        """Initialize Connector.

        Args:
            config (PostgresqlConfigBase): Configs including the DB url.
        """
        self._configs = config
        self.db_url = config.db_url


class SyncPostgresqlConnector(PostgresqlConnectorBase):
    """A class for dealing with synchronous connections to
    PostgreSQL databases."""

    def __init__(self, config: PostgresqlConfigBase):
        """Initialize Connector.

        Args:
            config (PostgresqlConfigBase): Configs including the DB url.
        """
        super().__init__(config=config)

        self.engine = create_engine(config.db_url)
        self.sessionmaker = sessionmaker(self.engine, expire_on_commit=False)

    @contextmanager
    def transactional_session(self) -> Generator[Session, None, None]:
        """
        Returns a session object that can be used as a context manager.
        It will automatically commit when leaving the context as long
        as no errors occur.
        """
        session = self.sessionmaker()
        session.begin()
        try:
            yield session
        except:
            session.rollback()
            raise
        else:
            session.commit()
        finally:
            session.close()


class AsyncPostgresqlConnector(PostgresqlConnectorBase):
    """A class for dealing with asynchronous connections to
    PostgreSQL databases."""

    def __init__(self, config: PostgresqlConfigBase):
        """Initialize Connector.

        Args:
            config (PostgresqlConfigBase): Configs including the DB url.
        """
        super().__init__(config=config)

        # change url prefix to use the asyncpg driver:
        self.db_url_async = config.db_url.replace(
            "postgresql://", "postgresql+asyncpg://"
        )

        self.engine = create_async_engine(self.db_url_async)
        self.sessionmaker = sessionmaker(
            self.engine, class_=AsyncSession, expire_on_commit=False
        )

    @asynccontextmanager
    async def transactional_session(self) -> AsyncGenerator[AsyncSession, None]:
        """
        Returns a session object that can be used as a context manager.
        It will automatically commit when leaving the context as long
        as no errors occur.
        """
        session = self.sessionmaker()
        await session.begin()
        try:
            yield session
        except:
            await session.rollback()
            raise
        else:
            await session.commit()
        finally:
            await session.close()
