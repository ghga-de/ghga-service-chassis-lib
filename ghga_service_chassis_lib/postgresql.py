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
This module contains functionalities that simplifies the connection to SQL databases.
"""

from pydantic import BaseSettings, validator


class PostgresqlBaseConfig(BaseSettings):
    """A base class with Postrgesql-specific config params.
    Inherit your config class from this class if you need
    PostgreSQL in the backend."""

    db_url: str
    db_print_logs: bool = False

    # pylint: disable=no-self-argument,no-self-use
    @validator("db_url")
    def db_url_prefix(cls, value: str):
        """Checks if db_url is a valid postgres URL."""
        prefix = "postgresql://"
        if not value.startswith("postgresql://"):
            raise ValueError(f"must start with '{prefix}'")
        return value
