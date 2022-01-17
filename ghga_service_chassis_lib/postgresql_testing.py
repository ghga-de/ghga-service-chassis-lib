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
This module contains utilities for testing code created with the functionality
from the `postgresql` module.
"""

from testcontainers.postgres import PostgresContainer

from .postgresql import PostgresqlConfigBase


def config_from_psql_container(container: PostgresContainer) -> PostgresqlConfigBase:
    """Prepares a PostgresqlConfigBase from an instance of a postgres test container."""
    db_url = container.get_connection_url()
    db_url_formatted = db_url.replace("postgresql+psycopg2", "postgresql")
    return PostgresqlConfigBase(db_url=db_url_formatted)
