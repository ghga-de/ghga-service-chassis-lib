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

"""Config parameters for this package."""

from ghga_service_chassis_lib.api import ApiConfigBase
from ghga_service_chassis_lib.config import config_from_yaml
from ghga_service_chassis_lib.postgresql import PostgresqlConfigBase

# The db url is hard coded in this example.
# In a production application, this would come from an env variable or
# a config yaml.
DB_URL = "postgresql://postgres:postgres@postgresql/todo"

# You may inherit from multiple config base classes:
@config_from_yaml(prefix="my_postgres_demo")
class Config(ApiConfigBase, PostgresqlConfigBase):
    """Config Parameters"""

    an_additional_param: str = "some value"
    another_additional_param: int = 37


CONFIG = Config(db_url=DB_URL)
