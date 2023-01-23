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

"""Entrypoint of the package"""

import asyncio

from ghga_service_chassis_lib.api import run_server
from ghga_service_chassis_lib.utils import assert_tz_is_utc

from .api import app  # noqa: F401 pylint: disable=unused-import
from .config import get_config


def run():
    """Run the service"""
    assert_tz_is_utc()
    asyncio.run(
        run_server(app="hello_world_web_server.__main__:app", config=get_config())
    )


if __name__ == "__main__":
    run()
