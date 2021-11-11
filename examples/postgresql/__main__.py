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

"""package entrypoint"""

import asyncio

from .config import config
from .core import add_some_todos, print_all_todos
from .setup_db import setup_db


async def main():
    """main function handed to the event loop"""
    await add_some_todos()
    await print_all_todos()


if __name__ == "__main__":
    setup_db(config.db_url)
    asyncio.run(main())
