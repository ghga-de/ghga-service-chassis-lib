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

"""Test api module"""

import multiprocessing
import time

import requests

from ghga_service_chassis_lib.api import ApiConfigBase, run_server

from .fixtures.hello_world_test_app import GREETING, app
from .fixtures.utils import find_free_port


def test_run_server():
    """Test the run_server wrapper function"""
    config = ApiConfigBase()
    config.port = find_free_port()

    process = multiprocessing.Process(
        target=run_server, kwargs={"app": app, "config": config}
    )
    process.start()

    # give server time to come up:
    time.sleep(2)

    # run test query:
    try:
        response = requests.get(f"http://{config.host}:{config.port}/greet")
    except Exception as exc:
        raise exc
    finally:
        process.kill()
    assert response.status_code == 200
    assert response.json() == GREETING
