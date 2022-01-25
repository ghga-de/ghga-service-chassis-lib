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

"""Fixtures for pub sub"""

import json

from ghga_service_chassis_lib.pubsub_testing import amqp_fixture_factory

from .utils import BASE_DIR

EXAMPLE_TOPIC_NAME = "test_topic"

EXAMPLE_MESSAGE = {"greet": "Hello World!"}

EXAMPLE_MESSAGE_SCHEMA_PATH = BASE_DIR / "example_schema.json"

with open(EXAMPLE_MESSAGE_SCHEMA_PATH, "r", encoding="utf8") as schema_file:
    EXAMPLE_MESSAGE_SCHEMA = json.load(schema_file)

amqp_fixture = amqp_fixture_factory()
