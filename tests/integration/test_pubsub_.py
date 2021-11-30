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
Test the pubsub module.
These tests require a RabbitMQ Broker to be running
at host "localhost" and port "5672".
You may also adapt the RabbitMQ host by defining the
`RABBITMQ_TEST_HOST` environment variable.
"""

import multiprocessing
from datetime import datetime
from time import sleep

import pytest

from ghga_service_chassis_lib.pubsub import AmqpTopic, PubSubConfigBase
from ghga_service_chassis_lib.pubsub_testing import amqp_fixture


def test_pub_sub(amqp_fixture):
    """Test basic publish subscribe senario"""

    timestamp = str(datetime.now().timestamp())
    test_message = {"timestamp": timestamp}

    def process_message(message_received: dict):
        """proccess the message"""
        # nothing to do in this simple example
        pass

    amqp_fixture.pubsub_exchange()
