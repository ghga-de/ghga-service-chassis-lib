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
Test the pubsub module.
These tests require a RabbitMQ Broker to be running
at host "localhost" and port "5672".
You may also adapt the RabbitMQ host by defining the
`RABBITMQ_TEST_HOST` environment variable.
"""

from copy import deepcopy

from ghga_service_chassis_lib.pubsub import AmqpTopic
from ghga_service_chassis_lib.utils import exec_with_timeout

from .fixtures.pubsub import amqp_fixture  # noqa: F401
from .fixtures.pubsub import EXAMPLE_MESSAGE, EXAMPLE_MESSAGE_SCHEMA, EXAMPLE_TOPIC_NAME


def test_publishing(amqp_fixture):  # noqa: F811
    """Test basic publish senario"""
    downstream_subscriber = amqp_fixture.get_test_subscriber(
        topic_name=EXAMPLE_TOPIC_NAME,
        message_schema=EXAMPLE_MESSAGE_SCHEMA,
    )

    topic = AmqpTopic(config=amqp_fixture.config, topic_name=EXAMPLE_TOPIC_NAME)
    topic.publish(EXAMPLE_MESSAGE)

    downstream_subscriber.subscribe(expected_message=EXAMPLE_MESSAGE, timeout_after=2)


def test_subscribing(amqp_fixture):  # noqa: F811
    """Test basic subscribe senario"""
    upstream_publisher = amqp_fixture.get_test_publisher(
        topic_name=EXAMPLE_TOPIC_NAME,
        message_schema=EXAMPLE_MESSAGE_SCHEMA,
    )

    upstream_publisher.publish(EXAMPLE_MESSAGE)

    def process_message(message: dict):
        """Process the incomming message."""

        message_stripped = deepcopy(message)
        del message_stripped["timestamp"]
        assert (
            message_stripped == EXAMPLE_MESSAGE
        ), "The content of the received message did not match the expectations."

    topic = AmqpTopic(
        config=amqp_fixture.config,
        topic_name=EXAMPLE_TOPIC_NAME,
        json_schema=EXAMPLE_MESSAGE_SCHEMA,
    )
    exec_with_timeout(
        func=lambda: topic.subscribe(
            exec_on_message=process_message, run_forever=False
        ),
        timeout_after=2,
    )
