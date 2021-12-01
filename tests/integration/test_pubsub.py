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

from ghga_service_chassis_lib.pubsub import AmqpTopic, PubSubConfigBase
from ghga_service_chassis_lib.pubsub_testing import RabbitMqContainer


def test_pub_sub():
    """Test basic publish subscribe senario"""

    timestamp = str(datetime.now().timestamp())
    topic_name = "test_pub_sub"
    test_message = {"timestamp": timestamp}

    with RabbitMqContainer() as rabbitmq:
        connection_params = rabbitmq.get_connection_params()

        # publish the message:
        def publish(topic_name: str, test_message: dict):
            """function that runs as background process"""
            # sleep shortly to make sure that the subscriber starts first:
            sleep(0.2)

            # create a topic object:
            topic = AmqpTopic(
                config=PubSubConfigBase(
                    rabbitmq_host=connection_params.host,
                    rabbitmq_port=connection_params.port,
                    service_name="test_publisher",
                ),
                topic_name=topic_name,
                json_schema=None,
            )

            # send a test message:
            topic.publish(test_message)

        process = multiprocessing.Process(
            target=publish,
            kwargs={"topic_name": topic_name, "test_message": test_message},
        )
        process.start()

        # receive the message:
        topic2 = AmqpTopic(
            config=PubSubConfigBase(
                rabbitmq_host=connection_params.host,
                rabbitmq_port=connection_params.port,
                service_name="test_subscriber",
            ),
            topic_name=topic_name,
            json_schema=None,
        )

        def process_message(message_received: dict):
            """proccess the message"""
            assert message_received == test_message

        topic2.subscribe(exec_on_message=process_message, run_forever=False)
