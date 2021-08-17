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

"""This script receives messages to an AMQP Topic"""

import json
from pathlib import Path
import pika
from ghga_service_chassis_lib.pubsub import AmqpTopic

HERE = Path(__file__).parent.resolve()


def process_message(message: dict):
    """process a message"""
    count = message["count"]
    print(f"Received the message number: {count}")


def run():
    """Runs a subscribing process."""

    # read json schema:
    with open(HERE / "message_schema.json", "r") as schema_file:
        message_schema = json.load(schema_file)

    # create a topic object:
    topic = AmqpTopic(
        connection_params=pika.ConnectionParameters(host="rabbitmq"),
        topic_name="my_topic",
        service_name="subscriber",
        json_schema=message_schema,
    )

    # subscribe:
    topic.subscribe_for_ever(exec_on_message=process_message)


if __name__ == "__main__":
    run()