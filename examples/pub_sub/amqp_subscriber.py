# Copyright 2021 - 2023 Universität Tübingen, DKFZ and EMBL
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

from ghga_service_chassis_lib.pubsub import AmqpTopic, PubSubConfigBase

HERE = Path(__file__).parent.resolve()


def process_message(message: dict):
    """process a message"""
    count = message["count"]
    print(f"Received the message number: {count}")


def run():
    """Runs a subscribing process."""

    # read json schema:
    with open(HERE / "message_schema.json", "r", encoding="utf8") as schema_file:
        message_schema = json.load(schema_file)

    # create a topic object:
    config = PubSubConfigBase(service_name="subscriber")
    topic = AmqpTopic(
        config=config,
        topic_name="my_topic",
        json_schema=message_schema,
    )

    # subscribe:
    topic.subscribe(exec_on_message=process_message)


if __name__ == "__main__":
    run()
