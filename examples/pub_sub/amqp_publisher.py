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

"""This script publishes messages to an AMQP Topic"""

# pylint: disable-all


import json
from pathlib import Path

from ghga_service_chassis_lib.pubsub import AmqpTopic, PubSubConfigBase

HERE = Path(__file__).parent.resolve()


def run():
    """Runs publishing process."""

    # read json schema:
    with open(HERE / "message_schema.json", "r") as schema_file:
        message_schema = json.load(schema_file)

    # create a topic object:
    config = PubSubConfigBase(service_name="publisher")
    topic = AmqpTopic(
        config=config,
        topic_name="my_topic",
        json_schema=message_schema,
    )

    # publish 10 messages:
    for count in range(0, 10):
        message = {"count": count}
        topic.publish(message)


if __name__ == "__main__":
    run()
