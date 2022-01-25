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
#

import json

from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from testcontainers.kafka import KafkaContainer

with KafkaContainer(port_to_expose=9888) as kafka:

    topic = "test_topic"
    bootstrap_servers = [kafka.get_bootstrap_server()]

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        client_id="test_prod",
    )

    print("produce:")
    producer.send(topic, key="test".encode("ascii"), value="test".encode("ascii"))
    producer.flush()

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        client_id="test_prod.1",
        group_id="test_prod",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )

    print("consume:")
    print(next(consumer))
