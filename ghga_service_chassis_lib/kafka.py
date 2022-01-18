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

"""Functionality for publishing or subscribing to a Kafka topic"""

import json
import logging
from copy import deepcopy
from datetime import datetime, timezone
from typing import Callable, Optional, Tuple, List

import jsonschema
import pika
from pydantic import BaseSettings, pydantic
from kafka import KafkaConsumer, KafkaProducer
from ghga_message_schemas import SCHEMAS


class KafkaConfigBase(BaseSettings):
    """A base class with config params related to
    asynchronous messaging.
    Inherit your config class from this class if you need
    to run an async PubSub API."""

    service_name: str
    client_suffix: str

    kafka_servers: List[str]


def validate_event_value(
    value: dict, json_schema: dict, raise_on_exception: bool = False
) -> bool:
    """Validate a value based on a json_schema."""
    try:
        jsonschema.validate(instance=value, schema=json_schema)
        return True
    except jsonschema.exceptions.ValidationError as exc:
        logging.error(
            "%s: Message payload does not comform to JSON schema.",
            datetime.now(timezone.utc).isoformat(),
        )
        logging.exception(exc)
        if raise_on_exception:
            raise exc
        return False


class KafkaTopic:
    """A base class to connect and iteract to/with a Kafka host."""

    def __init__(
        self,
        config: KafkaConfigBase,
        topic_name: str,
        json_schema: Optional[dict] = None,
    ):
        """Initialize the AMQP topic.

        Args:
            config [KafkaConfigBase]:
                Config paramaters provided as KafkaConfigBase object.
            topic_name (str):
                The name of the topic (only use letters, numbers, and "_").
            json_schema (Optional[dict]):
                Optional. If provided, the message body will be validated against this
                json schema.
        """
        self.bootstrap_servers = config.kafka_servers
        self.service_name = config.service_name
        self.client_suffix = config.client_suffix
        self.client_id = f"{self.service_name}.{self.client_suffix}"
        self.topic_name = topic_name
        self.json_schema = json_schema

        self.producer_ = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            client_id=self.client_id,
            value_serializer=lambda m: json.dumps(m).encode("ascii"),
        )

    def subscribe(
        self, exec_on_event: Callable[[str, dict], None], run_forever: bool = True
    ):
        """Subscribe to a topic and execute the specified function whenever
        a message is received.

        Args:
            exec_on_message (Callable[[str, dict], None]):
                A callable that is executed whenever an event is received.
                The callable takes the event key (a string) as the first argument and
                the event value (a dict) as the second argument.
            run_forever (bool):
                If `True`, the function will continue to consume event for ever.
                If `False`, the function will wait for the first event, cosume it,
                and exit. Defaults to `True`.
        """
        consumer = KafkaConsumer(
            self.topic_name,
            client_id=self.client_id,
            group_id=self.service_name,
            bootstrap_servers=self.bootstrap_servers,
        )

        if run_forever:
            for event in consumer:
                exec_on_event(event.key, event.value)
        else:
            event = next(consumer)

    def publish(self, key: str, value: dict):
        """Publish a message to the topic

        Args:
            key (str):
                The event key as str.
            value (dict):
                The event value as dict.
        """

        validate_event_value(value, json_schema=SCHEMAS[key], raise_on_exception=True)

        self.producer_.send(self.topic_name, {key: value})
