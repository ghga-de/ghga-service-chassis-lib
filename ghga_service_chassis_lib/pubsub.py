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

"""Functionality for publishing or subscribing to a AMQP/RabbitMQ topic"""

import json
import logging
from copy import deepcopy
from datetime import datetime, timezone
from typing import Callable, Optional, Tuple

import jsonschema
import pika
from pydantic import BaseSettings


class PubSubConfigBase(BaseSettings):
    """A base class with config params related to
    asynchronous messaging.
    Inherit your config class from this class if you need
    to run an async PubSub API."""

    service_name: str
    rabbitmq_host: str = "rabbitmq"
    rabbitmq_port: int = 5672


class MaxAttemptsReached(Exception):
    """Raised when the maximum number of attempts has been reached."""


def validate_message(
    message: dict, json_schema: dict, raise_on_exception: bool = False
) -> bool:
    """Validate a message based on a json_schema."""
    try:
        jsonschema.validate(instance=message, schema=json_schema)
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


def callback_factory(
    exec_on_message: Callable,
    json_schema: Optional[dict] = None,
    stop_on_consume: bool = False,
) -> Callable:
    """
    Generates a callback function that is executed whenever a message reaches
    the queue. It performs logging, message validation against a json schema (if provided)
    and, finally, executes the function `exec_on_message`.
    If `stop_on_cosume` is set to `True`, a signal is sends that terminates the
    `channel.start_consuming()` loop.
    """

    def callback(
        channel: pika.channel.Channel,
        method: pika.spec.Basic.Deliver,
        _: pika.spec.BasicProperties,
        body: str,
    ):
        """A wrapper around the actual function that is executed
        once a message arrives:"""

        if stop_on_consume:
            channel.stop_consuming()

        logging.info(
            " [x] %s: Message received",
            datetime.now(timezone.utc).isoformat(),
        )

        message = json.loads(body)

        if json_schema:
            valid = validate_message(message, json_schema)
            if not valid:
                channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

        try:
            exec_on_message(message)
        except (MaxAttemptsReached, ValueError):
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        else:
            channel.basic_ack(delivery_tag=method.delivery_tag)

    return callback


class AmqpTopic:
    """A base class to connect and iteract to/with a RabbitMQ host
    via the `topic` exchange type.

    Naming patterns for Exchanges and Queues:
        Exchanges will always be named according to the `topic_name`.
        Queue names will be generated by concatenating the `service_name`
        and the `topic_name`
    """

    def __init__(
        self,
        config: PubSubConfigBase,
        topic_name: str,
        json_schema: Optional[dict] = None,
    ):
        """Initialize the AMQP topic.

        Args:
            config [PubSubConfigBase]:
                Config paramaters provided as PubSubConfigBase object.
            topic_name (str):
                The name of the topic (only use letters, numbers, and "_").
                The queue binding key as well as the names for the associated exchange
                and queue will be derived from this string.
            json_schema (Optional[dict]):
                Optional. If provided, the message body will be validated against this
                json schema.
        """
        self.connection_params = pika.ConnectionParameters(
            host=config.rabbitmq_host, port=config.rabbitmq_port
        )
        self.service_name = config.service_name
        self.topic_name = topic_name
        self.json_schema = json_schema
        self.sub_queue_name = f"{self.service_name}.{self.topic_name}"

    def _create_channel_and_exchange(
        self,
    ) -> Tuple[pika.BlockingConnection, pika.channel.Channel]:
        """Creates a channel and declare an exchange.
        Returns the channel object."""
        # open a connection and create a new channel:
        connection = pika.BlockingConnection(self.connection_params)
        channel = connection.channel()

        # declare an exchange:
        channel.exchange_declare(exchange=self.topic_name, exchange_type="topic")

        return connection, channel

    def init_subscriber_queue(
        self,
    ) -> Tuple[pika.BlockingConnection, pika.channel.Channel]:
        """
        Initialize the queue that is used for subscribing to the topic.
        This method is called by the `subscribe_for_ever` method.
        The only reason to use this method outside of `subscribe_for_ever` is if you
        want to create the queue for subscription without immediatly starting to consume
        from it.

        Returns a tuple containing:
            1. the connection to the AMQP broker as pika.BlockingConnection object
            2. a pika channel object in which the subscriber queue is declared
        """

        # open a connection, create a channel, and declare an exchange:
        connection, channel = self._create_channel_and_exchange()

        # declare a new queue:
        channel.queue_declare(queue=self.sub_queue_name, durable=True)

        # bind the queue to the exchange:
        channel.queue_bind(
            exchange=self.topic_name,
            queue=self.sub_queue_name,
            routing_key=f"#.{self.topic_name}.#",
        )

        return connection, channel

    def subscribe(self, exec_on_message: Callable, run_forever: bool = True):
        """Subscribe to a topic and execute the specified function whenever
        a message is received.

        Args:
            exec_on_message (Callable):
                A callable that is executed whenever a message is received.
                This function should take the message payload (as dictionary)
                as a single argument.
            run_forever (bool):
                If `True`, the function will continue to consume messages for ever.
                If `False`, the function will wait for the first message, cosume it,
                and exit. Defaults to `True`.
        """

        _, channel = self.init_subscriber_queue()

        # consume from the channel:
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(
            queue=self.sub_queue_name,
            on_message_callback=callback_factory(
                exec_on_message=exec_on_message,
                json_schema=self.json_schema,
                stop_on_consume=not run_forever,
            ),
        )

        logging.info(
            ' [*] %s: Waiting for messages in topic "%s".',
            datetime.now(timezone.utc).isoformat(),
            self.topic_name,
        )

        channel.start_consuming()

    def publish(self, message: dict):
        """Publish a message to the topic

        Args:
            message (dict):
                The message payload to be send via the topic.
        """

        # add timestamp to message:
        message_stamped = deepcopy(message)
        message_stamped["timestamp"] = datetime.now(timezone.utc).isoformat()

        # validate message:
        if self.json_schema:
            validate_message(message_stamped, self.json_schema, raise_on_exception=True)

        # convert message dict to json:
        message_json = json.dumps(message_stamped)

        # open a connection, create a channel, and declare an exchange:
        connection, channel = self._create_channel_and_exchange()

        # publish the message:
        channel.basic_publish(
            exchange=self.topic_name,
            routing_key=self.topic_name,
            body=message_json,
            properties=pika.BasicProperties(delivery_mode=2),
        )
        logging.info(
            " [x] %s: Sent message.",
            datetime.now(timezone.utc).isoformat(),
        )
        connection.close()
