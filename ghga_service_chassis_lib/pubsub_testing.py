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
This module contains utilities for testing code created with the functionality
from the `pubsub` module.
"""

# Skip pylint as this code should be migrated to the testcontainers-python repo
# (https://github.com/testcontainers/testcontainers-python), uses different styling
# pattern.
# pylint: skip-file

import copy
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from time import sleep
from typing import Generator, Optional

import pika
import pytest
from testcontainers.core.container import DockerContainer

from ghga_service_chassis_lib.utils import exec_with_timeout

from .pubsub import AmqpTopic, PubSubConfigBase


class ReadinessTimeoutError(TimeoutError):
    """Thrown when readiness probes failed until the startup timeout is reached."""

    pass


class RabbitMqContainer(DockerContainer):
    """
    Test container for RabbitMQ.

    Example
    -------
    The example spins up a RabbitMQ broker and uses the `pika` client library
    (https://pypi.org/project/pika/) establish a connection to the broker.
    ::
        from testcontainer.rabbitmq import RabbitMqContainer
        import pika

        with RabbitMqContainer("rabbitmq:3.9.10") as rabbitmq:

            connection = pika.BlockingConnection(rabbitmq.get_connection_params())
            channel = connection.channel()
    """

    _CONFIG_FILE_FROM_ENV: Optional[str] = os.environ.get("RABBITMQ_CONFIG_FILE")
    _READINESS_RETRY_DELAY: float = 0.1

    def __init__(
        self,
        image: str = "rabbitmq:latest",
        port: int = 5672,
        config_file_path: Optional[Path] = None,
        startup_timeout: int = 60,
    ) -> None:
        """Initialize the RabbitMQ test container.

        Args:
            image (str, optional):
                The docker image from docker hub. Defaults to "rabbitmq:latest".
            port (int, optional):
                The port to reach the AMQP API. Defaults to 5672.
            config_file_path (Optional[Path], optional):
                Path to RabbitMQ config file (`*.conf`). See the RabbitMQ documentation for further
                details:
                https://www.rabbitmq.com/configure.html#configuration-files
                Defaults to None.
            startup_timeout (int, optional):
                The maximally allowed startup time in seconds. The AMQP API should be reachable
                by then or an ReadinessTimeoutError is thrown.
                Defaults to 60.
        """
        super(RabbitMqContainer, self).__init__(image=image)
        self.RABBITMQ_NODE_PORT = port
        self.startup_timeout = startup_timeout

        # Use the config file path either from the function argument or the env var
        # or fall back to `None`:
        self.RABBITMQ_CONFIG_FILE: Optional[Path]
        if config_file_path is not None:
            self.RABBITMQ_CONFIG_FILE = config_file_path
        elif self._CONFIG_FILE_FROM_ENV is not None:
            self.RABBITMQ_CONFIG_FILE = Path(self._CONFIG_FILE_FROM_ENV)
        else:
            self.RABBITMQ_CONFIG_FILE = None

        self.with_exposed_ports(self.RABBITMQ_NODE_PORT)
        self.with_env("RABBITMQ_NODE_PORT", self.RABBITMQ_NODE_PORT)
        if config_file_path is not None:
            self.with_env("RABBITMQ_CONFIG_FILE", self.RABBITMQ_CONFIG_FILE)

    def readiness_probe(self) -> bool:
        """Test if the RabbitMQ broker is ready."""
        try:
            connection = pika.BlockingConnection(self.get_connection_params())
            if connection.is_open:
                connection.close()
                return True
        except pika.exceptions.AMQPConnectionError:
            pass

        return False

    def get_connection_params(self) -> pika.ConnectionParameters:
        """
        Get connection params as a pika.ConnectionParameters object.
        For more details see:
        https://pika.readthedocs.io/en/latest/modules/parameters.html
        """
        return pika.ConnectionParameters(
            host=self.get_container_host_ip(),
            port=self.get_exposed_port(self.RABBITMQ_NODE_PORT),
        )

    def start(self):
        """Start the test container."""
        super().start()

        # wait until RabbitMQ is ready:
        timeout_deadline = datetime.now(timezone.utc) + timedelta(
            seconds=self.startup_timeout
        )
        while datetime.now(timezone.utc) < timeout_deadline:
            sleep(self._READINESS_RETRY_DELAY)
            if self.readiness_probe():
                return self

        raise ReadinessTimeoutError(
            "The RabbitMQ broker failed to start within the expected time frame."
        )


class TestPubSubClient:
    """A base class used to simulate publishing or subscribing services."""

    def __init__(
        self,
        config: PubSubConfigBase,
        subscriber_service_name: str,
        topic_name: str,
        message_schema: Optional[dict] = None,
    ):
        """
        This does not only create a AmqpTopic object that is later used for
        publishing/subscribing but it also already initializes the channel that will be
        used for subscription.
        """

        self.config = config
        self.topic_name = topic_name
        self.message_schema = message_schema
        self.subscriber_service_name = subscriber_service_name

        # create topic later used for publishing/subscribing:
        self.topic = AmqpTopic(
            config=self.config,
            topic_name=self.topic_name,
            json_schema=self.message_schema,
        )

        # initialize the channel that is later used for subscription:
        subscriber_topic: AmqpTopic
        if self.config.service_name == subscriber_service_name:
            subscriber_topic = self.topic
        else:
            subscriber_config = copy.deepcopy(self.config)
            subscriber_config.service_name = self.subscriber_service_name
            subscriber_topic = AmqpTopic(
                config=subscriber_config,
                topic_name=self.topic_name,
            )

        subscriber_topic.init_subscriber_queue()


class TestPublisher(TestPubSubClient):
    """A class simulating a service that publishes to the specified topic."""

    def __init__(
        self,
        config: PubSubConfigBase,
        subscriber_service_name: str,
        topic_name: str,
        message_schema: Optional[dict] = None,
    ):
        """Initialize the test publisher."""
        super().__init__(
            config=config,
            message_schema=message_schema,
            topic_name=topic_name,
            subscriber_service_name=subscriber_service_name,
        )

    def publish(self, message: dict):
        """publish a message"""

        self.topic.publish(message)


class TestSubscriber(TestPubSubClient):
    """A class simulating a service that subscribes to the specified topic."""

    def __init__(
        self,
        config: PubSubConfigBase,
        topic_name: str,
        message_schema: Optional[dict] = None,
    ):
        """Initialize the test subscriber."""
        super().__init__(
            config=config,
            message_schema=message_schema,
            topic_name=topic_name,
            subscriber_service_name=config.service_name,
        )

    def subscribe(
        self,
        expected_message: Optional[dict] = None,
        timeout_after: int = 2,
    ) -> dict:
        """
        Subscribe to the channel and expect the specified message (`exected_message`).
        A TimeoutError is thrown after the specified number of seconds (`timeout_after`).
        It returns the received message.
        """

        message_to_return: dict = {}  # will be filled by the `process_message` function

        def process_message(message: dict, update_with_message: dict):
            """Process the incoming message and update the `update_with_message``
            with the message content"""
            if expected_message is not None:
                message_stripped = copy.deepcopy(message)
                del message_stripped["timestamp"]
                assert (  # nosec
                    message_stripped == expected_message
                ), "The content of the received message did not match the expectations."

            update_with_message.update(message)

        exec_with_timeout(
            func=self.topic.subscribe,
            func_kwargs={
                "exec_on_message": lambda message: process_message(
                    message, message_to_return
                ),
                "run_forever": False,
            },
            timeout_after=timeout_after,
        )

        # return the `message_to_return` dict that was populated by the
        # `process_message` function:
        return message_to_return


class AmqpFixture:
    """Info yielded by the `amqp_fixture` function"""

    def __init__(self, config: PubSubConfigBase) -> None:
        """Initialize fixture"""
        self.config = config

    def get_test_publisher(
        self,
        topic_name: str,
        service_name: str = "upstream_publisher",
        message_schema: Optional[dict] = None,
    ):
        """
        Get a TestPublisher object that simulates a service that publishes to the
        specified topic.
        Please note, the function has to be called before calling the subscribing
        function.
        """

        pub_config = PubSubConfigBase(
            rabbitmq_host=self.config.rabbitmq_host,
            rabbitmq_port=self.config.rabbitmq_port,
            service_name=service_name,
        )

        return TestPublisher(
            config=pub_config,
            subscriber_service_name=self.config.service_name,
            topic_name=topic_name,
            message_schema=message_schema,
        )

    def get_test_subscriber(
        self,
        topic_name: str,
        service_name: str = "downstream_subscriber",
        message_schema: Optional[dict] = None,
    ):
        """
        Get TestSubscriber object that simulates a service that subscribes to the
        specified topic.
        Please note, the function has to be called before publishing a message to the
        specified topic.
        """
        sub_config = PubSubConfigBase(
            rabbitmq_host=self.config.rabbitmq_host,
            rabbitmq_port=self.config.rabbitmq_port,
            service_name=service_name,
        )

        return TestSubscriber(
            config=sub_config,
            topic_name=topic_name,
            message_schema=message_schema,
        )


def amqp_fixture_factory(service_name: str = "my_service"):
    """A factory for creating Pytest fixture for working with AMQP."""

    @pytest.fixture
    def amqp_fixture() -> Generator[AmqpFixture, None, None]:
        """Pytest fixture for working with AMQP."""

        with RabbitMqContainer() as rabbitmq:
            connection_params = rabbitmq.get_connection_params()

            config = PubSubConfigBase(
                rabbitmq_host=connection_params.host,
                rabbitmq_port=connection_params.port,
                service_name=service_name,
            )

            yield AmqpFixture(config=config)

    return amqp_fixture
