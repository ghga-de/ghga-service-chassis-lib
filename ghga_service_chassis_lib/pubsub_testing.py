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
This module contains utilities for testing code created with the functionality
from the `pubsub` module.
"""

# Skip pylint as this code should be migrated to the testcontainers-python repo
# (https://github.com/testcontainers/testcontainers-python), uses different styling
# pattern.
# pylint: skip-file

import os
from datetime import datetime, timedelta
from dataclasses import dataclass
from pathlib import Path
from time import sleep
from typing import Callable, Optional, Generator

import pytest
import pika
from testcontainers.core.container import DockerContainer

from ghga_service_chassis_lib.utils import exec_with_timeout

from .pubsub import PubSubConfigBase, AmqpTopic


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
        timeout_deadline = datetime.now() + timedelta(seconds=self.startup_timeout)
        while datetime.now() < timeout_deadline:
            sleep(self._READINESS_RETRY_DELAY)
            if self.readiness_probe():
                return self

        raise ReadinessTimeoutError(
            "The RabbitMQ broker failed to start within the expected time frame."
        )


class MessageSuccessfullyReceived(RuntimeError):
    """This Exception can be used to signal that the message
    was successfully received.
    """

    ...


def message_processing_wrapper(
    func: Callable,
    received_message: dict,
    expected_message: dict,
):
    """
    This function is used in the AmqpFixture to wrap the function `func` specified for
    processing incomming message.
    """
    assert (
        received_message == expected_message
    ), "The published message did not match the received message."

    func(received_message)
    raise MessageSuccessfullyReceived()


class AmqpFixture:
    """Info yielded by the `amqp_fixture` function"""

    def __init__(
        self,
        subscriber_config: PubSubConfigBase,
        publisher_config: PubSubConfigBase,
    ) -> None:
        """Initialize fixture"""
        self.subscriber_config = subscriber_config
        self.publisher_config = publisher_config

    def pubsub_exchange(
        self,
        message: dict,
        exec_on_receive: Callable,
        message_schema: Optional[dict] = None,
        timeout_after: int = 2,
    ):
        """
        Publish a message (`message`) and specify a function that is
        executed once received by the subscriber (`exec_on_receive`).
        """

        def exchange_message():
            """inner function for performing the exchange"""
            # create topic instances used by the subscriber and publisher:
            subscriber_topic = AmqpTopic(
                config=self.subscriber_config,
                topic_name=self.topic_name,
                json_schema=message_schema,
            )

            publish_topic = AmqpTopic(
                config=self.publisher_config,
                topic_name=self.topic_name,
                json_schema=message_schema,
            )

            # initialize subscriber queue so that published messages will be captured:
            subscriber_topic.init_subscriber_queue()

            # publish message:
            publish_topic.publish(message)

            # receive topic by expecting the MessageSuccessfullyReceived exception:
            wrapped_func = lambda received_message: message_processing_wrapper(
                func=exec_on_receive,
                received_message=received_message,
                expected_message=message,
            )

            with pytest.raises(MessageSuccessfullyReceived):
                subscriber_topic.subscribe_for_ever(wrapped_func)

        # run the innter function `exchange_message` with a timer:
        exec_with_timeout(func=exchange_message, timeout_after=timeout_after)


@pytest.fixture
def amqp_fixture(topic_name="my_test_topic") -> Generator[AmqpFixture, None, None]:
    """Pytest fixture for tests of the Prostgres DAO implementation."""

    with RabbitMqContainer() as rabbitmq:
        connection_params = rabbitmq.get_connection_params()

        subscriber_config = PubSubConfigBase(
            rabbitmq_host=connection_params.host,
            rabbitmq_port=connection_params.port,
            service_name="test_publisher",
        )

        publisher_config = PubSubConfigBase(
            rabbitmq_host=connection_params.host,
            rabbitmq_port=connection_params.port,
            service_name="test_publisher",
        )

        yield AmqpFixture(
            subscriber_config=subscriber_config,
            publisher_config=publisher_config,
            topic_name=topic_name,
        )
