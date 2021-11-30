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
from pathlib import Path
from time import sleep
from typing import Optional

import pika
from testcontainers.core.container import DockerContainer


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