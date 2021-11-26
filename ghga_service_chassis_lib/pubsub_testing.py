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
from pathlib import Path
from typing import Optional

import pika
from testcontainers.core.container import DockerContainer


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

    _CONFIG_FILE_FROM_ENV = os.environ.get("RABBITMQ_CONFIG_FILE")

    def __init__(
        self,
        image: str = "rabbitmq:latest",
        port: int = 5672,
        config_file_path: Optional[Path] = None,
    ) -> None:
        super(RabbitMqContainer, self).__init__(image=image)
        self.RABBITMQ_NODE_PORT = port

        # Use the config file path either from the function argument or the env var
        # or fall back to `None`:
        self.RABBITMQ_CONFIG_FILE: Optional[Path]
        if config_file_path is not None:
            self.RABBITMQ_CONFIG_FILE = config_file_path
        elif self._CONFIG_FILE_FROM_ENV is not None:
            self.RABBITMQ_CONFIG_FILE = Path(self._CONFIG_FILE_FROM_ENV)
        else:
            self.RABBITMQ_CONFIG_FILE = None

        self.with_exposed_ports(self.port)
        self.with_env("RABBITMQ_NODE_PORT", port)
        if config_file_path is not None:
            self.with_env("RABBITMQ_CONFIG_FILE", self.RABBITMQ_CONFIG_FILE)

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
