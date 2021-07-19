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

"""Functionality for initializing, configuring, and running RESTful
webapps with FastAPI"""

from typing import Literal, Type, Union
from pydantic import BaseSettings
import uvicorn
from fastapi import FastAPI


# type alias for log level parameter
LogLevel = Literal["critical", "error", "warning", "info", "debug", "trace"]


class ApiConfigBase(BaseSettings):
    """A base class with API-required config params.
    Inherit your config class from this class if you need
    to run an API backend."""

    host: str = "127.0.0.1"
    port: int = 8080
    log_level: LogLevel = "info"
    auto_reload: bool = False
    workers: int = 1


def run_server(app: Union[str, Type[FastAPI]], config: Type[ApiConfigBase]):
    """Starts backend server.

    Args:
        app_import_path (str, Type[FastAPI]):
            Either a FastAPI app object (auto reload and multiple
            workers won't work) or the import path to the app object.
            The path follows the same style that is also used for
            the console_scripts in a setup.py/setup.cfg
            (see here for an example:
            from ghga_service_chassis_lib.api import run_server).
        config (BaseSettings):
            A pydantic BaseSettings class that contains attributes
            "host", "port", and "log_level".
    """
    uvicorn.run(
        app,
        host=config.host,
        port=config.port,
        log_level=config.log_level,
        reload=config.auto_reload,
        workers=config.workers,
    )
