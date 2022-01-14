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

"""Functionality for initializing, configuring, and running RESTful
webapps with FastAPI"""

from typing import Dict, Literal, Optional, Sequence, Union

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseSettings

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
    api_root_path: str = "/"
    openapi_url: str = "/openapi.json"
    docs_url: str = "/docs"

    # Starlettes defaults will only be overwritten if a
    # non-None value is specified:
    cors_allowed_origins: Optional[Sequence[str]] = None
    cors_allow_credentials: Optional[bool] = None
    cors_allowed_methods: Optional[Sequence[str]] = None
    cors_allowed_headers: Optional[Sequence[str]] = None


def configure_app(app: FastAPI, config: ApiConfigBase):
    """Configure a FastAPI app based on a config object."""

    app.root_path = config.api_root_path
    app.openapi_url = config.openapi_url
    app.docs_url = config.docs_url

    # configure CORS:
    kwargs: Dict[str, Optional[Union[Sequence[str], bool]]] = {}
    if config.cors_allowed_origins is not None:
        kwargs["allow_origins"] = config.cors_allowed_origins
    if config.cors_allowed_headers is not None:
        kwargs["allow_headers"] = config.cors_allowed_headers
    if config.cors_allowed_methods is not None:
        kwargs["allow_methods"] = config.cors_allowed_methods
    if config.cors_allow_credentials is not None:
        kwargs["allow_credentials"] = config.cors_allow_credentials

    app.add_middleware(CORSMiddleware, **kwargs)


def run_server(app: Union[str, FastAPI], config: ApiConfigBase):
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
