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

"""Config parsing functionality based on pydantic's BaseSettings"""

import os
import pathlib
from typing import Literal, Dict, Any, Optional, Callable, Type
from pydantic import BaseSettings
import yaml

# Default config prefix:
DEFAULT_CONFIG_PREFIX = "ghga_services"

# type alias for log level parameter
LogLevel = Literal["critical", "error", "warning", "info", "debug", "trace"]


def yaml_settings_factory(
    config_yaml: Optional[str] = None,
) -> Callable[[BaseSettings], Dict[str, Any]]:
    """
    A factory of source methods for pydantic's BaseSettings Config that load
    settings from a yaml file.
    """

    def yaml_settings(  # pylint: disable=unused-argument
        settings: BaseSettings,
    ) -> Dict[str, Any]:
        """source method for loading pydantic BaseSettings from a yaml file"""
        if config_yaml is None:
            return {}

        with open(config_yaml, "r") as yaml_file:
            return yaml.safe_load(yaml_file)

    return yaml_settings


def yaml_as_config_source(settings: Type[BaseSettings]) -> Callable:
    """A decorator function that extends a pydantic BaseSettings class
    to read in parameters from a config yaml.
    It replaces (or adds) a Config subclass to the BaseSettings class that configures
    the priorities for parameter sources as follows (highest Priority first):
        - parameters passed using **kwargs
        - environment variables
        - file secrets
        - yaml config file
        - defaults


    Args:
        Settings (BaseSettings): [description]
    """

    def constructor_wrapper(
        config_yaml: Optional[str] = None,
        config_prefix: str = DEFAULT_CONFIG_PREFIX,
        **kwargs,
    ) -> Callable:
        """A wrapper for constructing a pydantic BaseSetting with modified sources

        Args:
            config_yaml (str, optional):
                Path to a config yaml. Defaults to "~/.{config_prefix}.yaml"
                (see below).
            config_prefix: (str, optional):
                When defining parameters via enviroment variables, all variables
                have to be prefixed with this string following this pattern
                "{config_prefix}_{actual_variable_name}". Moreover, this prefix is used
                to derive the default location for the config yaml file
                ("~/.{config_prefix}.yaml"). Defaults to "ghga_services".
        """

        # if config yaml not given try to find it at the default location:
        if config_yaml is None:
            default_config_yaml = os.path.join(
                pathlib.Path.home(), f".{config_prefix}.yaml"
            )
            if os.path.isfile(default_config_yaml):
                config_yaml = default_config_yaml

        class ModSettings(settings):  # type: ignore
            """Modifies the orginal Settings class provided by the user"""

            class Config:
                """pydantic Config subclass"""

                # add this prefix to all variable names to
                # define them as environment variables:
                env_prefix = f"{config_prefix}_"

                @classmethod
                def customise_sources(
                    cls,
                    init_settings,
                    env_settings,
                    file_secret_settings,
                ):
                    """add custom yaml source"""
                    return (
                        init_settings,
                        env_settings,
                        file_secret_settings,
                        yaml_settings_factory(config_yaml),
                    )

        # construct settings class:
        return ModSettings(**kwargs)

    return constructor_wrapper
