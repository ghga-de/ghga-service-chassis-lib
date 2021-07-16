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
from typing import Dict, Any, Optional, Callable, Final
from pydantic import BaseSettings
import yaml

# Default config prefix:
DEFAULT_CONFIG_PREFIX: Final = "ghga_services"


def get_default_config_yaml(prefix: str) -> Optional[str]:
    """Get the path to the default config function.

    Args:
        prefix (str):
            Name prefix used to derive the default paths.
    """
    # construct file name from prefix:
    file_name = f".{prefix}.yaml"

    # look in the current directory:
    default_pwd_path = os.path.join(os.getcwd(), file_name)
    if os.path.isfile(default_pwd_path):
        return default_pwd_path

    # look in the home directory:
    default_home_path = os.path.join(pathlib.Path.home(), file_name)
    if os.path.isfile(default_home_path):
        return default_home_path

    # if nothing was found return None:
    return None


def yaml_settings_factory(
    config_yaml: Optional[str] = None,
) -> Callable[[BaseSettings], Dict[str, Any]]:
    """
    A factory of source methods for pydantic's BaseSettings Config that load
    settings from a yaml file.

    Args:
        config_yaml (str, Optional):
            Path to the yaml file to read from.
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


def config_from_yaml(
    prefix: str = DEFAULT_CONFIG_PREFIX,
) -> Callable:
    """A factory that returns decorator functions which extends a
    pydantic BaseSettings class to read in parameters from a config yaml.
    It replaces (or adds) a Config subclass to the BaseSettings class that configures
    the priorities for parameter sources as follows (highest Priority first):
        - parameters passed using **kwargs
        - environment variables
        - file secrets
        - yaml config file
        - defaults

    Args:
        prefix: (str, optional):
            When defining parameters via enviroment variables, all variables
            have to be prefixed with this string following this pattern
            "{prefix}_{actual_variable_name}". Moreover, this prefix is used
            to derive the default location for the config yaml file
            ("~/.{prefix}.yaml"). Defaults to "ghga_services".
    """

    def decorator(settings) -> Callable:
        # settings should be a pydantic BaseSetting,
        # there is no type hint here to not restrict
        # autocompletion for attributes of the
        # modified settings class returned by the
        # contructor_wrapper
        """The actual decorator function.

        Args
            settings (BaseSettings):
                A pydantic BaseSettings class to be modified.
        """

        # check if settings inherits from pydantic BaseSettings:
        if not issubclass(settings, BaseSettings):
            raise TypeError(
                "The specified settings class is not a subclass of pydantic.BaseSettings"
            )

        def constructor_wrapper(
            config_yaml: Optional[str] = None,
            **kwargs,
        ):
            """A wrapper for constructing a pydantic BaseSetting with modified sources

            Args:
                config_yaml (str, optional):
                    Path to a config yaml. Overwrites the default location.
            """

            # get default path if config_yaml not specified:
            if config_yaml is None:
                config_yaml = get_default_config_yaml(prefix)

            class ModSettings(settings):  # type: ignore
                """Modifies the orginal Settings class provided by the user"""

                class Config:
                    """pydantic Config subclass"""

                    # add this prefix to all variable names to
                    # define them as environment variables:
                    env_prefix = f"{prefix}_"

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

    return decorator
