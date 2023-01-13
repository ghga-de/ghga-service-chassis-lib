#!/usr/bin/env python3

# Copyright 2021 - 2023 Universität Tübingen, DKFZ and EMBL
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

"""Generates a JSON schema from the service's Config class as well as a corresponding
example config yaml (or check whether these files are up to date).
"""

import importlib
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Type

import typer
import yaml
from pydantic import BaseSettings
from script_utils.cli import echo_failure, echo_success

HERE = Path(__file__).parent.resolve()
REPO_ROOT_DIR = HERE.parent
DEV_CONFIG_YAML = REPO_ROOT_DIR / ".devcontainer" / ".dev_config.yaml"
GET_PACKAGE_NAME_SCRIPT = HERE / "get_package_name.py"
EXAMPLE_CONFIG_YAML = REPO_ROOT_DIR / "example_config.yaml"
CONFIG_SCHEMA_JSON = REPO_ROOT_DIR / "config_schema.json"


class ValidationError(RuntimeError):
    """Raised when validation of config documentation failes."""


def get_config_class() -> Type[BaseSettings]:
    """
    Dynamically imports and returns the Config class from the current service.
    This makes the script service repo agnostic.
    """
    # get the name of the microservice package
    with subprocess.Popen(
        args=[GET_PACKAGE_NAME_SCRIPT],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
    ) as process:
        assert (
            process.wait() == 0 and process.stdout is not None
        ), "Failed to get package name."
        package_name = process.stdout.read().decode("utf-8").strip("\n")

    # import the Config class from the microservice package:
    config_module: Any = importlib.import_module(f"{package_name}.config")
    config_class = config_module.Config

    return config_class


def get_dev_config():
    """Get dev config object."""
    config_class = get_config_class()
    return config_class(config_yaml=DEV_CONFIG_YAML)


def get_schema() -> str:
    """Returns a JSON schema generated from a Config class."""

    config = get_dev_config()
    return config.schema_json(indent=2)


def get_example() -> str:
    """Retruns an example config yaml."""

    config = get_dev_config()
    normalized_config_dict = json.loads(config.json())
    return yaml.dump(normalized_config_dict)


def update_docs():
    """Update the example config and config schema files documenting the config
    options."""

    example = get_example()
    with open(EXAMPLE_CONFIG_YAML, "w", encoding="utf-8") as example_file:
        example_file.write(example)

    schema = get_schema()
    with open(CONFIG_SCHEMA_JSON, "w", encoding="utf-8") as schema_file:
        schema_file.write(schema)


def check_docs():
    """Check whether the example config and config schema files documenting the config
    options are up to date.

    Raises:
        ValidationError: if not up to date.
    """

    example_expected = get_example()
    with open(EXAMPLE_CONFIG_YAML, "r", encoding="utf-8") as example_file:
        example_observed = example_file.read()
    if example_expected != example_observed:
        raise ValidationError(
            f"Example config YAML at '{EXAMPLE_CONFIG_YAML}' is not up to date."
        )

    schema_expected = get_schema()
    with open(CONFIG_SCHEMA_JSON, "r", encoding="utf-8") as schema_file:
        schema_observed = schema_file.read()
    if schema_expected != schema_observed:
        raise ValidationError(
            f"Config schema JSON at '{CONFIG_SCHEMA_JSON}' is not up to date."
        )


def cli_main(check: bool = False):
    """Main function to be run by the typer CLI to update or check config documentation
    files."""

    if check:
        try:
            check_docs()
        except ValidationError as error:
            echo_failure(f"Validation failed: {error}")
            sys.exit(1)
        echo_success("Config docs are up to date.")
        return

    update_docs()
    echo_success("Successfully updated the config docs.")


def main():
    """Main function that runs the CLI."""
    typer.run(cli_main)


if __name__ == "__main__":
    main()
