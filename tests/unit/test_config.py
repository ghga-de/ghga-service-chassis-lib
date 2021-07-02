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

import os
from ghga_service_chassis_lib.config import yaml_as_config_source
from .fixtures.config import BasicConfig, config_yamls, basic_env_var_fixture


def test_config_from_yaml():
    """Test that config yaml correctly overwrites
    default parameters"""

    config_yaml = config_yamls["basic"]

    # update config class with content of config yaml
    config_constructor = yaml_as_config_source(BasicConfig)
    config = config_constructor(config_yaml=config_yaml.path)

    # compare to expected content:
    expected = BasicConfig(**config_yaml.content)
    assert config.dict() == expected


def test_config_from_yaml_and_env():
    """Test that config yaml correctly overwrites
    default parameters"""

    config_yaml = config_yamls["basic"]

    with basic_env_var_fixture:
        # update config class with content of config yaml and
        # from the env vars
        config_constructor = yaml_as_config_source(BasicConfig)
        config = config_constructor(config_yaml=config_yaml.path)

    # compare to expected content:
    overwrite_params = {**config_yaml.content, **basic_env_var_fixture.env_vars}
    expected = BasicConfig(**overwrite_params)
    assert config.dict() == expected
