#!/usr/bin/env python3

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

"""Extracts the package name from the setup.cfg"""

from pathlib import Path

REPO_ROOT_DIR = Path(__file__).parent.parent.resolve()
SETUP_CFG_PATH = REPO_ROOT_DIR / "setup.cfg"
NAME_PREFIX = "name = "


def run():
    """Extracts the package name"""

    with open(SETUP_CFG_PATH, "r", encoding="utf8") as setup_cfg:
        for line in setup_cfg.readlines():
            line_stripped = line.strip()
            if line_stripped.startswith(NAME_PREFIX):
                package_name = line_stripped[len(NAME_PREFIX) :]
                print(package_name)
                return


if __name__ == "__main__":
    run()
