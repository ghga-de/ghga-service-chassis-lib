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

"""This script evaluates the entries in .static_files, .mandatory_files and
.deprecated_files and compares them with the microservice template repository,
or verifies their existence or non-existence depending on the list they are in.
"""

import difflib
import shutil
import sys
import urllib.parse
from pathlib import Path

import requests
import typer
from script_utils.cli import echo_failure, echo_success

REPO_ROOT_DIR = Path(__file__).parent.parent.resolve()

DEPRECATED_FILES = ".deprecated_files"
MANDATORY_FILES = ".mandatory_files"
STATIC_FILES = ".static_files"
IGNORE_SUFFIX = "_ignore"
RAW_TEMPLATE_URL = (
    "https://raw.githubusercontent.com/ghga-de/microservice-repository-template/main/"
)


class ValidationError(RuntimeError):
    """Raised when files need to be updated."""


def get_file_list(list_name: str) -> list[str]:
    """Return a list of all file names specified in a given list file."""
    list_path = REPO_ROOT_DIR / list_name
    with open(list_path, "r", encoding="utf8") as list_file:
        file_list = [
            clean_line
            for clean_line in (
                line.rstrip() for line in list_file if not line.startswith("#")
            )
            if clean_line
        ]
    if not list_name.endswith(IGNORE_SUFFIX):
        ignore_list_name = list_name + IGNORE_SUFFIX
        try:
            file_set_ignore = set(get_file_list(ignore_list_name))
        except FileNotFoundError:
            print(f"  - {ignore_list_name} is missing, no exceptions from the template")
        else:
            file_list = [line for line in file_list if line not in file_set_ignore]
    return file_list


def get_template_file_content(relative_file_path: str):
    """Get the content of the template file corresponding ot the given path."""
    remote_file_url = urllib.parse.urljoin(RAW_TEMPLATE_URL, relative_file_path)
    remote_file_request = requests.get(remote_file_url)
    if remote_file_request.status_code != 200:
        print(
            f"  - WARNING: request to remote file {remote_file_url} returned"
            f" status code {remote_file_request.status_code}"
        )
        return None
    return remote_file_request.text


def diff_content(local_file_path, local_file_content, template_file_content) -> bool:
    """Show diff between given local and remote template file content."""
    if local_file_content != template_file_content:
        print(f"  - {local_file_path}: differs from template")
        for line in difflib.unified_diff(
            template_file_content.splitlines(keepends=True),
            local_file_content.splitlines(keepends=True),
            fromfile="template",
            tofile="local",
        ):
            print("   ", line.rstrip())
        return True
    return False


def update_files(files: list[str], diff: bool = False, check: bool = False) -> bool:
    """Update or check all the files in the given list"""
    ok = True
    for relative_file_path in files:

        local_file_path = REPO_ROOT_DIR / Path(relative_file_path)
        local_parent_dir = local_file_path.parent
        if check:
            if not local_file_path.exists():
                print(f"  - {local_file_path} does not exist")
                ok = False
            elif diff:
                template_file_content = get_template_file_content(relative_file_path)
                if template_file_content is None:
                    print(f"  - {local_file_path}: cannot check, remote is missing")
                    ok = False
                else:
                    with open(local_file_path, "r", encoding="utf8") as local_file:
                        local_file_content = local_file.read()
                        if diff_content(
                            local_file_path, local_file_content, template_file_content
                        ):
                            ok = False
        else:
            if not local_parent_dir.exists():
                local_parent_dir.mkdir(parents=True)
            if diff or not local_file_path.exists():
                ok = False
                template_file_content = get_template_file_content(relative_file_path)
                if template_file_content is None:
                    print(f"  - {local_file_path}: cannot update, remote is missing")

                else:
                    with open(local_file_path, "w", encoding="utf8") as local_file:
                        local_file.write(template_file_content)
                    print(f"  - {local_file_path}: updated")

    return ok


def remove_files(files: list[str], check: bool = False) -> bool:
    """Remove or check all the files in the given list"""
    ok = True
    for relative_file_path in files:
        local_file_path = REPO_ROOT_DIR / Path(relative_file_path)

        if local_file_path.exists():
            ok = False
            if check:
                print(f"  - {local_file_path}: deprecated, but exists")
            else:
                shutil.rmtree(local_file_path)
                print(f"  - {local_file_path}: removed, since it is deprecated")
    return ok


def cli_main(check: bool = False):
    """Update the static files in the service template."""
    ok = True
    if not check:
        update_files([STATIC_FILES], check=False)

    print("Static files...")
    files_to_update = get_file_list(STATIC_FILES)
    if check:
        files_to_update.append(STATIC_FILES)
    files_to_update.extend((MANDATORY_FILES, DEPRECATED_FILES))
    if not update_files(files_to_update, diff=True, check=check):
        ok = False

    print("Mandatory files...")
    files_to_guarantee = get_file_list(MANDATORY_FILES)
    if not update_files(files_to_guarantee, check=check):
        ok = False

    print("Deprecated files...")
    files_to_remove = get_file_list(DEPRECATED_FILES)
    if not remove_files(files_to_remove, check=check):
        ok = False

    if not ok:
        echo_failure("Validating the template files failed.")
        sys.exit(1)

    echo_success(
        "Successfully validated the template files."
        if check
        else "Successfully updated the template files."
    )


def main():
    """Main function that runs the CLI."""
    typer.run(cli_main)


if __name__ == "__main__":
    main()
