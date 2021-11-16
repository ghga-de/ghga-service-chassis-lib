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

# pylint: skip-file

"""This script checks that the license and license headers
exists and that they are up to date.
"""

import argparse
import re
import sys
from datetime import date
from pathlib import Path
from typing import List, Tuple

# root directory of the package:
ROOT_DIR = Path(__file__).parent.parent.resolve()

# exlude files and dirs from license header check:
EXCLUDE = [
    ".devcontainer",
    "eggs",
    ".eggs",
    "dist",
    "build",
    "develop-eggs",
    "lib",
    "lib62",
    "parts",
    "sdist",
    "wheels",
    "pip-wheel-metadata",
    ".git",
    ".github",
    ".flake8",
    ".gitignore",
    ".pylintrc",
    "example-config.yaml",
    "LICENSE",  # is checked but not for the license header
    ".pre-commit-config.yaml",
    "docs",
    "requirements.txt",
    ".vscode",
    ".mypy_cache",
    "db_migration",
    ".pytest_cache",
    ".editorconfig",
    ".static_files",
    ".mandatory_files",
]

# exclude file by file ending from license header check:
EXCLUDE_ENDINGS = ["json", "pyc", "yaml", "yml", "md", "html", "xml"]

# exclude any files with names that match any of the following regex:
EXCLUDE_PATTERN = [r".*\.egg-info.*", r".*__cache__.*", r".*\.git.*"]

# The License header, "{year}" will be replaced by current year:
LICENSE_HEADER = """Copyright {year} {author}

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License."""

AUTHOR = """Universität Tübingen, DKFZ and EMBL
for the German Human Genome-Phenome Archive (GHGA)"""

# The path to the License file relative to target dir
LICENCE_FILE = "LICENSE"


def get_target_files(  # pylint: disable=dangerous-default-value
    target_dir: Path,
    exclude: List[str] = EXCLUDE,
    exclude_endings: List[str] = EXCLUDE_ENDINGS,
    exclude_pattern: List[str] = EXCLUDE_PATTERN,
) -> List[Path]:
    """Get target files that are not match the exclude conditions.
    Args:
        target_dir (pathlib.Path): The target dir to search.
        exclude (List[str], optional):
            Overwrite default list of file/dir paths relative to
            the target dir that shall be excluded.
        exclude_endings (List[str], optional):
            Overwrite default list of file endings that shall
            be excluded.
        exclude_pattern (List[str], optional):
            Overwrite default list of regex patterns match file path
            for exclusion.
    """
    abs_target_dir = Path(target_dir).absolute()
    exclude_normalized = [(abs_target_dir / excl).absolute() for excl in exclude]

    # get all files:
    all_files = [
        file_.absolute() for file_ in Path(abs_target_dir).rglob("*") if file_.is_file()
    ]

    target_files = [
        file_
        for file_ in all_files
        if not (
            any([file_.is_relative_to(excl) for excl in exclude_normalized])
            or any([str(file_).endswith(ending) for ending in exclude_endings])
            or any([re.match(pattern, str(file_)) for pattern in exclude_pattern])
        )
    ]
    return target_files


def normalized_text(text: str) -> str:
    "Normalize a license header text."
    return "\n".join(
        [
            line.strip("#").strip()
            for line in text.split("\n")
            if not (  # exclude shebang and empty lines
                line.startswith("#!") or line.strip("#").strip() == ""
            )
        ]
    ).strip("\n")


def format_license_header_template(license_header_template: str, author: str) -> str:
    """Formats license header by inserting the current year and the
    specified author for every occurence of "{year}" and "{author}",
    respectively, in the header template.
    """
    current_year = str(date.today().year)
    return normalized_text(
        license_header_template.replace("{year}", current_year).replace(
            "{author}", author
        )
    )


def check_file_headers(  # pylint: disable=dangerous-default-value
    target_dir: Path,
    license_header: str = LICENSE_HEADER,
    author: str = AUTHOR,
    exclude: List[str] = EXCLUDE,
    exclude_endings: List[str] = EXCLUDE_ENDINGS,
    exclude_pattern: List[str] = EXCLUDE_PATTERN,
) -> Tuple[List[Path], List[Path]]:
    """Check files for presence of a license header and verify that
    the copyright notice is up to date (correct year).

    Args:
        target_dir (pathlib.Path): The target dir to search.
        license_header (str, optional):
            A string of the license header. You may include
            "{year}" which will be replace by the current year.
            This defaults to the Apache 2.0 Copyright notice.
        author (str, optional):
            The author that shall be included in the license header.
            It will replace any appearance of "{author}" in the license
            header. This defaults to an auther info for GHGA.
        exclude (List[str], optional):
            Overwrite default list of file/dir paths relative to
            the target dir that shall be excluded.
        exclude_endings (List[str], optional):
            Overwrite default list of file endings that shall
            be excluded.
        exclude_pattern (List[str], optional):
            Overwrite default list of regex patterns match file path
            for exclusion.
    """
    target_files = get_target_files(
        target_dir,
        exclude=exclude,
        exclude_endings=exclude_endings,
        exclude_pattern=exclude_pattern,
    )

    # insert current year and author into license header:
    license_header_formatted = format_license_header_template(license_header, author)

    # check if license header present in file:
    passed_files: List[Path] = []
    failed_files: List[Path] = []

    n_header_lines = len(license_header_formatted.split("\n"))

    for target_file in target_files:
        # read in file
        with open(target_dir / target_file, "r") as file_:
            file_content = normalized_text(file_.read())
        # check whether file has enough lines
        file_lines = file_content.split("\n")
        n_file_lines = len(file_lines)
        if n_file_lines < n_header_lines:
            failed_files.append(target_file)
            continue
        # check whether first lines match the header:
        header_expected = "\n".join(file_lines[0:n_header_lines])
        if header_expected == license_header_formatted:
            passed_files.append(target_file)
        else:
            failed_files.append(target_file)

    return (passed_files, failed_files)


def check_license_file(
    license_file: Path,
    copyright_notice: str = LICENSE_HEADER,
    author: str = AUTHOR,
) -> bool:
    """Currently only checks if the copyright notice in the
    License file is up to data.

    Args:
        license_file (pathlib.Path, optional): Overwrite the default license file.
        copyright_notice (str, optional):
            A string of the copyright notice (usually same as license header).
            You may include "{year}" which will be replace by the current year.
            This defaults to the Apache 2.0 Copyright notice.
        author (str, optional):
            The author that shall be included in the copyright notice.
            It will replace any appearance of "{author}" in the copyright
            notice. This defaults to an auther info for GHGA.
    """

    if not license_file.is_file():
        print(f'Could not find license file "{str(license_file)}".')
        return False

    with open(license_file, "r") as file_:
        license_text = normalized_text(file_.read())

    expected_copyright = format_license_header_template(copyright_notice, author)

    return expected_copyright in license_text


def run():
    """Run checks from CLI."""
    parser = argparse.ArgumentParser(
        prog="license-checker",
        description=(
            "This script checks that the license and license headers "
            + "exists and that they are up to date."
        ),
    )

    parser.add_argument(
        "-L",
        "--no-license-file-check",
        help="Disables the check of the license file",
        action="store_true",
    )

    parser.add_argument(
        "-t",
        "--target-dir",
        help="Specify a custom target dir. Overwrites the default package root.",
    )

    args = parser.parse_args()

    target_dir = Path(args.target_dir).absolute() if args.target_dir else ROOT_DIR

    print(f'Working in "{target_dir}"\n')

    print("Checking license headers in files:")
    passed_files, failed_files = check_file_headers(target_dir)
    print(f"{len(passed_files)} files passed.")
    print(f"{len(failed_files)} files failed" + (":" if failed_files else "."))
    for failed_file in failed_files:
        print(f'  - "{failed_file.relative_to(target_dir)}"')

    print("")

    if args.no_license_file_check:
        license_file_valid = True
    else:
        license_file = Path(target_dir / LICENCE_FILE)
        print(f'Checking if LICENSE file is up to date: "{license_file}"')
        license_file_valid = check_license_file(license_file)
        print(
            "Copyright notice in license file is "
            + ("" if license_file_valid else "not ")
            + "up to date.\n"
        )

    if failed_files or not license_file_valid:
        print("Some checks failed.")
        sys.exit(1)

    print("All checks passed.")
    sys.exit(0)


if __name__ == "__main__":
    run()
