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

"""Fixtures for testing the S3 DAO"""

from pathlib import Path
from tempfile import TemporaryDirectory

import requests

from ghga_service_chassis_lib.object_storage_dao import PresignedPostURL
from ghga_service_chassis_lib.s3 import S3Credentials

from . import BASE_DIR
from .utils import calc_md5

TEST_CREDENTIALS = S3Credentials(aws_access_key_id="test", aws_secret_access_key="test")

TEST_FILE_PATH = BASE_DIR / "test_file.txt"
TEST_FILE_MD5 = calc_md5(TEST_FILE_PATH)


def upload_test_file(presigned_url: PresignedPostURL):
    """Uploads the test file to the specified URL"""
    with open(TEST_FILE_PATH, "rb") as test_file:
        files = {"file": (str(TEST_FILE_PATH), test_file)}
        response = requests.post(
            presigned_url.url, data=presigned_url.fields, files=files
        )
        response.raise_for_status()


def download_and_check_test_file(presigned_url: str):
    """Downloads the test file from thespecified URL and checks its integrity (md5)."""

    response = requests.get(presigned_url)
    response.raise_for_status()

    with TemporaryDirectory() as temp_dir:
        temp_file_path = Path(temp_dir) / "downloaded_file"

        with open(temp_file_path, "wb") as temp_file:
            temp_file.write(response.content)

        assert (
            calc_md5(temp_file_path) == TEST_FILE_MD5
        ), "downloaded file has unexpected md5 checksum"
