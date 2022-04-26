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

"""
This module contains utilities for testing code created with the functionality
from the `object_storage_dao` module.
"""

import hashlib
import math
import os
from pathlib import Path
from typing import List

import requests
from pydantic import BaseModel, validator

from .object_storage_dao import ObjectStorageDao, PresignedPostURL
from .utils import TEST_FILE_PATHS


def calc_md5(content: bytes) -> str:
    """
    Calc the md5 checksum for the specified bytes.
    """
    return hashlib.md5(content).hexdigest()  # nosec


class ObjectFixture(BaseModel):
    """A Model for describing fixtures for the object storage."""

    file_path: Path
    bucket_id: str
    object_id: str
    content: bytes = b"will be overwritten"
    md5: str = "will be overwritten"

    # pylint: disable=no-self-argument,no-self-use
    @validator("content", always=True)
    def read_content(cls, _, values):
        """Read in the file content."""
        with open(values["file_path"], "rb") as file:
            return file.read()

    # pylint: disable=no-self-argument,no-self-use
    @validator("md5", always=True)
    def calc_md5_from_content(cls, _, values):
        """Calculate md5 based on the content."""
        return calc_md5(values["content"])


def upload_file(presigned_url: PresignedPostURL, file_path: Path, file_md5: str):
    """Uploads the test file to the specified URL"""
    with open(file_path, "rb") as test_file:
        files = {"file": (str(file_path), test_file)}
        headers = {"ContentMD5": file_md5}
        response = requests.post(
            presigned_url.url, data=presigned_url.fields, files=files, headers=headers
        )
        response.raise_for_status()


def calc_part_size(file_path: Path, n_parts: int) -> int:
    """Calculate the part size in bytes for the given file when desiring the specified number of
    parts."""
    file_size = os.path.getsize(file_path)
    return math.ceil(file_size / n_parts)


def multipart_upload_file(
    storage_dao: ObjectStorageDao,
    bucket_id: str,
    object_id: str,
    file_path: Path,
    n_parts: int,
) -> None:
    """Uploads the test file to the specified URL"""
    print(f" - initiate multipart upload for test object {object_id}")
    upload_id = storage_dao.init_mulitpart_upload(
        bucket_id=bucket_id, object_id=object_id
    )

    part_size = calc_part_size(file_path=file_path, n_parts=n_parts)
    parts_tag_mapping: dict[int, str] = {}
    with open(file_path, "rb") as test_file:
        for part_number in range(0, n_parts):
            print(f" - get upload url for part number: {part_number}")
            upload_url = storage_dao.get_part_upload_url(
                upload_id=upload_id,
                bucket_id=bucket_id,
                object_id=object_id,
                part_number=part_number,
            )
            print(f" - read {part_size} from file: {str(file_path)}")
            file_part = test_file.read(part_size)
            print(f" - upload part number {part_number} using upload url")
            response = requests.put(upload_url, data=file_part)
            response.raise_for_status()
            parts_tag_mapping[part_number] = response.headers["ETag"]

    print(" - complete mulitpart upload")
    storage_dao.complete_mulitpart_upload(
        upload_id=upload_id,
        bucket_id=bucket_id,
        object_id=object_id,
        part_tag_mapping=parts_tag_mapping,
    )


def download_and_check_test_file(presigned_url: str, expected_md5: str):
    """Downloads the test file from thespecified URL and checks its integrity (md5)."""

    response = requests.get(presigned_url)
    response.raise_for_status()

    observed_md5 = calc_md5(response.content)

    assert (  # nosec
        observed_md5 == expected_md5
    ), "downloaded file has unexpected md5 checksum"


DEFAULT_EXISTING_BUCKETS = [
    "myexistingtestbucket100",
    "myexistingtestbucket200",
]
DEFAULT_NON_EXISTING_BUCKETS = [
    "mynonexistingtestobject100",
    "mynonexistingtestobject200",
]

DEFAULT_EXISTING_OBJECTS = [
    ObjectFixture(
        file_path=file_path,
        bucket_id=f"myexistingtestbucket{idx}",
        object_id=f"myexistingtestobject{idx}",
    )
    for idx, file_path in enumerate(TEST_FILE_PATHS[0:2])
]

DEFAULT_NON_EXISTING_OBJECTS = [
    ObjectFixture(
        file_path=file_path,
        bucket_id=f"mynonexistingtestbucket{idx}",
        object_id=f"mynonexistingtestobject{idx}",
    )
    for idx, file_path in enumerate(TEST_FILE_PATHS[2:4])
]


def populate_storage(
    storage: ObjectStorageDao,
    bucket_fixtures: List[str],
    object_fixtures: List[ObjectFixture],
):
    """Populate Storage with object and bucket fixtures"""

    for bucket_fixture in bucket_fixtures:
        storage.create_bucket(bucket_fixture)

    for object_fixture in object_fixtures:
        if not storage.does_bucket_exist(object_fixture.bucket_id):
            storage.create_bucket(object_fixture.bucket_id)

        presigned_url = storage.get_object_upload_url(
            bucket_id=object_fixture.bucket_id, object_id=object_fixture.object_id
        )

        upload_file(
            presigned_url=presigned_url,
            file_path=object_fixture.file_path,
            file_md5=object_fixture.md5,
        )
