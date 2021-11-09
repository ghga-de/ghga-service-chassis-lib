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

"""
Runs a worflow to test basic handling of objects using an S3 service.

Help on using this script:
`./s3_workflow_check.py --help`
"""

import hashlib
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import List

import requests
import typer

from ghga_service_chassis_lib.object_storage_dao import (
    ObjectStorageDao,
    PresignedPostURL,
)
from ghga_service_chassis_lib.s3 import ObjectStorageS3, S3Credentials


def calc_md5(file_path: Path) -> str:
    """
    Calc the md5 checksum for the specified file.
    """  # nosec
    with open(file_path, "rb") as file:
        file_content = file.read()
        return hashlib.md5(file_content).hexdigest()


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


HERE = Path(__file__).parent.resolve()
TEST_FILE_PATH = HERE.parent / "tests" / "integration" / "fixtures" / "test_file.txt"
TEST_FILE_MD5 = calc_md5(TEST_FILE_PATH)


def cleanup_buckets_and_objects(
    storage_client: ObjectStorageDao,
    bucket_ids: List[str],
    object_id: str,
):
    """Cleanup buckets and objects."""
    typer.echo(" - cleanup buckets and objects.")
    for bucket_id in bucket_ids:
        if storage_client.does_object_exist(bucket_id=bucket_id, object_id=object_id):
            storage_client.delete_object(bucket_id=bucket_id, object_id=object_id)
        if storage_client.does_bucket_exist(bucket_id):
            storage_client.delete_bucket(bucket_id)


def test_workflow(  # pylint: disable=too-many-arguments
    endpoint_url: str,
    aws_access_key_id: str,
    aws_secret_access_key: str,
    bucket1_id: str = "mytestbucket1",
    bucket2_id: str = "mytestbucket2",
    object_id: str = "mytestfile",
):
    """Run a workflow for testing basic object handling using an S3 service."""

    typer.echo("Run a workflow for testing basic object handling using an S3 service:")
    credentials = S3Credentials(
        aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key
    )

    with ObjectStorageS3(endpoint_url=endpoint_url, credentials=credentials) as storage:
        cleanup_buckets_and_objects(
            storage, bucket_ids=[bucket1_id, bucket2_id], object_id=object_id
        )

        typer.echo(f" - create new bucket {bucket1_id}")
        storage.create_bucket(bucket1_id)

        typer.echo(" - confirm bucket creation")
        assert storage.does_bucket_exist(bucket1_id)

        typer.echo(f" - upload test object {object_id} to bucket")
        upload_url = storage.get_object_upload_url(
            bucket_id=bucket1_id, object_id=object_id
        )
        upload_test_file(upload_url)

        typer.echo(" - confirm object upload")
        assert storage.does_object_exist(bucket_id=bucket1_id, object_id=object_id)

        typer.echo(" - download and check object")
        download_url1 = storage.get_object_download_url(
            bucket_id=bucket1_id, object_id=object_id
        )
        download_and_check_test_file(download_url1)

        typer.echo(f" - create a second bucket {bucket2_id} and move the object there")
        storage.create_bucket(bucket2_id)
        storage.copy_object(
            source_bucket_id=bucket1_id,
            source_object_id=object_id,
            dest_bucket_id=bucket2_id,
            dest_object_id=object_id,
        )
        storage.delete_object(bucket_id=bucket1_id, object_id=object_id)

        typer.echo(" - confirm move")
        assert not storage.does_object_exist(bucket_id=bucket1_id, object_id=object_id)
        assert storage.does_object_exist(bucket_id=bucket2_id, object_id=object_id)

        typer.echo(f" - delete bucket {bucket1_id}")
        storage.delete_bucket(bucket1_id)

        typer.echo(" - confirm bucket deletion")
        assert not storage.does_bucket_exist(bucket1_id)

        typer.echo(f" - download object from bucket {bucket2_id}")
        download_url2 = storage.get_object_download_url(
            bucket_id=bucket2_id, object_id=object_id
        )
        download_and_check_test_file(download_url2)

        cleanup_buckets_and_objects(
            storage, bucket_ids=[bucket1_id, bucket2_id], object_id=object_id
        )

        typer.echo("Done.")


if __name__ == "__main__":
    typer.run(test_workflow)
