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
Test S3 storage DAO
"""

from pathlib import Path

import pytest
from testcontainers.localstack import LocalStackContainer

from ghga_service_chassis_lib.object_storage_dao import (
    BucketAlreadyExists,
    BucketNotFoundError,
    ObjectAlreadyExistsError,
    ObjectNotFoundError,
    ObjectStorageDao,
)
from ghga_service_chassis_lib.object_storage_dao_testing import (
    download_and_check_test_file,
    populate_storage,
    upload_file,
)
from ghga_service_chassis_lib.s3 import ObjectStorageS3
from ghga_service_chassis_lib.s3_testing import config_from_localstack_container

from .fixtures.storage import OBJECT_FIXTURE


# This workflow is defined as a seperate function so that it can also be used
# outside of the `tests` package:
def typical_workflow(
    storage_client: ObjectStorageDao,
    bucket1_id: str = "mytestbucket1",
    bucket2_id: str = "mytestbucket2",
    object_id: str = "mytestfile",
    test_file_path: Path = OBJECT_FIXTURE.file_path,
    test_file_md5: str = OBJECT_FIXTURE.md5,
):
    """
    Run a typical workflow of basic object operations using a S3 service.
    """
    print("Run a workflow for testing basic object operations using a S3 service:")

    print(f" - create new bucket {bucket1_id}")
    storage_client.create_bucket(bucket1_id)

    print(" - confirm bucket creation")
    assert storage_client.does_bucket_exist(bucket1_id)

    print(f" - upload test object {object_id} to bucket")
    upload_url = storage_client.get_object_upload_url(
        bucket_id=bucket1_id, object_id=object_id
    )
    upload_file(
        presigned_url=upload_url, file_path=test_file_path, file_md5=test_file_md5
    )

    print(" - confirm object upload")
    assert storage_client.does_object_exist(bucket_id=bucket1_id, object_id=object_id)

    print(" - download and check object")
    download_url1 = storage_client.get_object_download_url(
        bucket_id=bucket1_id, object_id=object_id
    )
    download_and_check_test_file(
        presigned_url=download_url1, expected_md5=test_file_md5
    )

    print(f" - create a second bucket {bucket2_id} and move the object there")
    storage_client.create_bucket(bucket2_id)
    storage_client.copy_object(
        source_bucket_id=bucket1_id,
        source_object_id=object_id,
        dest_bucket_id=bucket2_id,
        dest_object_id=object_id,
    )
    storage_client.delete_object(bucket_id=bucket1_id, object_id=object_id)

    print(" - confirm move")
    assert not storage_client.does_object_exist(
        bucket_id=bucket1_id, object_id=object_id
    )
    assert storage_client.does_object_exist(bucket_id=bucket2_id, object_id=object_id)

    print(f" - delete bucket {bucket1_id}")
    storage_client.delete_bucket(bucket1_id)

    print(" - confirm bucket deletion")
    assert not storage_client.does_bucket_exist(bucket1_id)

    print(f" - download object from bucket {bucket2_id}")
    download_url2 = storage_client.get_object_download_url(
        bucket_id=bucket2_id, object_id=object_id
    )
    download_and_check_test_file(
        presigned_url=download_url2, expected_md5=test_file_md5
    )

    print("Done.")


def test_typical_workflow():
    """
    Tests all methods of the ObjectStorageS3 DAO implementation in one long workflow.
    """
    with LocalStackContainer().with_services("s3") as localstack:
        config = config_from_localstack_container(localstack)

        with ObjectStorageS3(config=config) as storage:
            typical_workflow(storage)


def test_object_and_bucket_collisions():
    """
    Tests whether overwriting (re-creation, re-upload, or copy to exisitng object) fails with the expected error.
    """

    with LocalStackContainer().with_services("s3") as localstack:
        existing_bucket_id = OBJECT_FIXTURE.bucket_id
        existing_object_id = OBJECT_FIXTURE.object_id
        config = config_from_localstack_container(localstack)

        with ObjectStorageS3(config=config) as storage:
            populate_storage(
                storage=storage,
                fixtures=[OBJECT_FIXTURE],
            )

            with pytest.raises(BucketAlreadyExists):
                storage.create_bucket(existing_bucket_id)

            with pytest.raises(ObjectAlreadyExistsError):
                storage.get_object_upload_url(
                    bucket_id=existing_bucket_id, object_id=existing_object_id
                )

            with pytest.raises(ObjectAlreadyExistsError):
                storage.copy_object(
                    source_bucket_id=existing_bucket_id,
                    source_object_id=existing_object_id,
                    dest_bucket_id=existing_bucket_id,
                    dest_object_id=existing_object_id,
                )


def test_handling_non_existing_file_and_bucket():
    """
    Tests whether the re-creaction of an existing bucket fails with the expected error.
    """
    with LocalStackContainer().with_services("s3") as localstack:
        non_exisiting_bucket_id = "mynonexistingbucket"
        non_existing_object_id = "mynonexistingobject"
        existing_bucket_id = OBJECT_FIXTURE.bucket_id
        existing_object_id = OBJECT_FIXTURE.object_id
        config = config_from_localstack_container(localstack)

        with ObjectStorageS3(config=config) as storage:
            populate_storage(
                storage=storage,
                fixtures=[OBJECT_FIXTURE],
            )

            with pytest.raises(BucketNotFoundError):
                storage.delete_bucket(non_exisiting_bucket_id)

            with pytest.raises(BucketNotFoundError):
                storage.get_object_download_url(
                    bucket_id=non_exisiting_bucket_id, object_id=non_existing_object_id
                )

            with pytest.raises(BucketNotFoundError):
                storage.get_object_upload_url(
                    bucket_id=non_exisiting_bucket_id, object_id=non_existing_object_id
                )

            with pytest.raises(BucketNotFoundError):
                storage.delete_object(
                    bucket_id=non_exisiting_bucket_id, object_id=non_existing_object_id
                )

            with pytest.raises(BucketNotFoundError):
                storage.copy_object(
                    source_bucket_id=non_exisiting_bucket_id,
                    source_object_id=non_existing_object_id,
                    dest_bucket_id=existing_bucket_id,
                    dest_object_id=non_existing_object_id,
                )

            with pytest.raises(BucketNotFoundError):
                storage.copy_object(
                    source_bucket_id=existing_bucket_id,
                    source_object_id=existing_object_id,
                    dest_bucket_id=non_exisiting_bucket_id,
                    dest_object_id=non_existing_object_id,
                )

            with pytest.raises(ObjectNotFoundError):
                storage.get_object_download_url(
                    bucket_id=existing_bucket_id, object_id=non_existing_object_id
                )

            with pytest.raises(ObjectNotFoundError):
                storage.delete_object(
                    bucket_id=existing_bucket_id, object_id=non_existing_object_id
                )
