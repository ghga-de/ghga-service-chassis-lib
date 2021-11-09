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

import pytest
from testcontainers.localstack import LocalStackContainer

from ghga_service_chassis_lib.object_storage_dao import (
    BucketAlreadyExists,
    BucketNotFoundError,
    ObjectAlreadyExistsError,
    ObjectNotFoundError,
)
from ghga_service_chassis_lib.s3 import ObjectStorageS3

from .fixtures.s3 import (
    TEST_CREDENTIALS,
    create_existing_object_and_bucket,
    download_and_check_test_file,
    upload_test_file,
)


def test_typical_workflow():
    """
    Tests all methods of the ObjectStorageS3 DAO implementation in one long workflow.
    """
    bucket1_id = "mytestbucket1"
    bucket2_id = "mytestbucket2"
    object_id = "mytestfile"

    with LocalStackContainer().with_services("s3") as localstack:
        with ObjectStorageS3(
            endpoint_url=localstack.get_url(), credentials=TEST_CREDENTIALS
        ) as storage:
            # confirm that no bucket with the specified id can be found:
            assert not storage.does_bucket_exist(bucket1_id)

            # create a new bucket:
            storage.create_bucket(bucket1_id)

            # confirm that bucket can be found:
            assert storage.does_bucket_exist(bucket1_id)

            # upload a test file to that bucket
            upload_url = storage.get_object_upload_url(
                bucket_id=bucket1_id, object_id=object_id
            )
            upload_test_file(upload_url)

            # confirm that file can be found:
            assert storage.does_object_exist(bucket_id=bucket1_id, object_id=object_id)

            # download the file from the first bucket:
            download_url1 = storage.get_object_download_url(
                bucket_id=bucket1_id, object_id=object_id
            )
            download_and_check_test_file(download_url1)

            # create a second bucket and move (copy & delete) the file there:
            storage.create_bucket(bucket2_id)
            storage.copy_object(
                source_bucket_id=bucket1_id,
                source_object_id=object_id,
                dest_bucket_id=bucket2_id,
                dest_object_id=object_id,
            )
            storage.delete_object(bucket_id=bucket1_id, object_id=object_id)

            # confirm that file is not longer found in bucket1 but in bucket 2:
            assert not storage.does_object_exist(
                bucket_id=bucket1_id, object_id=object_id
            )
            assert storage.does_object_exist(bucket_id=bucket2_id, object_id=object_id)

            # delete bucket 1:
            storage.delete_bucket(bucket1_id)

            # download the file from the second bucket:
            download_url2 = storage.get_object_download_url(
                bucket_id=bucket2_id, object_id=object_id
            )
            download_and_check_test_file(download_url2)


def test_object_and_bucket_collisions():
    """
    Tests whether overwriting (re-creation, re-upload, or copy to exisitng object) fails with the expected error.
    """

    with LocalStackContainer().with_services("s3") as localstack:
        with ObjectStorageS3(
            endpoint_url=localstack.get_url(), credentials=TEST_CREDENTIALS
        ) as storage:
            existing_bucket_id, existing_object_id = create_existing_object_and_bucket(
                storage
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
    non_exisiting_bucket_id = "mynonexistingbucket"
    non_existing_object_id = "mynonexistingobject"

    with LocalStackContainer().with_services("s3") as localstack:
        with ObjectStorageS3(
            endpoint_url=localstack.get_url(), credentials=TEST_CREDENTIALS
        ) as storage:
            existing_bucket_id, existing_object_id = create_existing_object_and_bucket(
                storage
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
