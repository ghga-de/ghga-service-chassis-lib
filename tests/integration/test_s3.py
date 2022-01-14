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
Test S3 storage DAO
"""

from pathlib import Path

import pytest

from ghga_service_chassis_lib.object_storage_dao import (
    BucketAlreadyExists,
    BucketNotFoundError,
    ObjectAlreadyExistsError,
    ObjectNotFoundError,
    ObjectStorageDao,
)
from ghga_service_chassis_lib.object_storage_dao_testing import (
    DEFAULT_NON_EXISTING_OBJECTS,
    download_and_check_test_file,
    upload_file,
)

from .fixtures.s3 import s3_fixture  # noqa: F401


# This workflow is defined as a seperate function so that it can also be used
# outside of the `tests` package:
def typical_workflow(
    storage_client: ObjectStorageDao,
    bucket1_id: str = "mytestbucket1",
    bucket2_id: str = "mytestbucket2",
    object_id: str = DEFAULT_NON_EXISTING_OBJECTS[0].object_id,
    test_file_path: Path = DEFAULT_NON_EXISTING_OBJECTS[0].file_path,
    test_file_md5: str = DEFAULT_NON_EXISTING_OBJECTS[0].md5,
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


def test_typical_workflow(s3_fixture):  # noqa: F811
    """
    Tests all methods of the ObjectStorageS3 DAO implementation in one long workflow.
    """
    object_fixture = s3_fixture.non_existing_objects[0]
    typical_workflow(
        storage_client=s3_fixture.storage,
        bucket1_id=s3_fixture.non_existing_buckets[0],
        bucket2_id=s3_fixture.non_existing_buckets[1],
        object_id=object_fixture.object_id,
        test_file_md5=object_fixture.md5,
        test_file_path=object_fixture.file_path,
    )


def test_object_and_bucket_collisions(s3_fixture):  # noqa: F811
    """
    Tests whether overwriting (re-creation, re-upload, or copy to exisitng object) fails with the expected error.
    """
    existing_object = s3_fixture.existing_objects[0]

    with pytest.raises(BucketAlreadyExists):
        s3_fixture.storage.create_bucket(existing_object.bucket_id)

    with pytest.raises(ObjectAlreadyExistsError):
        s3_fixture.storage.get_object_upload_url(
            bucket_id=existing_object.bucket_id, object_id=existing_object.object_id
        )

    with pytest.raises(ObjectAlreadyExistsError):
        s3_fixture.storage.copy_object(
            source_bucket_id=existing_object.bucket_id,
            source_object_id=existing_object.object_id,
            dest_bucket_id=existing_object.bucket_id,
            dest_object_id=existing_object.object_id,
        )


def test_handling_non_existing_file_and_bucket(s3_fixture):  # noqa: F811
    """
    Tests whether the re-creaction of an existing bucket fails with the expected error.
    """
    existing_bucket = s3_fixture.existing_buckets[-1]
    existing_object = s3_fixture.existing_objects[0]
    existing_object_id = s3_fixture.existing_objects[0].object_id
    non_existing_object = s3_fixture.non_existing_objects[0]

    with pytest.raises(BucketNotFoundError):
        s3_fixture.storage.delete_bucket(non_existing_object.bucket_id)

    with pytest.raises(BucketNotFoundError):
        s3_fixture.storage.get_object_download_url(
            bucket_id=non_existing_object.bucket_id,
            object_id=non_existing_object.object_id,
        )

    with pytest.raises(BucketNotFoundError):
        s3_fixture.storage.get_object_upload_url(
            bucket_id=non_existing_object.bucket_id,
            object_id=non_existing_object.object_id,
        )

    with pytest.raises(BucketNotFoundError):
        s3_fixture.storage.delete_object(
            bucket_id=non_existing_object.bucket_id,
            object_id=non_existing_object.object_id,
        )

    with pytest.raises(BucketNotFoundError):
        s3_fixture.storage.copy_object(
            source_bucket_id=non_existing_object.bucket_id,
            source_object_id=non_existing_object.object_id,
            dest_bucket_id=existing_bucket,
            dest_object_id=non_existing_object.object_id,
        )

    with pytest.raises(BucketNotFoundError):
        s3_fixture.storage.copy_object(
            source_bucket_id=existing_object.bucket_id,
            source_object_id=existing_object_id,
            dest_bucket_id=non_existing_object.bucket_id,
            dest_object_id=non_existing_object.object_id,
        )

    with pytest.raises(ObjectNotFoundError):
        s3_fixture.storage.get_object_download_url(
            bucket_id=existing_object.bucket_id, object_id=non_existing_object.object_id
        )

    with pytest.raises(ObjectNotFoundError):
        s3_fixture.storage.delete_object(
            bucket_id=existing_object.bucket_id, object_id=non_existing_object.object_id
        )
