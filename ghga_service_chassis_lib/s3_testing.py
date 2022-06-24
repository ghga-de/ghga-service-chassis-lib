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
from the `s3` module.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Generator, List, Optional

import pytest
from testcontainers.localstack import LocalStackContainer

from ghga_service_chassis_lib.object_storage_dao import (
    DEFAULT_PART_SIZE,
    ObjectStorageDao,
)
from ghga_service_chassis_lib.object_storage_dao_testing import (
    DEFAULT_EXISTING_BUCKETS,
    DEFAULT_EXISTING_OBJECTS,
    DEFAULT_NON_EXISTING_BUCKETS,
    DEFAULT_NON_EXISTING_OBJECTS,
    ObjectFixture,
    download_and_check_test_file,
    multipart_upload_file,
    populate_storage,
    upload_file,
    upload_part,
)
from ghga_service_chassis_lib.s3 import ObjectStorageS3, S3ConfigBase


def config_from_localstack_container(container: LocalStackContainer) -> S3ConfigBase:
    """Prepares a S3ConfigBase from an instance of a localstack test container."""
    s3_endpoint_url = container.get_url()
    return S3ConfigBase(  # nosec
        s3_endpoint_url=s3_endpoint_url,
        s3_access_key_id="test",
        s3_secret_access_key="test",
    )


@dataclass
class S3Fixture:
    """Info yielded by the `s3_fixture` function"""

    config: S3ConfigBase
    storage: ObjectStorageS3
    existing_buckets: List[str]
    non_existing_buckets: List[str]
    existing_objects: List[ObjectFixture]
    non_existing_objects: List[ObjectFixture]


def s3_fixture_factory(
    existing_buckets: Optional[List[str]] = None,
    non_existing_buckets: Optional[List[str]] = None,
    existing_objects: Optional[List[ObjectFixture]] = None,
    non_existing_objects: Optional[List[ObjectFixture]] = None,
):
    """A factory for generating a pre-configured Pytest fixture working with S3."""

    # list defaults:
    # (listting instances of primitive types such as lists as defaults in the function
    # header is dangerous)
    existing_buckets_ = (
        DEFAULT_EXISTING_BUCKETS if existing_buckets is None else existing_buckets
    )
    non_existing_buckets_ = (
        DEFAULT_NON_EXISTING_BUCKETS
        if non_existing_buckets is None
        else non_existing_buckets
    )
    existing_objects_ = (
        DEFAULT_EXISTING_OBJECTS if existing_objects is None else existing_objects
    )
    non_existing_objects_ = (
        DEFAULT_NON_EXISTING_OBJECTS
        if non_existing_objects is None
        else non_existing_objects
    )

    @pytest.fixture
    def s3_fixture() -> Generator[S3Fixture, None, None]:
        """Pytest fixture for tests depending on the ObjectStorageS3 DAO."""
        with LocalStackContainer(image="localstack/localstack:0.14.2").with_services(
            "s3"
        ) as localstack:
            config = config_from_localstack_container(localstack)

            with ObjectStorageS3(config=config) as storage:
                populate_storage(
                    storage=storage,
                    bucket_fixtures=existing_buckets_,
                    object_fixtures=existing_objects_,
                )

                assert not set(existing_buckets_) & set(  # nosec
                    non_existing_buckets_
                ), "The existing and non existing bucket lists may not overlap"

                yield S3Fixture(
                    config=config,
                    storage=storage,
                    existing_buckets=existing_buckets_,
                    non_existing_buckets=non_existing_buckets_,
                    existing_objects=existing_objects_,
                    non_existing_objects=non_existing_objects_,
                )

    return s3_fixture


# This workflow is defined as a seperate function so that it can also be used
# outside of the `tests` package:
# pylint: disable=too-many-arguments
def typical_workflow(
    storage_client: ObjectStorageDao,
    bucket1_id: str = "mytestbucket1",
    bucket2_id: str = "mytestbucket2",
    object_id: str = DEFAULT_NON_EXISTING_OBJECTS[0].object_id,
    test_file_path: Path = DEFAULT_NON_EXISTING_OBJECTS[0].file_path,
    test_file_md5: str = DEFAULT_NON_EXISTING_OBJECTS[0].md5,
    use_multipart_upload: bool = True,
    part_size: int = DEFAULT_PART_SIZE,
):
    """
    Run a typical workflow of basic object operations using a S3 service.
    """
    print("Run a workflow for testing basic object operations using a S3 service:")

    print(f" - create new bucket {bucket1_id}")
    storage_client.create_bucket(bucket1_id)

    print(" - confirm bucket creation")
    assert storage_client.does_bucket_exist(bucket1_id)  # nosec

    if use_multipart_upload:
        multipart_upload_file(
            storage_dao=storage_client,
            bucket_id=bucket1_id,
            object_id=object_id,
            file_path=test_file_path,
            part_size=part_size,
        )
    else:
        print(f" - upload test object {object_id} to bucket")
        upload_url = storage_client.get_object_upload_url(
            bucket_id=bucket1_id, object_id=object_id
        )
        upload_file(
            presigned_url=upload_url, file_path=test_file_path, file_md5=test_file_md5
        )

    print(" - confirm object upload")
    assert storage_client.does_object_exist(  # nosec
        bucket_id=bucket1_id, object_id=object_id
    )

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
    assert not storage_client.does_object_exist(  # nosec
        bucket_id=bucket1_id, object_id=object_id
    )
    assert storage_client.does_object_exist(  # nosec
        bucket_id=bucket2_id, object_id=object_id
    )

    print(f" - delete bucket {bucket1_id}")
    storage_client.delete_bucket(bucket1_id)

    print(" - confirm bucket deletion")
    assert not storage_client.does_bucket_exist(bucket1_id)  # nosec

    print(f" - download object from bucket {bucket2_id}")
    download_url2 = storage_client.get_object_download_url(
        bucket_id=bucket2_id, object_id=object_id
    )
    download_and_check_test_file(
        presigned_url=download_url2, expected_md5=test_file_md5
    )

    print("Done.")


def get_initialized_upload(s3_fixture: S3Fixture):
    """Initialize a new empty multipart upload."""

    bucket_id = s3_fixture.existing_buckets[0]
    object_id = s3_fixture.non_existing_objects[0].object_id
    upload_id = s3_fixture.storage.init_multipart_upload(
        bucket_id=bucket_id, object_id=object_id
    )

    return upload_id, bucket_id, object_id


def prepare_non_completed_upload(s3_fixture: S3Fixture):
    """Prepare an upload that has not been marked as completed, yet."""

    upload_id, bucket_id, object_id = get_initialized_upload(s3_fixture)

    object_fixture = s3_fixture.non_existing_objects[0]

    upload_part(
        storage_dao=s3_fixture.storage,
        upload_id=upload_id,
        bucket_id=bucket_id,
        object_id=object_id,
        content=object_fixture.content,
    )

    return upload_id, bucket_id, object_id
