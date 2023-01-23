#!/usr/bin/env python3

# Copyright 2021 - 2023 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
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
Runs a worflow for testing basic objects operations using a S3 service.

Help on using this script:
`./s3_workflow_check.py --help`
"""

from typing import List

import typer

from ghga_service_chassis_lib.object_storage_dao import ObjectStorageDao
from ghga_service_chassis_lib.s3 import ObjectStorageS3, S3ConfigBase
from ghga_service_chassis_lib.s3_testing import typical_workflow


def cleanup_buckets_and_objects(
    storage_client: ObjectStorageDao,
    bucket_ids: List[str],
    object_id: str,
):
    """Cleanup buckets and objects."""

    print("Cleanup buckets and objects.")

    for bucket_id in bucket_ids:
        if storage_client.does_object_exist(bucket_id=bucket_id, object_id=object_id):
            storage_client.delete_object(bucket_id=bucket_id, object_id=object_id)
        if storage_client.does_bucket_exist(bucket_id):
            storage_client.delete_bucket(bucket_id)


def test_workflow(  # pylint: disable=too-many-arguments
    s3_endpoint_url: str,
    s3_access_key_id: str,
    s3_secret_access_key: str,
    bucket1_id: str = "mytestbucket1",
    bucket2_id: str = "mytestbucket2",
    object_id: str = "mytestfile",
):
    """Run a workflow for testing basic object operations using a S3 service."""

    config = S3ConfigBase(
        s3_endpoint_url=s3_endpoint_url,
        s3_access_key_id=s3_access_key_id,
        s3_secret_access_key=s3_secret_access_key,
    )

    with ObjectStorageS3(config) as storage:
        cleanup_buckets_and_objects(
            storage_client=storage,
            bucket_ids=[bucket1_id, bucket2_id],
            object_id=object_id,
        )
        typical_workflow(
            storage_client=storage,
            bucket1_id=bucket1_id,
            bucket2_id=bucket2_id,
            object_id=object_id,
        )
        cleanup_buckets_and_objects(
            storage_client=storage,
            bucket_ids=[bucket1_id, bucket2_id],
            object_id=object_id,
        )


if __name__ == "__main__":
    typer.run(test_workflow)
