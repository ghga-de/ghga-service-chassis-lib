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
from typing import Generator, List, Optional

import pytest
from testcontainers.localstack import LocalStackContainer

from .object_storage_dao import ObjectStorageDao
from .object_storage_dao_testing import (
    DEFAULT_EXISTING_BUCKETS,
    DEFAULT_EXISTING_OBJECTS,
    DEFAULT_NON_EXISTING_BUCKETS,
    DEFAULT_NON_EXISTING_OBJECTS,
    ObjectFixture,
    populate_storage,
)
from .s3 import ObjectStorageS3, S3ConfigBase


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
    storage: ObjectStorageDao
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
        with LocalStackContainer().with_services("s3") as localstack:
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
