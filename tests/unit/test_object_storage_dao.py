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

"""Tests for the `object_storage_dao` module."""

import pytest

from ghga_service_chassis_lib.object_storage_dao import (
    BucketIdValidationError,
    ObjectIdValidationError,
    validate_bucket_id,
    validate_object_id,
)

from .fixtures.object_storage_dao import too_long_id, too_short_id


def test_validate_bucket_id():
    """validate_bucket_id function: no error for valid id"""
    validate_bucket_id("ghgas-12239992232323422")


def test_validate_bucket_id_too_long():
    """validate_bucket_id function: expect error for too long id"""
    with pytest.raises(BucketIdValidationError):
        validate_bucket_id(too_long_id)


def test_validate_bucket_id_too_short():
    """validate_bucket_id function: expect error for too short id"""
    with pytest.raises(BucketIdValidationError):
        validate_bucket_id(too_short_id)


@pytest.mark.parametrize("non_allowed_char", ["_", ".", "/", "&", "+", ":"])
def test_validate_bucket_id_non_allowed_chars(non_allowed_char):
    """validate_bucket_id function: expect error for not allowed characters"""
    with pytest.raises(BucketIdValidationError):
        validate_bucket_id(f"a{non_allowed_char}a")


def test_validate_bucket_id_non_allowed_ends():
    """validate_bucket_id function: expect error for not allowed starting or tailing
    characters"""
    non_allowed_ids = ["-aa", "aa-"]
    for id_ in non_allowed_ids:
        with pytest.raises(BucketIdValidationError):
            validate_bucket_id(id_)


def test_validate_object_id():
    """validate_object_id function: no error for valid id"""
    validate_object_id("ghgaf-12239992232323422.test")


def test_validate_object_id_too_long():
    """validate_object_id function: expect error for too long id"""
    with pytest.raises(ObjectIdValidationError):
        validate_object_id(too_long_id)


def test_validate_object_id_too_short():
    """validate_object_id function: expect error for too short id"""
    with pytest.raises(ObjectIdValidationError):
        validate_object_id(too_short_id)


@pytest.mark.parametrize("non_allowed_char", ["_", "/", "&", "+", ":"])
def test_validate_object_id_non_allowed_chars(non_allowed_char: str):
    """validate_object_id function: expect error for not allowed characters"""
    with pytest.raises(ObjectIdValidationError):
        validate_object_id(f"a{non_allowed_char}a")


def test_validate_object_id_non_allowed_ends():
    """validate_object_id function: expect error for not allowed starting or tailing
    characters"""
    non_allowed_ids = ["-aa", "aa-", ".aa", "aa."]
    for id_ in non_allowed_ids:
        with pytest.raises(ObjectIdValidationError):
            validate_object_id(id_)
