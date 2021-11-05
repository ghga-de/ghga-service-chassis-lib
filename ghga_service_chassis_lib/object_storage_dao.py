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
This modules contains a DAO base class for interacting
with file objects along with some specific implementations
of that DAO.
"""

from typing import Tuple


class BucketError(RuntimeError):
    """Generic base exceptions for error that occur while handling buckets."""

    pass  # pylint: disable=unnecessary-pass


class BucketNotFound(BucketError):
    """Thrown when trying to access a bucket with an ID that doesn't exist."""

    pass  # pylint: disable=unnecessary-pass


class BucketIdAlreadyInUse(BucketError):
    """Thrown when trying to create a bucket with an ID that already exists."""

    pass  # pylint: disable=unnecessary-pass


class FileObjectError(RuntimeError):
    """Generic base exceptions for error that occur while handling file objects."""

    pass  # pylint: disable=unnecessary-pass


class FileObjectNotFound(FileObjectError):
    """Thrown when trying to access a bucket with an ID that doesn't exist."""

    pass  # pylint: disable=unnecessary-pass


class FileObjectAlreadyInUse(FileObjectError):
    """Thrown when trying to access a file with an ID that doesn't exist."""

    pass  # pylint: disable=unnecessary-pass


class ObjectStorageDAO:
    """
    A DAO base class for interacting with file objects.
    Exceptions may include:
        - NotImplementedError (if a DAO implementation chooses to not implement a
          specific method)
        - BucketError, or derived exceptions:
            - BucketNotFound
            - BucketIdAlreadyInUse
        - FileObjectError, or derived exceptions:
            - FileObjectNotFound
            - FileObjectAlreadyInUse
    """

    # Connection/session teardown and setup should
    # be handled using pythons context manager protocol
    # by implementing `__enter__` and `__exit__`
    # methods:

    def __enter__(self):
        """Setup storage connection/session."""
        ...

    def __exit__(self, err_type, err_value, err_traceback):
        """Teardown storage connection/session"""
        ...

    # Please implement following methods for interacting
    # with file objects and buckets:

    def create_bucket(self, bucket_id: str):
        """
        Create a bucket (= a structure that can hold multiple file objects) with the
        specified unique ID.
        """
        raise NotImplementedError()

    def delete_bucket(self, bucket_id: str):
        """
        Delete a bucket (= a structure that can hold multiple file objects) with the
        specified unique ID.
        """
        raise NotImplementedError()

    def get_object_upload_url(
        self, bucket_id: str, object_id: str, expires_after: int = 86400
    ) -> str:
        """Generates and returns an HTTP URL to upload a new file object with the given
        id (`object_id`) to the bucket with the specified id (`bucket_id`).
        You may also specify a custom expiry duration in seconds (`expires_after`).
        """
        raise NotImplementedError()

    def get_object_download_url(
        self, bucket_id: str, object_id: str, expires_after: int = 86400
    ) -> Tuple[str, str]:
        """Generates and returns a presigns HTTP-URL to download a file object with
        the specified ID (`object_id`) from bucket with the specified id (`bucket_id`).
        You may also specify a custom expiry duration in seconds (`expires_after`).
        """
        raise NotImplementedError()

    def delete_object_file(
        self, bucket_id: str, object_id: str, expires_after: int = 86400
    ) -> str:
        """Generates and returns an HTTP URL to upload a new file object with the given
        id (`object_id`) to the bucket with the specified id (`bucket_id`).
        You may also specify a custom expiry duration in seconds (`expires_after`).
        """
        raise NotImplementedError()
