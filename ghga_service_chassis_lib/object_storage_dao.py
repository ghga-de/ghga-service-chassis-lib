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
This modules contains a DAO base class for interacting
with file objects along with some specific implementations
of that DAO.
"""

import re
from dataclasses import dataclass
from typing import Optional

from .utils import DaoGenericBase

# constants for multipart upload
DEFAULT_PART_SIZE = 16 * 1024 * 1024
MAX_FILE_PART_NUMBER = 10000


class ObjectStorageDaoError(RuntimeError):
    """Generic base exceptions for all error related to the DAO base class."""


class BucketError(ObjectStorageDaoError):
    """Generic base exceptions for error that occur while handling buckets."""

    pass  # pylint: disable=unnecessary-pass


class BucketNotFoundError(BucketError):
    """Thrown when trying to access a bucket with an ID that doesn't exist."""

    def __init__(self, bucket_id: Optional[str]):
        message = (
            "The bucket "
            + (f"with ID '{bucket_id}' " if bucket_id else "")
            + "does not exist."
        )
        super().__init__(message)


class BucketAlreadyExists(BucketError):
    """Thrown when trying to create a bucket with an ID that already exists."""

    def __init__(self, bucket_id: Optional[str]):
        message = (
            "The bucket "
            + (f"with ID '{bucket_id}' " if bucket_id else "")
            + "already exist."
        )
        super().__init__(message)


class ObjectError(ObjectStorageDaoError):
    """Generic base exceptions for error that occur while handling file objects."""

    pass  # pylint: disable=unnecessary-pass


class ObjectNotFoundError(ObjectError):
    """Thrown when trying to access a bucket with an ID that doesn't exist."""

    def __init__(
        self, bucket_id: Optional[str] = None, object_id: Optional[str] = None
    ):
        message = (
            "The object "
            + (f"with ID '{object_id}' " if object_id else "")
            + (f"in bucket with ID '{bucket_id}' " if bucket_id else "")
            + "does not exist."
        )
        super().__init__(message)


class ObjectAlreadyExistsError(ObjectError):
    """Thrown when trying to access a file with an ID that doesn't exist."""

    def __init__(
        self, bucket_id: Optional[str] = None, object_id: Optional[str] = None
    ):
        message = (
            "The object "
            + (f"with ID '{object_id}' " if object_id else "")
            + (f"in bucket with ID '{bucket_id}' " if bucket_id else "")
            + "already exist."
        )
        super().__init__(message)


class BucketIdValidationError(BucketError):
    """Thrown when a bucket ID is not valid."""

    def __init__(self, bucket_id: str, reason: Optional[str]):
        message = f"The specified bucket ID '{bucket_id}' is not valid" + (
            f": {reason}." if reason else "."
        )
        super().__init__(message)


class ObjectIdValidationError(ObjectError):
    """Thrown when an object ID is not valid."""

    def __init__(self, object_id: str, reason: Optional[str]):
        message = f"The specified object ID '{object_id}' is not valid" + (
            f": {reason}." if reason else "."
        )
        super().__init__(message)


class MultiPartUploadError(ObjectError):
    """Thrown when a confirmation of an upload is rejected."""


class MultiPartUploadAlreadyExistsError(MultiPartUploadError):
    """Thrown when trying to create a multi-part upload for an object for which another
    upload is already active."""

    def __init__(self, bucket_id: str, object_id: str):
        message = (
            f"Failed to initiate a multi-part upload for object '{object_id}' in bucket"
            + f" '{bucket_id}. Another upload is already ongoing for that file."
        )
        super().__init__(message)


class MultipleActiveUploadsError(MultiPartUploadError):
    """Thrown when multiple active multi-part uploads are detected for the same object."""

    def __init__(self, bucket_id: str, object_id: str, upload_ids: list[str]):
        message = (
            f"Multiple active multi-part uploads were detected for object '{object_id}'"
            + f" in bucket '{bucket_id}. Another upload is already ongoing for that"
            + f" file. The IDs of the active uploads are: {','.join(upload_ids)}"
        )
        super().__init__(message)


class MultiPartUploadNotFoundError(MultiPartUploadError):
    """Thrown when a upload with the specified upload, bucket, and object id was not found."""

    def __init__(
        self,
        upload_id: str,
        bucket_id: str,
        object_id: str,
        details: Optional[str] = None,
    ):
        message = (
            f"The multi-part upload with ID '{upload_id}' for object '{object_id}'"
            + f" in bucket '{bucket_id} could not be found"
            + (f": {details}." if details else ".")
        )
        super().__init__(message)


class MultiPartUploadConfirmError(MultiPartUploadError):
    """Thrown when a confirmation of an upload is rejected."""

    def __init__(
        self,
        upload_id: str,
        bucket_id: str,
        object_id: str,
        reason: Optional[str] = None,
    ):
        message = (
            f"The confirmation of multi-part upload '{upload_id}' for object"
            + f" '{object_id}' in bucket '{bucket_id} was rejected"
            + (f": {reason}." if reason else ".")
        )
        super().__init__(message)


class MultiPartUploadAbortError(MultiPartUploadError):
    """Thrown when failed to abort a multi-part upload."""

    def __init__(
        self,
        upload_id: str,
        bucket_id: str,
        object_id: str,
    ):
        message = (
            f"Failed to abort the multi-part upload '{upload_id}' for object"
            + f" '{object_id}' in bucket '{bucket_id}'. An ongoing part upload might"
            + " be a reason. Please complete all part upload and try to abort again."
        )
        super().__init__(message)


def validate_bucket_id(bucket_id: str):
    """Check whether a bucket id follows the recommended naming pattern.
    This is roughly based on:
    https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html
    Raises BucketIdValidationError if not valid.
    """
    if len(bucket_id) not in range(3, 64):
        raise BucketIdValidationError(
            bucket_id=bucket_id,
            reason="must be between 3 and 63 character long",
        )
    if not re.match(r"^[a-z0-9\-]*$", bucket_id):
        raise BucketIdValidationError(
            bucket_id=bucket_id,
            reason="only lowercase letters, numbers, and hyphens (-) are allowd",
        )
    if bucket_id.startswith("-") or bucket_id.endswith("-"):
        raise BucketIdValidationError(
            bucket_id=bucket_id,
            reason="may not start or end with a hyphen (-).",
        )


def validate_object_id(object_id: str):
    """Check whether a object id follows the recommended naming pattern.
    This is roughly based on (plus some additional restrictions):
    https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
    Raises ObjectIdValidationError if not valid.
    """
    if len(object_id) not in range(3, 64):
        raise ObjectIdValidationError(
            object_id=object_id,
            reason="must be between 3 and 63 character long",
        )
    if not re.match(r"^[a-zA-Z0-9\-\.]*$", object_id):
        raise ObjectIdValidationError(
            object_id=object_id,
            reason="only letters, numbers, and hyphens (-), and dots (.) are allowd",
        )
    if re.match(r"^[\-\.].*", object_id) or re.match(r".*[\-\.]$", object_id):
        raise ObjectIdValidationError(
            object_id=object_id,
            reason="may not start or end with a hyphen (-) or a dot (.).",
        )


@dataclass
class PresignedPostURL:
    """Container for presigned POST URLs along with additional metadata fields that
    should be attached as body data when sending the POST request."""

    url: str
    fields: dict


class ObjectStorageDao(DaoGenericBase):
    """
    A DAO base class for interacting with file objects.
    Exceptions may include:
        - NotImplementedError (if a DAO implementation chooses to not implement a
          specific method)
        - ObjectStorageDaoError, or derived exceptions:
            - OutOfContextError (if the context manager protocol is not used correctly)
            - BucketError, or derived exceptions:
                - BucketIdValidationError
                - BucketNotFoundError
                - BucketAlreadyExists
            - ObjectError, or derived exceptions:
                - ObjectIdValidationError
                - ObjectNotFoundError
                - ObjectAlreadyExistsError
            - UploadConfirmationError
    For raising BucketIdValidationError and ObjectIdValidationError, it is recommended
    that the implementation uses the functions validate_bucket_id and
    validate_object_id.
    Please note, it is not required for an ObjectStorageDao to use the above errors.
    So when using an implementation you cannot rely on above errors to be always raised
    in the suitable situations.
    """

    # Connection/session teardown and setup should
    # be handled using pythons context manager protocol
    # by implementing `__enter__` and `__exit__`
    # methods:

    def __enter__(self) -> "ObjectStorageDao":
        """Setup storage connection/session."""
        ...
        return self

    def __exit__(self, err_type, err_value, err_traceback):
        """Teardown storage connection/session"""
        ...

    # Please implement following methods for interacting
    # with file objects and buckets:

    def does_bucket_exist(self, bucket_id: str) -> bool:
        """Check whether a bucket with the specified ID (`bucket_id`) exists.
        Return `True` if it exists and `False` otherwise.
        """
        raise NotImplementedError()

    def create_bucket(self, bucket_id: str) -> None:
        """
        Create a bucket (= a structure that can hold multiple file objects) with the
        specified unique ID.
        """
        raise NotImplementedError()

    def delete_bucket(self, bucket_id: str, delete_content: bool = False) -> None:
        """
        Delete a bucket (= a structure that can hold multiple file objects) with the
        specified unique ID. If `delete_content` is set to True, any contained objects
        will be deleted, if False (the default) an Error will be raised if the bucket is
        not empty.
        """
        raise NotImplementedError()

    def get_object_upload_url(
        self,
        bucket_id: str,
        object_id: str,
        expires_after: int = 86400,
        max_upload_size: Optional[int] = None,
    ) -> PresignedPostURL:
        """Generates and returns an HTTP URL to upload a new file object with the given
        id (`object_id`) to the bucket with the specified id (`bucket_id`).
        You may also specify a custom expiry duration in seconds (`expires_after`) and
        a maximum size (bytes) for uploads (`max_upload_size`).
        """
        raise NotImplementedError()

    def init_multipart_upload(
        self,
        bucket_id: str,
        object_id: str,
    ) -> str:
        """Initiates a mulipart upload procedure. Returns the upload ID."""
        raise NotImplementedError()

    def get_part_upload_url(
        self, upload_id: str, bucket_id: str, object_id: str, part_number: int
    ) -> str:
        """Given a id of an instantiated multipart upload along with the corresponding
        bucket and object ID, it returns a presign URL for uploading a file part with the
        specified number.
        Please note: the part number must be a non-zero, positive integer and parts
        should be uploaded in sequence.
        """
        raise NotImplementedError()

    def abort_multipart_upload(
        self,
        upload_id: str,
        bucket_id: str,
        object_id: str,
    ) -> None:
        """Cancel a multipart upload with the specified ID. All uploaded content is
        deleted.
        """
        raise NotImplementedError()

    # pylint: disable=too-many-arguments
    def complete_multipart_upload(
        self,
        upload_id: str,
        bucket_id: str,
        object_id: str,
        anticipated_part_quantity: Optional[int] = None,
        anticipated_part_size: Optional[int] = None,
    ) -> None:
        """Completes a multipart upload with the specified ID. In addition to the
        corresponding bucket and object id, you also specify an anticipated part size
        and an anticipated part quantity.
        This ensures that exactly the specified number of parts exist and that all parts
        (except the last one) have the specified size.

        """
        raise NotImplementedError()

    def get_object_download_url(
        self, bucket_id: str, object_id: str, expires_after: int = 86400
    ) -> str:
        """Generates and returns a presigns HTTP-URL to download a file object with
        the specified ID (`object_id`) from bucket with the specified id (`bucket_id`).
        You may also specify a custom expiry duration in seconds (`expires_after`).
        """
        raise NotImplementedError()

    def does_object_exist(
        self, bucket_id: str, object_id: str, object_md5sum: Optional[str] = None
    ) -> bool:
        """Check whether an object with specified ID (`object_id`) exists in the bucket
        with the specified id (`bucket_id`). Optionally, a md5 checksum (`object_md5sum`)
        may be provided to check the objects content.
        Return `True` if checks succeed and `False` otherwise.
        """
        raise NotImplementedError()

    def copy_object(
        self,
        source_bucket_id: str,
        source_object_id: str,
        dest_bucket_id: str,
        dest_object_id: str,
    ) -> None:
        """Copy an object from one bucket(`source_bucket_id` and `source_object_id`) to
        another bucket (`dest_bucket_id` and `dest_object_id`).
        """
        raise NotImplementedError()

    def delete_object(
        self, bucket_id: str, object_id: str, expires_after: int = 86400
    ) -> None:
        """Generates and returns an HTTP URL to upload a new file object with the given
        id (`object_id`) to the bucket with the specified id (`bucket_id`).
        You may also specify a custom expiry duration in seconds (`expires_after`).
        """
        raise NotImplementedError()
