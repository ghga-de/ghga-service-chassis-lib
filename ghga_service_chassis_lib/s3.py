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
This modules contains logic for interacting with S3-compatible object storage.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import boto3
import botocore.client
import botocore.config
import botocore.configloader
import botocore.exceptions

from .object_storage_dao import (
    BucketAlreadyExists,
    BucketError,
    BucketNotFoundError,
    ObjectAlreadyExistsError,
    ObjectError,
    ObjectNotFoundError,
    ObjectStorageDao,
    ObjectStorageDaoError,
    OutOfContextError,
    PresignedPostURL,
)


def read_aws_config_ini(aws_config_ini: Path) -> botocore.config.Config:
    """
    Reads an aws config ini (see:
    https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html#using-a-configuration-file)
    and returns an botocore.config.Config object.
    """

    config_profile = botocore.configloader.load_config(config_filename=aws_config_ini)
    return botocore.config.Config(**config_profile)


def _format_s3_error_code(error_code: str):
    """Formats a message to describe and s3 error code."""
    return f"S3 error with code: '{error_code}'"


def _translate_s3_client_errors(
    source_exception: botocore.exceptions.ClientError,
    bucket_id: Optional[str] = None,
    object_id: Optional[str] = None,
) -> Exception:
    """
    Translates S3 client errors based on their error codes into exceptions from the
    .object_storage_dao modules
    """
    error_code = source_exception.response["Error"]["Code"]

    exception: ObjectStorageDaoError

    # try to exactly match the error code:
    if error_code == "NoSuchBucket":
        exception = BucketNotFoundError(bucket_id=bucket_id)
    elif error_code == "BucketAlreadyExists":
        exception = BucketAlreadyExists(bucket_id=bucket_id)
    elif error_code == "NoSuchKey":
        exception = ObjectNotFoundError(object_id=object_id)
    elif error_code == "ObjectAlreadyInActiveTierError":
        exception = ObjectAlreadyExistsError(object_id=object_id)
    else:
        # exact match not found, match by keyword:
        if "Bucket" in error_code:
            exception = BucketError(_format_s3_error_code(error_code))
        elif "Object" in error_code or "Key" in error_code:
            exception = ObjectError(_format_s3_error_code(error_code))
        else:
            # if nothing matches, return a generic error:
            exception = ObjectStorageDaoError(_format_s3_error_code(error_code))

    return exception


@dataclass
class S3Credentials:
    """Container for credentials needed to connect to an (AWS) S3 service."""

    aws_access_key_id: str
    aws_secret_access_key: str
    aws_session_token: Optional[str] = None


class ObjectStorageS3(ObjectStorageDao):  # pylint: disable=too-many-instance-attributes
    """
    An implementation of the ObjectStorageDao interface for interacting specifically
    with S3 object storages.
    Exceptions may include:
        - BucketError, or derived exceptions:
            - BucketNotFoundError
            - BucketIdAlreadyInUse
        - ObjectError, or derived exceptions:
            - ObjectNotFoundError
            - ObjectAlreadyExistsError
    """

    _out_of_context_error = OutOfContextError(context_manager_name="ObjectStorageS3")

    def __init__(  # pylint: disable=too-many-arguments
        self,
        endpoint_url: str,
        credentials: S3Credentials,
        service_name: str = "s3",
        aws_config_ini: Optional[Path] = None,
        max_upload_size: Optional[int] = None,
    ):
        """Initialize with parameters needed to connect to the S3 storage

        The arguments are adapted from the boto3 library.
        Please have a look here for more descriptions:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html#boto3.session.Session.client

        Args:
            endpoint_url (str): The complete URL to use for the constructed client.
            service_name (str, optional):
                The name of a service, e.g. 's3' or 'ec2'.Defaults to "s3".
            credentials (S3Credentials): Credentials for login into the S3 service.
            aws_config_ini (Optional[Path], optional):
                Path to a config file for specifying more advanced S3 parameters.
                This should follow the format described here:
                https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html#using-a-configuration-file
                Defaults to None.
            max_upload_size (Optional[int], optional):
                Size limit for object uploads in bytes. Defaults to None (= no limit).

        """
        self.endpoint_url = endpoint_url
        self.service_name = service_name
        self.aws_config_ini = aws_config_ini
        self.max_upload_size = max_upload_size

        self._credentials = credentials
        self._advanced_config = (
            None
            if self.aws_config_ini is None
            else read_aws_config_ini(self.aws_config_ini)
        )

        # will be set on __enter__:
        self._client: Optional[botocore.client.BaseClient] = None
        self._resource: Optional[botocore.client.BaseClient] = None

    def __repr__(self) -> str:
        return (
            "ObjectStorageS3("
            + f"endpoint_url={self.endpoint_url}, "
            # credentials are missing on purpose
            + f"service_name={self.service_name}, "
            + f"aws_config_ini={self.aws_config_ini}, "
            + f"max_upload_size={self.max_upload_size}"
            + ")"
        )

    def __enter__(self) -> ObjectStorageDao:
        """Setup storage connection/session."""

        self._client = boto3.client(
            service_name=self.service_name,
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self._credentials.aws_access_key_id,
            aws_secret_access_key=self._credentials.aws_secret_access_key,
            aws_session_token=self._credentials.aws_session_token,
            config=self._advanced_config,
        )

        self._resource = boto3.resource(
            service_name=self.service_name,
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self._credentials.aws_access_key_id,
            aws_secret_access_key=self._credentials.aws_secret_access_key,
            aws_session_token=self._credentials.aws_session_token,
            config=self._advanced_config,
        )

        return self

    def __exit__(self, err_type, err_value, err_traceback):
        """Teardown storage connection/session"""
        # no special teardown is needed for this DAO implementation,
        # just deleting the reference to the client instance:
        self._client = None

    def does_bucket_exist(self, bucket_id: str) -> bool:
        """Check whether a bucket with the specified ID (`bucket_id`) exists.
        Return `True` if it exists and `False` otherwise.
        """
        if not isinstance(self._client, botocore.client.BaseClient):
            raise self._out_of_context_error

        try:
            bucket_list = self._client.list_buckets()
        except botocore.exceptions.ClientError as error:
            raise _translate_s3_client_errors(error, bucket_id=bucket_id) from error

        for bucket in bucket_list["Buckets"]:
            if bucket["Name"] == bucket_id:
                return True

        return False

    def _assert_bucket_exists(self, bucket_id: str) -> None:
        """Checks if the bucket with specified ID (`bucket_id`) exists and throws an
        BucketNotFoundError otherwise.
        """
        if not self.does_bucket_exist(bucket_id):
            raise BucketNotFoundError(bucket_id=bucket_id)

    def _assert_bucket_not_exists(self, bucket_id: str) -> None:
        """Checks if the bucket with specified ID (`bucket_id`) exists. If so, it throws
        an BucketAlreadyExists.
        """
        if self.does_bucket_exist(bucket_id):
            raise BucketAlreadyExists(bucket_id=bucket_id)

    def create_bucket(self, bucket_id: str) -> None:
        """
        Create a bucket (= a structure that can hold multiple file objects) with the
        specified unique ID.
        """
        if not isinstance(self._client, botocore.client.BaseClient):
            raise self._out_of_context_error

        self._assert_bucket_not_exists(bucket_id)

        try:
            self._client.create_bucket(Bucket=bucket_id)
        except botocore.exceptions.ClientError as error:
            raise _translate_s3_client_errors(error, bucket_id=bucket_id) from error

    def delete_bucket(self, bucket_id: str, delete_content: bool = False) -> None:
        """
        Delete a bucket (= a structure that can hold multiple file objects) with the
        specified unique ID. If `delete_content` is set to True, any contained objects
        will be deleted, if False (the default) an Error will be raised if the bucket is
        not empty.
        """
        if not isinstance(self._resource, boto3.resources.factory.ServiceResource):
            raise self._out_of_context_error

        self._assert_bucket_exists(bucket_id)

        try:
            bucket = self._resource.Bucket(bucket_id)
            if delete_content:
                bucket.objects.all().delete()
        except botocore.exceptions.ClientError as error:
            raise _translate_s3_client_errors(error, bucket_id=bucket_id) from error

    def does_object_exist(
        self, bucket_id: str, object_id: str, object_md5sum: Optional[str] = None
    ) -> bool:
        """Check whether an object with specified ID (`object_id`) exists in the bucket
        with the specified id (`bucket_id`). Optionally, a md5 checksum (`object_md5sum`)
        may be provided to check the objects content.
        Return `True` if checks succeed and `False` otherwise.
        """
        if not isinstance(self._client, botocore.client.BaseClient):
            raise self._out_of_context_error

        if object_md5sum is not None:
            raise NotImplementedError("Md5 checking is not yet implemented.")

        try:
            _ = self._client.head_object(
                Bucket=bucket_id,
                Key=object_id,
            )
        except botocore.exceptions.ClientError:
            return False

        return True

    def _assert_object_exists(self, bucket_id: str, object_id: str) -> None:
        """Checks if the file with specified ID (`object_id`) exists in the bucket with
        the specified ID (`bucket_id`) and throws an ObjectNotFoundError otherwise.
        """
        if not self.does_object_exist(bucket_id=bucket_id, object_id=object_id):
            raise ObjectNotFoundError(bucket_id=bucket_id, object_id=object_id)

    def _assert_object_not_exists(self, bucket_id: str, object_id: str) -> None:
        """Checks if the file with specified ID (`object_id`) exists in the bucket with
        the specified ID (`bucket_id`). If so, it throws an ObjectAlreadyExistsError otherwise.
        """
        if self.does_object_exist(bucket_id=bucket_id, object_id=object_id):
            raise ObjectAlreadyExistsError(bucket_id=bucket_id, object_id=object_id)

    def get_object_upload_url(
        self, bucket_id: str, object_id: str, expires_after: int = 86400
    ) -> PresignedPostURL:
        """Generates and returns an HTTP URL to upload a new file object with the given
        id (`object_id`) to the bucket with the specified id (`bucket_id`).
        You may also specify a custom expiry duration in seconds (`expires_after`).
        """
        if not isinstance(self._client, botocore.client.BaseClient):
            raise self._out_of_context_error

        self._assert_object_not_exists(bucket_id=bucket_id, object_id=object_id)

        conditions = [
            # set upload size limit:
            ["content-length-range", 0, self.max_upload_size],
        ]

        try:
            presigned_url = self._client.generate_presigned_post(
                Bucket=bucket_id,
                Key=object_id,
                Conditions=conditions,
                ExpiresIn=expires_after,
            )
        except botocore.exceptions.ClientError as error:
            raise _translate_s3_client_errors(
                error, bucket_id=bucket_id, object_id=object_id
            ) from error

        return PresignedPostURL(
            url=presigned_url["url"], fields=presigned_url["fields"]
        )

    def get_object_download_url(
        self, bucket_id: str, object_id: str, expires_after: int = 86400
    ) -> str:
        """Generates and returns a presigns HTTP-URL to download a file object with
        the specified ID (`object_id`) from bucket with the specified id (`bucket_id`).
        You may also specify a custom expiry duration in seconds (`expires_after`).
        """
        if not isinstance(self._client, botocore.client.BaseClient):
            raise self._out_of_context_error

        self._assert_object_exists(bucket_id=bucket_id, object_id=object_id)

        try:
            presigned_url = self._client.generate_presigned_url(
                "get_object",
                Params={"Bucket": bucket_id, "Key": object_id},
                ExpiresIn=expires_after,
            )
        except botocore.exceptions.ClientError as error:
            raise _translate_s3_client_errors(
                error, bucket_id=bucket_id, object_id=object_id
            ) from error

        return presigned_url

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
        if not isinstance(self._client, botocore.client.BaseClient):
            raise self._out_of_context_error

        self._assert_object_exists(
            bucket_id=source_bucket_id, object_id=source_object_id
        )
        self._assert_object_not_exists(
            bucket_id=dest_bucket_id, object_id=dest_object_id
        )

        try:
            copy_source = {
                "Bucket": source_bucket_id,
                "Key": source_object_id,
            }
            self._client.copy(
                CopySource=copy_source,
                Bucket=dest_bucket_id,
                Key=dest_object_id,
            )
        except botocore.exceptions.ClientError as error:
            raise _translate_s3_client_errors(error) from error

    def delete_object(
        self, bucket_id: str, object_id: str, expires_after: int = 86400
    ) -> None:
        """Generates and returns an HTTP URL to upload a new file object with the given
        id (`object_id`) to the bucket with the specified id (`bucket_id`).
        You may also specify a custom expiry duration in seconds (`expires_after`).
        """
        if not isinstance(self._client, botocore.client.BaseClient):
            raise self._out_of_context_error

        self._assert_object_exists(bucket_id=bucket_id, object_id=object_id)

        try:
            self._client.delete_object(
                Bucket=bucket_id,
                Key=object_id,
            )
        except botocore.exceptions.ClientError as error:
            raise _translate_s3_client_errors(
                error, bucket_id=bucket_id, object_id=object_id
            ) from error
