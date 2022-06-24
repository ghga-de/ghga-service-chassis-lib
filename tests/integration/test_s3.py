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

from typing import Optional

import pytest
from black import nullcontext

from ghga_service_chassis_lib.object_storage_dao import (
    DEFAULT_PART_SIZE,
    BucketAlreadyExists,
    BucketNotFoundError,
    MultiPartUploadAlreadyExistsError,
    MultiPartUploadConfirmError,
    MultiPartUploadNotFoundError,
    MultipleActiveUploadsError,
    ObjectAlreadyExistsError,
    ObjectNotFoundError,
)
from ghga_service_chassis_lib.object_storage_dao_testing import (
    MEBIBYTE,
    ObjectFixture,
    upload_part,
    upload_part_of_size,
)
from ghga_service_chassis_lib.s3_testing import (
    S3Fixture,
    get_initialized_upload,
    prepare_non_completed_upload,
    typical_workflow,
)
from ghga_service_chassis_lib.utils import big_temp_file

from .fixtures.s3 import s3_fixture  # noqa: F401


@pytest.mark.parametrize("use_multipart_upload", [True, False])
def test_typical_workflow(
    use_multipart_upload: bool, s3_fixture: S3Fixture  # noqa: F811
):
    """
    Tests all methods of the ObjectStorageS3 DAO implementation in one long workflow.
    """
    with (
        big_temp_file(size=20 * MEBIBYTE) if use_multipart_upload else nullcontext()
    ) as temp_file:
        object_fixture = (
            ObjectFixture(
                file_path=temp_file.name, bucket_id="", object_id="some-big-file"
            )
            if use_multipart_upload
            else s3_fixture.non_existing_objects[0]
        )

        typical_workflow(
            storage_client=s3_fixture.storage,
            bucket1_id=s3_fixture.non_existing_buckets[0],
            bucket2_id=s3_fixture.non_existing_buckets[1],
            object_id=object_fixture.object_id,
            test_file_md5=object_fixture.md5,
            test_file_path=object_fixture.file_path,
            use_multipart_upload=use_multipart_upload,
        )


def test_object_and_bucket_collisions(s3_fixture: S3Fixture):  # noqa: F811
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


def test_handling_non_existing_file_and_bucket(s3_fixture: S3Fixture):  # noqa: F811
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


@pytest.mark.parametrize(
    "upload_id_correct, bucket_id_correct, object_id_correct, exception",
    [
        (True, True, True, None),
        (False, True, True, MultiPartUploadNotFoundError),
        (True, False, True, BucketNotFoundError),
        (True, True, False, MultiPartUploadNotFoundError),
    ],
)
def test_using_non_existing_upload(
    upload_id_correct: bool,
    bucket_id_correct: bool,
    object_id_correct: bool,
    exception,
    s3_fixture,  # noqa: F811
):
    """
    Makes sure that using a non existing upload_id-bucket_id-object_id combination
    throws the right error.
    """
    # prepare a non-completed upload:
    real_upload_id, real_bucket_id, real_object_id = prepare_non_completed_upload(
        s3_fixture
    )

    # prepare some calls:
    upload_id = real_upload_id if upload_id_correct else "wrong-upload"
    bucket_id = real_bucket_id if bucket_id_correct else "wrong-bucket"
    object_id = real_object_id if object_id_correct else "wrong-object"
    calls = [
        lambda: s3_fixture.storage._assert_multipart_upload_exist(
            upload_id=upload_id, bucket_id=bucket_id, object_id=object_id
        ),
        lambda: s3_fixture.storage.get_part_upload_url(
            upload_id=upload_id, bucket_id=bucket_id, object_id=object_id, part_number=1
        ),
        lambda: s3_fixture.storage.complete_multipart_upload(
            upload_id=upload_id, bucket_id=bucket_id, object_id=object_id
        ),
    ]

    # run the calls and expect exceptions:
    for call in calls:
        with (pytest.raises(exception) if exception else nullcontext()):  # type: ignore
            call()


@pytest.mark.parametrize(
    "part_number, exception",
    [(0, ValueError), (1, None), (10000, None), (10001, ValueError)],
)
def test_invalid_part_number(
    part_number: int,
    exception: Optional[Exception],
    s3_fixture: S3Fixture,  # noqa: F811
):
    """Check that invalid part numbers are cached correcly."""

    upload_id, bucket_id, object_id = prepare_non_completed_upload(s3_fixture)

    with (pytest.raises(exception) if exception else nullcontext()):  # type: ignore
        _ = s3_fixture.storage.get_part_upload_url(
            upload_id=upload_id,
            bucket_id=bucket_id,
            object_id=object_id,
            part_number=part_number,
        )


@pytest.mark.parametrize(
    "part_sizes, anticipated_part_size, anticipated_part_quantity, exception",
    [
        ([10 * MEBIBYTE, 10 * MEBIBYTE, 1 * MEBIBYTE], None, None, None),
        ([10 * MEBIBYTE, 10 * MEBIBYTE, 1 * MEBIBYTE], 10 * MEBIBYTE, 3, None),
        ([], None, None, MultiPartUploadConfirmError),  # zero parts uploaded
        (
            [10 * MEBIBYTE, 10 * MEBIBYTE, 11 * MEBIBYTE],
            None,
            2,
            MultiPartUploadConfirmError,
        ),  # Missmatch with anticipated parts
        (
            [10 * MEBIBYTE, 5 * MEBIBYTE, 1 * MEBIBYTE],
            None,
            None,
            MultiPartUploadConfirmError,
        ),  # heterogenous part sizes
        (
            [10 * MEBIBYTE, 10 * MEBIBYTE, 11 * MEBIBYTE],
            None,
            None,
            MultiPartUploadConfirmError,
        ),  # last part bigger than first part
        (
            [10 * MEBIBYTE, 5 * MEBIBYTE, 1 * MEBIBYTE],
            10 * MEBIBYTE,
            None,
            MultiPartUploadConfirmError,
        ),  # missmatch anticipated part size
        (
            [10 * MEBIBYTE, 10 * MEBIBYTE, 11 * MEBIBYTE],
            10 * MEBIBYTE,
            None,
            MultiPartUploadConfirmError,
        ),  # Too large last part
    ],
)
def test_complete_multipart_upload(
    part_sizes: list[int],
    anticipated_part_size: Optional[int],
    anticipated_part_quantity: Optional[int],
    exception: Optional[Exception],
    s3_fixture: S3Fixture,  # noqa: F811
):
    """
    Test the complete_multipart_upload method.
    """
    upload_id, bucket_id, object_id = get_initialized_upload(s3_fixture)
    for part_idx, part_size in enumerate(part_sizes):
        upload_part_of_size(
            storage_dao=s3_fixture.storage,
            upload_id=upload_id,
            bucket_id=bucket_id,
            object_id=object_id,
            size=part_size,
            part_number=part_idx + 1,
        )

    with (pytest.raises(exception) if exception else nullcontext()):  # type: ignore
        s3_fixture.storage.complete_multipart_upload(
            upload_id=upload_id,
            bucket_id=bucket_id,
            object_id=object_id,
            anticipated_part_quantity=anticipated_part_quantity,
            anticipated_part_size=anticipated_part_size,
        )


@pytest.mark.parametrize("empty_upload", (True, False))
def test_abort_multipart_upload(
    empty_upload: bool,
    s3_fixture: S3Fixture,  # noqa: F811
):
    """
    Test the abort_multipart_upload method.
    """

    upload_id, bucket_id, object_id = get_initialized_upload(s3_fixture)

    upload_part = lambda part_number: upload_part_of_size(
        storage_dao=s3_fixture.storage,
        upload_id=upload_id,
        bucket_id=bucket_id,
        object_id=object_id,
        size=DEFAULT_PART_SIZE,
        part_number=part_number,
    )

    if not empty_upload:
        # upload 2 parts:
        for part_number in range(1, 3):
            upload_part(part_number)

    s3_fixture.storage.abort_multipart_upload(
        upload_id=upload_id,
        bucket_id=bucket_id,
        object_id=object_id,
    )

    # verify that the upload cannot be continued:
    with pytest.raises(MultiPartUploadNotFoundError):
        upload_part(part_number=3)

    # ... and also not restarted:
    with pytest.raises(MultiPartUploadNotFoundError):
        upload_part(part_number=1)


def test_multiple_active_uploads(s3_fixture: S3Fixture):  # noqa: F811
    """
    Test that multiple active uploads for the same object are not possible.
    """

    # initialize an upload:
    _, bucket_id, object_id = get_initialized_upload(s3_fixture)

    # initialize another upload for the same object:
    with pytest.raises(MultiPartUploadAlreadyExistsError):
        _ = s3_fixture.storage.init_multipart_upload(
            bucket_id=bucket_id, object_id=object_id
        )


def test_handling_multiple_coexisting_uploads(s3_fixture: S3Fixture):  # noqa: F811
    """
    Test that the invalid state of multiple uploads coexisting for the same object
    is correctly handeled.
    """

    # initialize an upload:
    upload1_id, bucket_id, object_id = get_initialized_upload(s3_fixture)

    # initialize another upload bypassing any checks:
    upload2_id = s3_fixture.storage._client.create_multipart_upload(  # type: ignore
        Bucket=bucket_id, Key=object_id
    )["UploadId"]

    # try to work on both uploads:
    for upload_id in [upload1_id, upload2_id]:
        with pytest.raises(MultipleActiveUploadsError):
            s3_fixture.storage.get_part_upload_url(
                upload_id=upload_id,
                bucket_id=bucket_id,
                object_id=object_id,
                part_number=1,
            )

        with pytest.raises(MultipleActiveUploadsError):
            s3_fixture.storage.complete_multipart_upload(
                upload_id=upload_id, bucket_id=bucket_id, object_id=object_id
            )

    # aborting should work:
    s3_fixture.storage.abort_multipart_upload(
        upload_id=upload2_id, bucket_id=bucket_id, object_id=object_id
    )

    # confirm that aborting one upload fixes the problem
    upload_part(
        s3_fixture.storage,
        upload_id=upload1_id,
        bucket_id=bucket_id,
        object_id=object_id,
        content=b"Test content.",
        part_number=1,
    )
    s3_fixture.storage.complete_multipart_upload(
        upload_id=upload1_id, bucket_id=bucket_id, object_id=object_id
    )


@pytest.mark.parametrize("abort_first", (True, False))
def test_handling_multiple_subsequent_uploads(
    abort_first: bool, s3_fixture: S3Fixture  # noqa: F811
):
    """
    Ensure that multiple subsequent uploads that target the same object are handled
    correctly. Three cases shall be distinguished:
        1. initiate an upload, upload some parts, abort it, then start another
          upload, uploads some parts, complete it (`abort_first` set to True)
        2. initiate an upload, upload some parts, complete it, then start another
          upload, uploads some parts, complete it (`abort_first` set to False)
    """

    # perform first upload:
    upload1_id, bucket_id, object_id = get_initialized_upload(s3_fixture)

    upload_part_shortcut = lambda upload_id: upload_part(
        s3_fixture.storage,
        upload_id=upload_id,
        bucket_id=bucket_id,
        object_id=object_id,
        content=b"Test content.",
        part_number=1,
    )

    upload_part_shortcut(upload1_id)
    if abort_first:
        s3_fixture.storage.abort_multipart_upload(
            upload_id=upload1_id, bucket_id=bucket_id, object_id=object_id
        )
    else:
        s3_fixture.storage.complete_multipart_upload(
            upload_id=upload1_id, bucket_id=bucket_id, object_id=object_id
        )

    # perform second upload:
    upload2_id = s3_fixture.storage.init_multipart_upload(
        bucket_id=bucket_id, object_id=object_id
    )

    upload_part_shortcut(upload2_id)
    s3_fixture.storage.complete_multipart_upload(
        upload_id=upload2_id, bucket_id=bucket_id, object_id=object_id
    )
