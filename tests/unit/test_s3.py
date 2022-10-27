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
Test S3 helper methods
"""

from contextlib import nullcontext

import pytest

from ghga_service_chassis_lib.s3 import adapt_part_size

MiB = 1024**2
GiB = 1024**3
TiB = 1024**4


@pytest.mark.parametrize(
    "part_size, file_size, expected_part_size",
    [
        (16 * MiB, 10 * GiB, 16 * MiB),
        (16 * MiB, 200 * GiB, 32 * MiB),
        (4 * MiB, 10 * GiB, 5 * MiB),
        (6 * GiB, 10 * GiB, 5 * GiB),
        (16 * MiB, 10 * TiB, None),
    ],
)
def test_adapt_part_size(part_size: int, file_size: int, expected_part_size: int):
    """Test code to dynamically adapt part size"""
    with pytest.raises(ValueError) if file_size > 5 * TiB else nullcontext():  # type: ignore
        adapted_part_size = adapt_part_size(
            current_part_size=part_size, file_size=file_size
        )
        assert adapted_part_size == expected_part_size
