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

"""fixtures for testing the `object_storage_dao` module"""

TOO_LONG_ID = "a" * 64
TOO_SHORT_ID = "a1"

VALID_BUCKET_ID = "ghgas-12239992232323422"
VALID_OBJECT_ID = "ghgaf-12239992232323422.test"

BAD_CHARS_BUCKET_ID = ["A", "_", ".", "/", "&", "+", ":"]
BAD_CHARS_OBJECT_ID = ["_", "/", "&", "+", ":"]

BAD_BUCKET_IDS = ["-aa", "aa-"]
BAD_OBJECT_IDS = ["-aa", "aa-", ".aa", "aa."]
