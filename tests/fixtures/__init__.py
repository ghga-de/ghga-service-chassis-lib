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
    Fixtures for metadata
"""
import json
import os

import mongomock
import pymongo
import pytest
from fastapi.testclient import TestClient
from metadata_service.api import app


@pytest.fixture(scope="session")
@mongomock.patch(servers=(("localhost", 28017),))
def initialize_test_db():
    """Initialize a test metadata store using mongomock"""
    curr_dir = os.path.dirname(__file__)
    json_files = [
        ("datasets.json", "dataset"),
        ("studies.json", "study"),
        ("experiments.json", "experiment"),
    ]
    client = pymongo.MongoClient("localhost:28017")
    for file, collection_name in json_files:
        objects = json.load(open(os.path.join(curr_dir, "..", "..", "examples", file)))
        client["test-metadata"][collection_name].delete_many({})
        client["test-metadata"][collection_name].insert_many(
            objects[file.split(".")[0]]
        )
