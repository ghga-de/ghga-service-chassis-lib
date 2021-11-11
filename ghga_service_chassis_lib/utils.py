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

"""General utilities that don't require heavy dependencies."""


class DaoGenericBase:
    """A generic base for implementing DAO interfaces."""

    # Every DAO must support the context manager protocol.
    # However, it is up to the DAO implementation whether
    # the `__enter__` and `__exit__` functions are doing
    # something useful. They may remain stubs:

    def __enter__(self):
        """Setup logic."""
        ...
        return self

    def __exit__(self, err_type, err_value, err_traceback):
        """Teardown logic."""
        ...


class AsyncDaoGenericBase:
    """A generic base for implementing an asynchronous DAO interfaces."""

    # Every DAO must support the context manager protocol.
    # However, it is up to the DAO implementation whether
    # the `__enter__` and `__exit__` functions are doing
    # something useful. They may remain stubs:

    async def __aenter__(self):
        """Setup logic."""
        ...
        return self

    # pylint: disable=unused-argument,no-self-use
    async def _aexit__(self, err_type, err_value, err_traceback):
        """Teardown logic."""
        ...
