# Copyright 2021 - 2023 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
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

from __future__ import annotations

import os
import signal
from abc import ABC
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, BinaryIO, Callable, Generator, Optional, TypeVar, cast

from pydantic import BaseSettings, parse_obj_as

__all__ = [
    "AsyncDaoGenericBase",
    "DaoGenericBase",
    "DateTimeUTC",
    "OutOfContextError",
    "UTC",
    "big_temp_file",
    "assert_tz_is_utc",
    "create_fake_drs_uri",
    "exec_with_timeout",
    "now_as_utc",
]

T = TypeVar("T")

TEST_FILE_DIR = Path(__file__).parent.resolve() / "test_files"

TEST_FILE_PATHS = [
    TEST_FILE_DIR / filename
    for filename in os.listdir(TEST_FILE_DIR)
    if filename.startswith("test_") and filename.endswith(".yaml")
]

UTC = timezone.utc


class OutOfContextError(RuntimeError):
    """Thrown when a context manager is used out of context."""

    def __init__(self):
        message = "Used context manager outside of a with block."
        super().__init__(message)


class DaoGenericBase:
    """A generic base for implementing DAO interfaces."""

    def __init__(self, config: BaseSettings):  # pylint: disable=unused-argument
        """Initialize DAO with a config params passed as pydantic BaseSettings object."""
        ...

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

    def __init__(self, config: BaseSettings):  # pylint: disable=unused-argument
        """Initialize DAO with a config params passed as pydantic BaseSettings object."""
        ...

    # Every DAO must support the context manager protocol.
    # However, it is up to the DAO implementation whether
    # the `__enter__` and `__exit__` functions are doing
    # something useful. They may remain stubs:

    async def __aenter__(self):
        """Setup logic."""
        ...
        return self

    # pylint: disable=unused-argument
    async def _aexit__(self, err_type, err_value, err_traceback):
        """Teardown logic."""
        ...


def raise_timeout_error(_signalnum, _handler) -> None:
    """Raise a TimeoutError"""
    raise TimeoutError()


def exec_with_timeout(
    func: Callable[..., T],
    timeout_after: int,
    func_args: Optional[list] = None,
    func_kwargs: Optional[dict] = None,
) -> T:
    """
    Exec a function (`func`) with a specified timeout (`timeout_after` in seconds).
    If the function doesn't finish before the timeout, a TimeoutError is thrown.
    """

    func_args_ = [] if func_args is None else func_args
    func_kwargs_ = {} if func_kwargs is None else func_kwargs

    # set a timer that raises an exception if timed out
    signal.signal(signal.SIGALRM, raise_timeout_error)
    signal.alarm(timeout_after)

    # execute the function
    result = func(*func_args_, **func_kwargs_)

    # disable the timer
    signal.alarm(0)

    return result


def create_fake_drs_uri(object_id: str) -> str:
    """Create a fake DRS URI based on an object id."""
    return f"drs://www.example.org/{object_id}"


class NamedBinaryIO(ABC, BinaryIO):
    """Return type of NamedTemporaryFile."""

    name: str


@contextmanager
def big_temp_file(size: int) -> Generator[NamedBinaryIO, None, None]:
    """Generates a big file with approximately the specified size in bytes."""
    current_size = 0
    current_number = 0
    next_number = 1
    with NamedTemporaryFile("w+b") as temp_file:
        while current_size <= size:
            byte_addition = f"{current_number}\n".encode("ASCII")
            current_size += len(byte_addition)
            temp_file.write(byte_addition)
            previous_number = current_number
            current_number = next_number
            next_number = previous_number + current_number
        temp_file.flush()
        yield cast(NamedBinaryIO, temp_file)


class DateTimeUTC(datetime):
    """A pydantic type for values that should have an UTC timezone.

    This behaves exactly like the normal datetime type, but requires that the value
    has a timezone and converts the timezone to UTC if necessary.
    """

    @classmethod
    def construct(cls, *args, **kwargs) -> DateTimeUTC:
        """Construct a datetime with UTC timezone."""
        if kwargs.get("tzinfo") is None:
            kwargs["tzinfo"] = UTC
        return cls(*args, **kwargs)

    @classmethod
    def __get_validators__(cls) -> Generator[Callable[[Any], datetime], None, None]:
        """Get all validators."""
        yield cls.validate

    @classmethod
    def validate(cls, value: Any) -> datetime:
        """Validate the given value."""
        date_value = parse_obj_as(datetime, value)
        if date_value.tzinfo is None:
            raise ValueError(f"Date-time value is missing a timezone: {value!r}")
        if date_value.tzinfo is not UTC:
            date_value = date_value.astimezone(UTC)
        return date_value


def assert_tz_is_utc() -> None:
    """Verifies that the default timezone is set to UTC.

    Raises a Runtimeerror if the default timezone is set differently.
    """
    if datetime.now().astimezone().tzinfo != UTC:
        raise RuntimeError("System must be configured to use UTC.")


def now_as_utc() -> DateTimeUTC:
    """Return the current datetime with UTC timezone.

    Note: This is different from datetime.utcnow() which has no timezone.
    """
    return DateTimeUTC.now(UTC)
