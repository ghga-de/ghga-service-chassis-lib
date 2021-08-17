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

"""Uitls for Fixture handling"""

import signal
import socket
from contextlib import closing
from typing import Callable


def find_free_port():
    """Find a free port."""
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        sock.bind(("", 0))
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return sock.getsockname()[1]


class TimeOutException(Exception):
    """Thrown by timeout handler."""


def raise_timeout(_, __):
    """Raise a TimeOutException"""
    raise TimeOutException()


def set_timeout(sec: int):
    """Decorater factory"""

    def timeout(func: Callable):
        """Decorator that wraps a function with timeout handler"""

        def func_wrapper(*args, **kwargs):
            """Wrapper around original function that adds a timeout handler"""
            # set a timer that raises an exception if timed out
            signal.signal(signal.SIGALRM, raise_timeout)
            signal.alarm(sec)
            # execute the function
            func(*args, **kwargs)
            # disable the timer
            signal.alarm(0)

        return func_wrapper

    return timeout
