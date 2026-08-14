"""Process-wide gate for authenticated X browser work."""
from __future__ import annotations

import threading
from contextlib import contextmanager
from typing import Iterator


_x_session_lock = threading.Semaphore(1)


@contextmanager
def x_session_slot() -> Iterator[None]:
    """Allow only one X pipeline to use the authenticated session at a time."""
    _x_session_lock.acquire()
    try:
        yield
    finally:
        _x_session_lock.release()
