"""Host-local exclusion for all Playwright actions against X.

SQS FIFO groups isolate individual profile retries.  They deliberately do not
serialize different profiles, so the local runnable pipeline holds this lock
for its entire discovery and consumption lifetime instead.
"""

from __future__ import annotations

import fcntl
from pathlib import Path
from typing import TextIO


class XWorkerLockUnavailableError(RuntimeError):
    """Raised before work starts when another local X worker holds the lock."""


_held_paths: set[Path] = set()


class ExclusiveLocalXWorkerLock:
    """A non-blocking, crash-safe advisory lock shared by all local companies."""

    def __init__(self, path: Path):
        self.path = path.expanduser().resolve()
        self._file: TextIO | None = None

    def acquire(self) -> None:
        if self._file is not None or self.path in _held_paths:
            raise XWorkerLockUnavailableError(
                "another local X discovery worker is already running; wait for it to finish or stop it first"
            )
        self.path.parent.mkdir(parents=True, exist_ok=True)
        lock_file = self.path.open("a+", encoding="utf-8")
        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            lock_file.close()
            raise XWorkerLockUnavailableError(
                "another local X discovery worker is already running; wait for it to finish or stop it first"
            ) from exc
        self._file = lock_file
        _held_paths.add(self.path)

    def release(self) -> None:
        if self._file is None:
            return
        try:
            fcntl.flock(self._file.fileno(), fcntl.LOCK_UN)
        finally:
            self._file.close()
            self._file = None
            _held_paths.discard(self.path)

    def __enter__(self) -> "ExclusiveLocalXWorkerLock":
        self.acquire()
        return self

    def __exit__(self, *_: object) -> None:
        self.release()
