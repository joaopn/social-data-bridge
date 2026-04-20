"""Shared protocol + helpers for backend adapters."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Protocol

from ..store import Job


# Path inside PG / SR containers where the jobs results folder is bind-mounted.
# The host directory is <result_root>; the mount is added to the DB services
# via docker-compose.override.yml when setup-jobs runs.
JOBS_EXPORT_CONTAINER_PATH = "/jobs_export"


class BackendError(RuntimeError):
    """Backend rejected a submission or raised during execution."""


@dataclass
class ExecutionHandle:
    """Opaque reference to a running query. Persisted into running/<id>.json
    so the web UI's Kill button can read it and call cancel() from a separate
    HTTP handler thread."""

    backend_pid: int | None = None
    connection_id: int | None = None


@dataclass
class ExecutionResult:
    """Outcome metadata written to the job JSON after a successful execute()."""

    rows: int | None = None
    size_bytes: int | None = None
    result_path: str | None = None


HandleCallback = Callable[[ExecutionHandle], None]


class Backend(Protocol):
    name: str

    def validate(self, job: Job) -> None:
        """Raise BackendError with a clear message if the submission is malformed."""
        ...

    def execute(
        self,
        job: Job,
        timeout_seconds: int,
        on_handle: HandleCallback,
    ) -> ExecutionResult:
        """Blocks until the query completes. Calls ``on_handle(handle)`` once
        as soon as the connection exposes a PID / connection_id so the UI Kill
        path has something to target."""
        ...

    def cancel(self, handle: ExecutionHandle) -> None:
        """Terminate a running query identified by the handle. Opens its own
        admin connection — must not share state with execute()."""
        ...


# ----------------------------------------------------------------------------
# Helpers shared by backends.


_FILENAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_ALLOWED_EXTS: frozenset[str] = frozenset({".parquet", ".csv"})


def validate_filename(name: str, allowed_exts: frozenset[str] = _ALLOWED_EXTS) -> str:
    """Reject paths, path traversal, and unknown extensions. Returns the ext."""
    if "/" in name or "\\" in name or name in ("", ".", ".."):
        raise BackendError(f"output_filename must be a basename (no slashes): {name!r}")
    if not _FILENAME_RE.match(name):
        raise BackendError(
            f"output_filename must match [A-Za-z0-9._-], 1-128 chars: {name!r}"
        )
    ext = Path(name).suffix.lower()
    if ext not in allowed_exts:
        raise BackendError(
            f"output_filename extension {ext!r} not supported (expected one of {sorted(allowed_exts)})"
        )
    return ext


def strip_trailing_semicolon(sql: str) -> str:
    s = sql.strip()
    while s.endswith(";"):
        s = s[:-1].rstrip()
    if not s:
        raise BackendError("sql is empty")
    return s


def dir_size_bytes(path: Path) -> int:
    total = 0
    for p in path.rglob("*"):
        if p.is_file():
            try:
                total += p.stat().st_size
            except FileNotFoundError:
                continue
    return total
