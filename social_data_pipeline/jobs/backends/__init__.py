"""Backend adapters for the query scheduler."""

from .base import Backend, BackendError, ExecutionHandle
from .mongo import MongoBackend
from .mongo import _MONGO_EXTS, _collection_from_job, _pipeline_from_job
from .postgres import PostgresBackend
from .starrocks import StarrocksBackend
from .base import strip_trailing_semicolon, validate_filename
from ..store import Job


def validate_submission(job: Job) -> None:
    """Pre-flight validate a job at submit time, before it lands in the queue.

    Mirrors the per-backend ``Backend.validate(job)`` checks, but is callable
    from the MCP submit path (which doesn't have a constructed Backend). Lets
    bad submissions reject immediately instead of waiting through an approval
    click to fail at execute time. Raises ``BackendError`` on any problem.
    """
    if job.backend in ("postgres", "starrocks"):
        validate_filename(job.output_filename)
        strip_trailing_semicolon(job.sql)
    elif job.backend == "mongodb":
        validate_filename(job.output_filename, allowed_exts=_MONGO_EXTS)
        _pipeline_from_job(job)
        _collection_from_job(job)
        if not (job.database or "").strip():
            raise BackendError(
                "mongo job has no database — submit with `database=<name>`."
            )
    else:
        raise BackendError(f"unknown backend: {job.backend!r}")


__all__ = [
    "Backend",
    "BackendError",
    "ExecutionHandle",
    "MongoBackend",
    "PostgresBackend",
    "StarrocksBackend",
    "validate_submission",
]
