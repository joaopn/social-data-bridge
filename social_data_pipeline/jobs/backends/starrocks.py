"""StarRocks backend adapter.

Runner appends ``INTO OUTFILE "file:///jobs_export/<job_id>/part_"`` to the
user's SELECT and executes it. StarRocks' BE writes chunked parquet or CSV
directly to the bind-mounted results folder.

Requires ``enable_outfile_to_local = true`` in ``fe.conf`` (setup-jobs writes
this when SR is a configured target).
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path

import mysql.connector
from mysql.connector.errors import Error as MySQLError

from ..config import admin_password, auth_enabled
from ..store import Job
from .base import (
    JOBS_EXPORT_CONTAINER_PATH,
    Backend,
    BackendError,
    ExecutionHandle,
    ExecutionResult,
    HandleCallback,
    dir_size_bytes,
    strip_trailing_semicolon,
    validate_filename,
)


_DEFAULT_MAX_FILE_SIZE = 1073741824  # 1 GiB
_SR_QUERY_TIMEOUT_MAX = 259200  # SR's maximum (72 hours); larger values are rejected


class StarrocksBackend:
    name = "starrocks"

    def __init__(
        self,
        result_root: Path,
        database: str,
        host: str = "starrocks",
        port: int | None = None,
        max_file_size: int = _DEFAULT_MAX_FILE_SIZE,
    ):
        self.result_root = Path(result_root)
        self.database = database
        self.host = host
        self.port = int(port or os.environ.get("STARROCKS_PORT", 9030))
        self.max_file_size = int(max_file_size)

    # ------------------------------------------------------------------

    def validate(self, job: Job) -> None:
        validate_filename(job.output_filename)
        strip_trailing_semicolon(job.sql)

    def execute(
        self,
        job: Job,
        timeout_seconds: int,
        on_handle: HandleCallback,
    ) -> ExecutionResult:
        ext = validate_filename(job.output_filename)
        inner = strip_trailing_semicolon(job.sql)

        job_dir = self.result_root / job.job_id
        if job.overwrite and job_dir.exists():
            shutil.rmtree(job_dir)
        if job_dir.exists():
            raise BackendError(
                f"result folder already exists: {job_dir}. "
                "Resubmit with overwrite=True or use a different job id."
            )
        job_dir.mkdir(parents=True, exist_ok=False)

        # SR's INTO OUTFILE treats the path as a prefix and appends
        # "<N>.<ext>". Use the submitted output_filename's stem so the
        # chunks land as <stem>_0.<ext>, <stem>_1.<ext>, … matching the
        # PG single-file convention as closely as SR's chunking allows.
        stem = Path(job.output_filename).stem
        prefix_in_container = f"{JOBS_EXPORT_CONTAINER_PATH}/{job.job_id}/{stem}_"

        if ext == ".parquet":
            format_as = "PARQUET"
            props = f'"max_file_size" = "{self.max_file_size}"'
        else:
            format_as = "CSV"
            props = (
                f'"max_file_size" = "{self.max_file_size}", '
                '"column_separator" = ",", '
                '"line_delimiter" = "\\n"'
            )

        wrapped = (
            f"{inner}\n"
            f'INTO OUTFILE "file://{prefix_in_container}"\n'
            f"FORMAT AS {format_as}\n"
            f"PROPERTIES ({props})"
        )

        conn = self._connect()
        try:
            cur = conn.cursor()
            try:
                cur.execute("SELECT CONNECTION_ID()")
                row = cur.fetchone()
                conn_id = int(row[0]) if row else None
                on_handle(ExecutionHandle(connection_id=conn_id))

                # StarRocks requires 1 <= query_timeout <= 259200 (72h).
                # setup-jobs enforces that range on the happy path; this
                # check guards against direct edits to config/jobs/config.yaml
                # that drift outside the valid range.
                sr_timeout = int(timeout_seconds)
                if sr_timeout < 1 or sr_timeout > _SR_QUERY_TIMEOUT_MAX:
                    raise BackendError(
                        f"timeout_seconds={sr_timeout} is out of StarRocks' valid range "
                        f"(1..{_SR_QUERY_TIMEOUT_MAX}, i.e. 1s to 72h). "
                        f"Run `sdp db setup-jobs` to set a valid timeout, or edit "
                        f"default_timeout_seconds in config/jobs/config.yaml."
                    )
                cur.execute(f"SET query_timeout = {sr_timeout}")

                try:
                    cur.execute(wrapped)
                    # INTO OUTFILE returns a single-row summary; drain it so we
                    # can read rows affected / file count consistently.
                    try:
                        summary = cur.fetchone()
                    except MySQLError:
                        summary = None
                except MySQLError as e:
                    raise BackendError(str(e)) from e
            finally:
                try:
                    cur.close()
                except Exception:
                    pass
        finally:
            try:
                conn.close()
            except Exception:
                pass

        size = dir_size_bytes(job_dir)
        rows = _rows_from_sr_outfile_summary(summary) if summary else None

        return ExecutionResult(
            rows=rows,
            size_bytes=size,
            result_path=str(job_dir),
        )

    def explain(self, sql: str) -> str:
        inner = strip_trailing_semicolon(sql)
        conn = self._connect()
        try:
            cur = conn.cursor()
            try:
                cur.execute("SET query_timeout = 10")
                try:
                    cur.execute(f"EXPLAIN {inner}")
                    rows = cur.fetchall()
                except MySQLError as e:
                    raise BackendError(str(e)) from e
            finally:
                try:
                    cur.close()
                except Exception:
                    pass
        finally:
            try:
                conn.close()
            except Exception:
                pass
        return "\n".join(str(r[0]) for r in rows)

    def cancel(self, handle: ExecutionHandle) -> None:
        if not handle.connection_id:
            return
        conn = self._connect()
        try:
            cur = conn.cursor()
            try:
                cur.execute(f"KILL QUERY {int(handle.connection_id)}")
                try:
                    cur.fetchall()
                except MySQLError:
                    pass
            finally:
                try:
                    cur.close()
                except Exception:
                    pass
        finally:
            try:
                conn.close()
            except Exception:
                pass

    # ------------------------------------------------------------------

    def _connect(self):
        params = dict(host=self.host, port=self.port, user="root")
        if auth_enabled("starrocks"):
            pw = admin_password("starrocks")
            if not pw:
                raise BackendError(
                    "STARROCKS_ROOT_PASSWORD is not set in the jobs container "
                    "environment but STARROCKS_AUTH_ENABLED=true"
                )
            params["password"] = pw
        if self.database:
            params["database"] = self.database
        return mysql.connector.connect(**params)


def _rows_from_sr_outfile_summary(row: tuple) -> int | None:
    """SR's INTO OUTFILE returns a row like (FileNumber, TotalRows, FileSize, URL).
    Best-effort extraction; None if the shape is unexpected."""
    if not row:
        return None
    try:
        if len(row) >= 2 and isinstance(row[1], int):
            return int(row[1])
    except (TypeError, ValueError):
        pass
    return None
