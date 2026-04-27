"""PostgreSQL backend adapter.

Agent submits a plain SELECT. Runner wraps it as:

    COPY (<sql>) TO '/jobs_export/<job_id>/<output_filename>'
    WITH (FORMAT parquet)   -- or (FORMAT csv, HEADER true)

The DB server writes directly to the bind-mounted results folder
(host path: <result_root>/<job_id>/<filename>). Runner reads the size
afterwards.
"""

from __future__ import annotations

import os
from pathlib import Path

import psycopg

from ..config import admin_password, auth_enabled
from ..store import Job
from .base import (
    JOBS_EXPORT_CONTAINER_PATH,
    BackendError,
    ExecutionHandle,
    ExecutionResult,
    HandleCallback,
    dir_size_bytes,
    strip_trailing_semicolon,
    validate_filename,
)


class PostgresBackend:
    name = "postgres"

    def __init__(self, result_root: Path, database: str, host: str = "postgres", port: int | None = None):
        self.result_root = Path(result_root)
        self.database = database
        self.host = host
        self.port = int(port or os.environ.get("POSTGRES_PORT", 5432))

    # ------------------------------------------------------------------

    def validate(self, job: Job) -> None:
        validate_filename(job.output_filename)
        strip_trailing_semicolon(job.sql)  # raises on empty

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
            import shutil
            shutil.rmtree(job_dir)
        if job_dir.exists():
            raise BackendError(
                f"result folder already exists: {job_dir}. "
                "Resubmit with overwrite=True or use a different job id."
            )
        job_dir.mkdir(parents=True, exist_ok=False)

        container_path = f"{JOBS_EXPORT_CONTAINER_PATH}/{job.job_id}/{job.output_filename}"

        if ext == ".parquet":
            format_clause = "FORMAT parquet"
        else:
            format_clause = "FORMAT csv, HEADER true, DELIMITER ','"

        wrapped = (
            f"COPY (\n{inner}\n) "
            f"TO '{container_path}' WITH ({format_clause})"
        )

        conn = self._connect()
        try:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("SELECT pg_backend_pid()")
                row = cur.fetchone()
                pid = int(row[0]) if row else None
                on_handle(ExecutionHandle(backend_pid=pid))

                cur.execute(f"SET statement_timeout = {int(timeout_seconds) * 1000}")

                try:
                    cur.execute(wrapped)
                except psycopg.errors.QueryCanceled as e:
                    raise BackendError(f"query cancelled: {e}") from e
                except psycopg.Error as e:
                    raise BackendError(str(e)) from e

                rows = cur.rowcount if cur.rowcount and cur.rowcount >= 0 else None
        finally:
            try:
                conn.close()
            except Exception:
                pass

        host_output = job_dir / job.output_filename
        size = host_output.stat().st_size if host_output.exists() else dir_size_bytes(job_dir)

        return ExecutionResult(
            rows=rows,
            size_bytes=size,
            result_path=str(job_dir),
        )

    def cancel(self, handle: ExecutionHandle, job: Job | None = None) -> None:
        if not handle.backend_pid:
            return
        conn = self._connect()
        try:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("SELECT pg_terminate_backend(%s)", (handle.backend_pid,))
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def explain(self, sql: str) -> str:
        inner = strip_trailing_semicolon(sql)
        conn = self._connect()
        try:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("SET statement_timeout = 10000")
                try:
                    cur.execute(f"EXPLAIN {inner}")
                except psycopg.Error as e:
                    raise BackendError(str(e)) from e
                rows = cur.fetchall()
        finally:
            try:
                conn.close()
            except Exception:
                pass
        return "\n".join(r[0] for r in rows)

    # ------------------------------------------------------------------

    def _connect(self) -> psycopg.Connection:
        params: dict = dict(
            host=self.host,
            port=self.port,
            user="postgres",
            dbname=self.database,
        )
        if auth_enabled("postgres"):
            pw = admin_password("postgres")
            if not pw:
                raise BackendError(
                    "POSTGRES_PASSWORD is not set in the jobs container environment "
                    "but POSTGRES_AUTH_ENABLED=true"
                )
            params["password"] = pw
        return psycopg.connect(**params)
