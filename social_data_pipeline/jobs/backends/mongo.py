"""MongoDB backend adapter.

Agent submits a collection + aggregation pipeline (JSON array of stages).
The runner streams cursor output to a single file (NDJSON or CSV) inside
the job folder. Mongo has no server-side local-file export, so the
writing happens in the runner process — same pattern as ``mongoexport``.

Cancellation uses a ``comment`` tag on the aggregation: the runner sets
``comment=sdp-jobs-<job_id>`` when starting the cursor and, on cancel,
opens a second admin connection, scans ``db.currentOp()``, and calls
``killOp`` for any matching operation. This avoids racing ``currentOp``
at start to capture an ``opid`` before the cursor has been seen.
"""

from __future__ import annotations

import csv
import datetime
import json
import os
import shutil
from pathlib import Path
from typing import Any

from pymongo import MongoClient
from pymongo.errors import ExecutionTimeout, OperationFailure, PyMongoError

from ..config import admin_password, auth_enabled
from ..store import Job
from .base import (
    Backend,
    BackendError,
    ExecutionHandle,
    ExecutionResult,
    HandleCallback,
    validate_filename,
)


_MONGO_EXTS = frozenset({".ndjson", ".csv"})


def _cancel_comment(job_id: str) -> str:
    return f"sdp-jobs-{job_id}"


class MongoBackend:
    name = "mongodb"

    def __init__(
        self,
        result_root: Path,
        host: str = "mongo",
        port: int | None = None,
    ):
        self.result_root = Path(result_root)
        self.host = host
        self.port = int(port or os.environ.get("MONGO_PORT", 27017))

    # ------------------------------------------------------------------

    def validate(self, job: Job) -> None:
        validate_filename(job.output_filename, allowed_exts=_MONGO_EXTS)
        _pipeline_from_job(job)
        _collection_from_job(job)
        if not (job.database or "").strip():
            raise BackendError(
                "mongo job has no database — submit with `database=<name>`."
            )

    def execute(
        self,
        job: Job,
        timeout_seconds: int,
        on_handle: HandleCallback,
    ) -> ExecutionResult:
        ext = validate_filename(job.output_filename, allowed_exts=_MONGO_EXTS)
        pipeline = _pipeline_from_job(job)
        collection = _collection_from_job(job)

        job_dir = self.result_root / job.job_id
        if job.overwrite and job_dir.exists():
            shutil.rmtree(job_dir)
        if job_dir.exists():
            raise BackendError(
                f"result folder already exists: {job_dir}. "
                "Resubmit with overwrite=True or use a different job id."
            )
        job_dir.mkdir(parents=True, exist_ok=False)
        output_path = job_dir / job.output_filename

        # Handle has no backend_pid/connection_id for Mongo; cancel uses the
        # comment tag instead. Publish the handle so the running/<id>.json
        # record is consistent with the other backends.
        on_handle(ExecutionHandle())

        database = (job.database or "").strip()
        if not database:
            raise BackendError(
                "mongo job has no database — submit with `database=<name>`."
            )

        client = self._client()
        try:
            db = client[database]

            agg_kwargs: dict[str, Any] = {
                "allowDiskUse": True,
                "comment": _cancel_comment(job.job_id),
            }
            if timeout_seconds and int(timeout_seconds) > 0:
                agg_kwargs["maxTimeMS"] = int(timeout_seconds) * 1000

            try:
                cursor = db[collection].aggregate(pipeline, **agg_kwargs)
            except OperationFailure as e:
                raise BackendError(f"aggregation rejected: {e}") from e
            except PyMongoError as e:
                raise BackendError(f"mongo driver error: {e}") from e

            try:
                if ext == ".ndjson":
                    rows = _write_ndjson(cursor, output_path)
                else:
                    rows = _write_csv(cursor, output_path)
            except ExecutionTimeout as e:
                raise BackendError(f"aggregation exceeded maxTimeMS: {e}") from e
            except OperationFailure as e:
                raise BackendError(f"aggregation error: {e}") from e
            except PyMongoError as e:
                raise BackendError(f"mongo driver error: {e}") from e
        finally:
            try:
                client.close()
            except Exception:
                pass

        size = output_path.stat().st_size if output_path.exists() else 0
        return ExecutionResult(
            rows=rows,
            size_bytes=size,
            result_path=str(job_dir),
        )

    def explain(self, sql: str, job: Job | None = None) -> str:
        """EXPLAIN for a mongo aggregation.

        ``sql`` here is the same JSON payload the submit flow stored in
        ``job.sql`` — a pretty-printed ``{"collection": ..., "pipeline": ...}``
        object. Returns a pretty-printed JSON plan. Needs the job for the
        database (may differ from the target's default).
        """
        try:
            payload = json.loads(sql)
        except json.JSONDecodeError as e:
            raise BackendError(f"mongo job payload is not valid JSON: {e}") from e
        collection = payload.get("collection")
        pipeline = payload.get("pipeline")
        if not collection or not isinstance(pipeline, list):
            raise BackendError("mongo payload must have 'collection' and 'pipeline'")

        database = (job.database if job else "") or ""
        database = database.strip()
        if not database:
            # EXPLAIN before submit: fall back to the payload's stashed db.
            try:
                database = (json.loads(sql) or {}).get("database") or ""
            except (json.JSONDecodeError, TypeError):
                database = ""
            database = (database or "").strip()
        if not database:
            raise BackendError("cannot EXPLAIN: mongo job has no database")

        client = self._client()
        try:
            db = client[database]
            try:
                # queryPlanner verbosity does NOT execute the aggregation —
                # same cost class as PG/SR EXPLAIN. maxTimeMS is a safety
                # bound matching the 10s limit used in the other adapters.
                plan = db.command({
                    "explain": {
                        "aggregate": collection,
                        "pipeline": pipeline,
                        "cursor": {},
                    },
                    "verbosity": "queryPlanner",
                    "maxTimeMS": 10_000,
                })
            except OperationFailure as e:
                raise BackendError(str(e)) from e
            except PyMongoError as e:
                raise BackendError(f"mongo driver error: {e}") from e
        finally:
            try:
                client.close()
            except Exception:
                pass
        return json.dumps(plan, indent=2, default=_json_default)

    def list_databases(self) -> list[dict]:
        """Return the list of databases visible to the admin connection.

        Each entry is ``{"name": str, "sizeOnDisk": int, "empty": bool}``.
        Used by the ``list_mongo_databases`` MCP tool so agents can discover
        what databases are reachable from a target.
        """
        client = self._client()
        try:
            try:
                result = list(client.list_databases())
            except OperationFailure as e:
                raise BackendError(str(e)) from e
            except PyMongoError as e:
                raise BackendError(f"mongo driver error: {e}") from e
        finally:
            try:
                client.close()
            except Exception:
                pass
        # Filter out internal databases agents shouldn't care about.
        return [
            {
                "name": d.get("name"),
                "sizeOnDisk": d.get("sizeOnDisk"),
                "empty": d.get("empty", False),
            }
            for d in result
            if d.get("name") not in ("admin", "config", "local")
        ]

    def cancel(self, handle: ExecutionHandle, job: Job | None = None) -> None:
        if job is None:
            return
        comment = _cancel_comment(job.job_id)
        client = self._client()
        try:
            admin = client.admin
            try:
                result = admin.command("currentOp", {"active": True})
            except OperationFailure:
                return
            for op in result.get("inprog", []):
                if op.get("comment") == comment:
                    opid = op.get("opid")
                    if opid is None:
                        continue
                    try:
                        admin.command("killOp", op=opid)
                    except OperationFailure:
                        pass
        finally:
            try:
                client.close()
            except Exception:
                pass

    # ------------------------------------------------------------------

    def _client(self) -> MongoClient:
        if auth_enabled("mongodb"):
            pw = admin_password("mongodb")
            if not pw:
                raise BackendError(
                    "MONGO_ADMIN_PASSWORD is not set in the jobs container "
                    "environment but MONGO_AUTH_ENABLED=true"
                )
            return MongoClient(
                host=self.host,
                port=self.port,
                username="admin",
                password=pw,
                authSource="admin",
                serverSelectionTimeoutMS=5000,
            )
        return MongoClient(
            host=self.host,
            port=self.port,
            serverSelectionTimeoutMS=5000,
        )


# ---------------------------------------------------------------------------
# Writers


def _write_ndjson(cursor, output_path: Path) -> int:
    rows = 0
    with open(output_path, "w") as f:
        for doc in cursor:
            f.write(json.dumps(doc, default=_json_default) + "\n")
            rows += 1
    return rows


def _write_csv(cursor, output_path: Path) -> int:
    iterator = iter(cursor)
    try:
        first = next(iterator)
    except StopIteration:
        output_path.touch()
        return 0

    columns = sorted(first.keys())
    _validate_flat_scalars(first, columns)

    rows = 0
    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerow({k: _stringify_scalar(first.get(k)) for k in columns})
        rows = 1
        for doc in iterator:
            doc_keys = set(doc.keys())
            extra = doc_keys - set(columns)
            if extra:
                raise BackendError(
                    f"CSV output requires a uniform schema; doc {rows + 1} "
                    f"introduces new keys: {sorted(extra)}. "
                    "Use .ndjson for heterogeneous aggregation output."
                )
            _validate_flat_scalars(doc, columns)
            writer.writerow({k: _stringify_scalar(doc.get(k)) for k in columns})
            rows += 1
    return rows


def _validate_flat_scalars(doc: dict, columns) -> None:
    for k in columns:
        v = doc.get(k)
        if isinstance(v, (dict, list, tuple, set)):
            raise BackendError(
                f"CSV output requires flat scalars; field {k!r} contains a "
                f"{type(v).__name__}. Use .ndjson for nested output, or add a "
                "$project stage that flattens the document."
            )


def _stringify_scalar(v: Any) -> Any:
    # csv.DictWriter handles primitive types; coerce ObjectId / datetime to
    # strings explicitly so the output is CSV-friendly.
    if v is None or isinstance(v, (str, int, float, bool)):
        return v
    if isinstance(v, datetime.datetime):
        return v.isoformat()
    return str(v)


def _json_default(o: Any) -> Any:
    """JSON encoder fallback for BSON types (ObjectId, datetime, etc.)."""
    if isinstance(o, datetime.datetime):
        return o.isoformat()
    return str(o)


# ---------------------------------------------------------------------------
# Payload helpers

def _pipeline_from_job(job: Job) -> list:
    try:
        payload = json.loads(job.sql) if job.sql else {}
    except json.JSONDecodeError as e:
        raise BackendError(f"mongo job payload is not valid JSON: {e}") from e
    pipeline = payload.get("pipeline") if isinstance(payload, dict) else None
    if not isinstance(pipeline, list):
        raise BackendError("mongo job is missing 'pipeline' (must be a JSON array)")
    return pipeline


def _collection_from_job(job: Job) -> str:
    # Collection is stored both on the job record and inside the payload;
    # the record is authoritative. Fall back to the payload for robustness.
    if job.collection:
        return job.collection
    try:
        payload = json.loads(job.sql) if job.sql else {}
    except json.JSONDecodeError:
        payload = {}
    coll = payload.get("collection") if isinstance(payload, dict) else None
    if not coll:
        raise BackendError("mongo job is missing 'collection'")
    return coll
