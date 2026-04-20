"""MCP tools exposed by the scheduler. Built dynamically from JobsConfig so
unconfigured backends' tools don't appear in discovery."""

from __future__ import annotations

import json
import logging
import time
from typing import Any

from mcp.server.fastmcp import FastMCP

try:
    from mcp.server.transport_security import TransportSecuritySettings
except ImportError:  # older mcp SDK
    TransportSecuritySettings = None  # type: ignore[assignment]

from .config import JobsConfig
from .runner import Runner
from .store import Job, Store


log = logging.getLogger(__name__)


def build_mcp(cfg: JobsConfig, store: Store, runner: Runner) -> FastMCP:
    """Assemble the FastMCP app for this process.

    Submit tools are only registered for backends with at least one configured
    target. The target name is validated against the live config, not baked
    into the tool schema — tool docstrings enumerate the configured names so
    agents see them during discovery.
    """
    # streamable_http_path="/" makes FastMCP's Starlette app serve the
    # streamable-HTTP endpoint at its root; combined with FastAPI mounting
    # the app at /mcp, the client-visible URL is exactly /mcp.
    #
    # transport_security: disable DNS-rebinding protection so remote hosts
    # can connect. The jobs scheduler binds 0.0.0.0 by design (local-network
    # MCP access); the SDK's default "only localhost" policy would reject
    # them with 421 Misdirected Request.
    mcp_kwargs: dict = {"name": "sdp-jobs", "streamable_http_path": "/"}
    if TransportSecuritySettings is not None:
        mcp_kwargs["transport_security"] = TransportSecuritySettings(
            enable_dns_rebinding_protection=False,
        )

    try:
        mcp = FastMCP(**mcp_kwargs)
    except TypeError:
        # Older mcp SDK that doesn't accept one of these kwargs — fall back
        # and set them on the settings object where possible.
        mcp = FastMCP(name="sdp-jobs")
        try:
            mcp.settings.streamable_http_path = "/"
        except AttributeError:
            log.warning(
                "mcp SDK version does not expose streamable_http_path; "
                "MCP endpoint may be served at /mcp/mcp instead of /mcp"
            )
        if TransportSecuritySettings is not None:
            try:
                mcp.settings.transport_security = TransportSecuritySettings(
                    enable_dns_rebinding_protection=False,
                )
            except AttributeError:
                log.warning(
                    "mcp SDK does not expose transport_security settings; "
                    "remote clients may be rejected with 421 Misdirected Request"
                )

    pg_targets = [t.name for t in cfg.targets_for("postgres")]
    sr_targets = [t.name for t in cfg.targets_for("starrocks")]
    mongo_targets = [t.name for t in cfg.targets_for("mongodb")]

    if pg_targets:
        _register_submit_postgres(mcp, cfg, store, pg_targets)
    if sr_targets:
        _register_submit_starrocks(mcp, cfg, store, sr_targets)
    if mongo_targets:
        _register_submit_mongo(mcp, cfg, store, mongo_targets)
        _register_list_mongo_databases(mcp, cfg, runner, mongo_targets)

    _register_status_tool(mcp, store)
    _register_cancel_tool(mcp, store)

    return mcp


# ----------------------------------------------------------------------------
# submit_postgres_query

def _register_submit_postgres(
    mcp: FastMCP, cfg: JobsConfig, store: Store, targets: list[str]
) -> None:
    targets_csv = ", ".join(repr(t) for t in targets)

    @mcp.tool(
        name="submit_postgres_query",
        description=(
            "Queue a SQL SELECT for execution against a PostgreSQL target. "
            "The runner wraps the query as "
            "`COPY (<sql>) TO '/jobs_export/<job_id>/<output_filename>' "
            "WITH (FORMAT parquet|csv)`; the PG server writes the result file "
            "directly. The result folder contains one file. "
            "Include a short `description` (1-2 sentences) explaining what "
            "the query is for — the human approver reads it to decide. "
            "Queries must be approved by a human in the web UI before they "
            "execute.\n\n"
            f"Configured targets: {targets_csv}."
        ),
    )
    def submit_postgres_query(
        sql: str,
        target: str,
        output_filename: str,
        description: str = "",
        overwrite: bool = False,
    ) -> dict[str, Any]:
        return _submit(
            store=store,
            cfg=cfg,
            backend="postgres",
            target=target,
            sql=sql,
            output_filename=output_filename,
            description=description,
            overwrite=overwrite,
        )


# ----------------------------------------------------------------------------
# submit_starrocks_query

def _register_submit_starrocks(
    mcp: FastMCP, cfg: JobsConfig, store: Store, targets: list[str]
) -> None:
    targets_csv = ", ".join(repr(t) for t in targets)

    @mcp.tool(
        name="submit_starrocks_query",
        description=(
            "Queue a SQL SELECT for execution against a StarRocks target. "
            "The runner appends "
            "`INTO OUTFILE \"file:///jobs_export/<job_id>/<stem>_\" FORMAT "
            "AS PARQUET|CSV ...` to the submitted SELECT, where <stem> is "
            "output_filename without its extension. SR chunks the output, "
            "so the result folder contains `<stem>_0.<ext>`, `<stem>_1.<ext>`, "
            "… (readable as a single dataset by pyarrow / polars / DuckDB). "
            "The SR target has no default database — queries must "
            "fully-qualify table names as `<database>.<table>`. "
            "Include a short `description` (1-2 sentences) explaining what "
            "the query is for — the human approver reads it to decide. "
            "Queries must be approved by a human in the web UI before they "
            "execute.\n\n"
            f"Configured targets: {targets_csv}."
        ),
    )
    def submit_starrocks_query(
        sql: str,
        target: str,
        output_filename: str,
        description: str = "",
        overwrite: bool = False,
    ) -> dict[str, Any]:
        return _submit(
            store=store,
            cfg=cfg,
            backend="starrocks",
            target=target,
            sql=sql,
            output_filename=output_filename,
            description=description,
            overwrite=overwrite,
        )


# ----------------------------------------------------------------------------
# submit_mongo_query

def _register_submit_mongo(
    mcp: FastMCP, cfg: JobsConfig, store: Store, targets: list[str]
) -> None:
    targets_csv = ", ".join(repr(t) for t in targets)

    @mcp.tool(
        name="submit_mongo_query",
        description=(
            "Queue a MongoDB aggregation for execution against a MongoDB "
            "target. A target is a Mongo node (not a single database) — "
            "agents always pass an explicit `database` on every submission. "
            "Call `list_mongo_databases(target)` first to discover what "
            "databases are available. "
            "`pipeline` is a JSON array of aggregation stages (a plain "
            "`find()` is expressed as a single `$match`, optionally followed "
            "by `$project` / `$sort` / `$limit`). "
            "`collection` is the collection within the chosen database. "
            "The runner streams the cursor output into "
            "`/data/jobs/results/<job_id>/<output_filename>`. Format is "
            "chosen by the output_filename extension: `.ndjson` (safe "
            "default; lossless) or `.csv` (requires a terminal `$project` "
            "producing flat scalars — fails fast otherwise). "
            "Include a short `description` (1-2 sentences) explaining what "
            "the query is for — the human approver reads it to decide. "
            "Queries must be approved by a human in the web UI before they "
            "execute.\n\n"
            f"Configured targets: {targets_csv}."
        ),
    )
    def submit_mongo_query(
        target: str,
        database: str,
        collection: str,
        pipeline: list[dict],
        output_filename: str,
        description: str = "",
        overwrite: bool = False,
    ) -> dict[str, Any]:
        return _submit_mongo(
            store=store,
            cfg=cfg,
            target=target,
            collection=collection,
            pipeline=pipeline,
            output_filename=output_filename,
            database=database,
            description=description,
            overwrite=overwrite,
        )


# ----------------------------------------------------------------------------
# list_mongo_databases

def _register_list_mongo_databases(
    mcp: FastMCP, cfg: JobsConfig, runner: Runner, targets: list[str]
) -> None:
    targets_csv = ", ".join(repr(t) for t in targets)

    @mcp.tool(
        name="list_mongo_databases",
        description=(
            "List the databases visible to a MongoDB target. Use this to "
            "discover which database to pass as `database=` on "
            "`submit_mongo_query`. Internal databases (admin, config, "
            "local) are filtered out. Requires admin read access.\n\n"
            f"Configured targets: {targets_csv}."
        ),
    )
    def list_mongo_databases(target: str) -> dict[str, Any]:
        tgt = cfg.targets.get(target)
        if tgt is None or tgt.backend != "mongodb":
            return {
                "error": (
                    f"{target!r} is not a configured mongodb target; "
                    f"configured: {targets}"
                )
            }
        try:
            return {"databases": runner.list_mongo_databases(target)}
        except Exception as e:
            return {"error": f"{type(e).__name__}: {e}"}


# ----------------------------------------------------------------------------
# query_status

def _register_status_tool(mcp: FastMCP, store: Store) -> None:
    @mcp.tool(
        name="query_status",
        description=(
            "Return the current status of a queued query. Status values: "
            "pending, approved, running, done, failed, rejected, cancelled. "
            "When status=done, `result_path` points at a folder containing "
            "the result file(s)."
        ),
    )
    def query_status(job_id: str) -> dict[str, Any]:
        located = store.find(job_id)
        if not located:
            return {"job_id": job_id, "status": "unknown", "error": "job not found"}
        phase, job = located
        return _job_to_status(job, phase=phase)


# ----------------------------------------------------------------------------
# query_cancel

def _register_cancel_tool(mcp: FastMCP, store: Store) -> None:
    @mcp.tool(
        name="query_cancel",
        description=(
            "Cancel a pending job (one that has not yet been approved). "
            "Already-running jobs must be killed from the web UI — the "
            "scheduler does not expose a kill-via-MCP path."
        ),
    )
    def query_cancel(job_id: str) -> dict[str, Any]:
        located = store.find(job_id)
        if not located:
            return {"job_id": job_id, "status": "unknown", "error": "job not found"}
        phase, _ = located
        if phase != "pending":
            return {
                "job_id": job_id,
                "status": phase,
                "error": f"cannot cancel via MCP while in phase {phase!r}",
            }
        job = store.cancel_pending(job_id)
        return _job_to_status(job, phase="history")


# ----------------------------------------------------------------------------
# shared helpers

def _submit(
    *,
    store: Store,
    cfg: JobsConfig,
    backend: str,
    target: str,
    sql: str,
    output_filename: str,
    overwrite: bool,
    description: str = "",
) -> dict[str, Any]:
    tgt = cfg.targets.get(target)
    if tgt is None:
        return {
            "error": (
                f"unknown target {target!r}; "
                f"configured: {sorted(cfg.targets)}"
            )
        }
    if tgt.backend != backend:
        return {
            "error": (
                f"target {target!r} is backend={tgt.backend!r}, "
                f"not {backend!r}; use the matching submit tool"
            )
        }

    job = Job(
        job_id=store.new_job_id(backend),
        target=target,
        backend=backend,
        sql=sql,
        output_filename=output_filename,
        overwrite=bool(overwrite),
        submitted_at=time.time(),
        description=(description or "").strip(),
        status="pending",
    )
    try:
        store.submit(job)
    except OSError as e:
        return {"error": f"failed to queue job: {e}"}
    log.info("submitted job %s target=%s backend=%s", job.job_id, target, backend)
    return {"job_id": job.job_id, "status": "pending"}


def _submit_mongo(
    *,
    store: Store,
    cfg: JobsConfig,
    target: str,
    collection: str,
    pipeline: list[dict],
    output_filename: str,
    overwrite: bool,
    database: str = "",
    description: str = "",
) -> dict[str, Any]:
    tgt = cfg.targets.get(target)
    if tgt is None:
        return {
            "error": (
                f"unknown target {target!r}; "
                f"configured: {sorted(cfg.targets)}"
            )
        }
    if tgt.backend != "mongodb":
        return {
            "error": (
                f"target {target!r} is backend={tgt.backend!r}, not 'mongodb'; "
                "use the matching submit tool"
            )
        }
    if not isinstance(pipeline, list):
        return {"error": "pipeline must be a JSON array of aggregation stages"}
    if not collection or not isinstance(collection, str):
        return {"error": "collection must be a non-empty string"}

    resolved_db = (database or "").strip()
    if not resolved_db:
        return {
            "error": (
                "database is required for Mongo submissions; call "
                "list_mongo_databases(target) to see what's available."
            )
        }

    # Store the aggregation as pretty-printed JSON in `sql` so the UI can
    # render it and the backend can decode it. `collection` and `database`
    # are also set on the job record for display + execution.
    payload = json.dumps(
        {
            "collection": collection,
            "database": resolved_db,
            "pipeline": pipeline,
        },
        indent=2,
        default=str,
    )

    job = Job(
        job_id=store.new_job_id("mongodb"),
        target=target,
        backend="mongodb",
        sql=payload,
        output_filename=output_filename,
        overwrite=bool(overwrite),
        submitted_at=time.time(),
        description=(description or "").strip(),
        collection=collection,
        database=resolved_db,
        status="pending",
    )
    try:
        store.submit(job)
    except OSError as e:
        return {"error": f"failed to queue job: {e}"}
    log.info(
        "submitted mongo job %s target=%s db=%s collection=%s",
        job.job_id, target, resolved_db, collection,
    )
    return {"job_id": job.job_id, "status": "pending"}


def _job_to_status(job: Job, *, phase: str) -> dict[str, Any]:
    return {
        "job_id": job.job_id,
        "status": job.status,
        "target": job.target,
        "backend": job.backend,
        "submitted_at": job.submitted_at,
        "approved_at": job.approved_at,
        "started_at": job.started_at,
        "finished_at": job.finished_at,
        "rows": job.rows,
        "size_bytes": job.size_bytes,
        "result_path": job.result_path,
        "error": job.error,
        "reject_reason": job.reject_reason,
        "phase": phase,
    }
