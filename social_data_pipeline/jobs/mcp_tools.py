"""MCP tools exposed by the scheduler. Built dynamically from JobsConfig so
unconfigured backends' tools don't appear in discovery."""

from __future__ import annotations

import logging
import time
from typing import Any

from mcp.server.fastmcp import FastMCP

try:
    from mcp.server.transport_security import TransportSecuritySettings
except ImportError:  # older mcp SDK
    TransportSecuritySettings = None  # type: ignore[assignment]

from .config import JobsConfig
from .store import Job, Store


log = logging.getLogger(__name__)


def build_mcp(cfg: JobsConfig, store: Store) -> FastMCP:
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

    if pg_targets:
        _register_submit_postgres(mcp, cfg, store, pg_targets)
    if sr_targets:
        _register_submit_starrocks(mcp, cfg, store, sr_targets)

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
