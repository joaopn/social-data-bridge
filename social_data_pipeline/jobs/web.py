"""Web UI routes: Pending / Running / History + approve/reject/kill actions."""

from __future__ import annotations

import logging
import time
from pathlib import Path

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from .config import JobsConfig
from .runner import Runner
from .store import Store


log = logging.getLogger(__name__)


TEMPLATES_DIR = Path(__file__).parent / "templates"


def build_router(cfg: JobsConfig, store: Store, runner: Runner) -> APIRouter:
    templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
    templates.env.globals["relative_time"] = _relative_time
    templates.env.globals["absolute_time"] = _absolute_time
    templates.env.globals["duration"] = _duration
    templates.env.globals["human_bytes"] = _human_bytes
    templates.env.globals["sql_preview"] = _sql_preview
    templates.env.globals["host_path"] = _make_host_path_translator(cfg)

    router = APIRouter()

    def _counts() -> dict[str, int]:
        return {
            "pending": len(list(store.pending.glob("*.json"))),
            "approved": len(list(store.approved.glob("*.json"))),
            "running": len(list(store.running.glob("*.json"))),
        }

    @router.get("/", response_class=HTMLResponse)
    async def root():
        return RedirectResponse(url="/pending", status_code=302)

    @router.get("/pending", response_class=HTMLResponse)
    async def pending(request: Request):
        jobs = store.list_phase("pending")
        return templates.TemplateResponse(
            request=request,
            name="pending.html",
            context={
                "tab": "pending",
                "jobs": jobs,
                "counts": _counts(),
            },
        )

    @router.get("/running", response_class=HTMLResponse)
    async def running(request: Request):
        return templates.TemplateResponse(
            request=request,
            name="running.html",
            context={
                "tab": "running",
                "running": store.list_phase("running"),
                "approved": store.list_phase("approved"),
                "now": time.time(),
                "counts": _counts(),
            },
        )

    @router.get("/fragments/running", response_class=HTMLResponse)
    async def running_fragment(request: Request):
        return templates.TemplateResponse(
            request=request,
            name="_running_rows.html",
            context={
                "running": store.list_phase("running"),
                "approved": store.list_phase("approved"),
                "now": time.time(),
            },
        )

    @router.get("/history", response_class=HTMLResponse)
    async def history(request: Request):
        jobs = store.iter_history(limit=cfg.history_retention)
        return templates.TemplateResponse(
            request=request,
            name="history.html",
            context={
                "tab": "history",
                "jobs": jobs,
                "retention": cfg.history_retention,
                "counts": _counts(),
            },
        )

    @router.post("/actions/approve/{job_id}")
    async def approve(job_id: str):
        try:
            store.approve(job_id)
            log.info("approved %s via web UI", job_id)
        except KeyError:
            log.warning("approve: %s not pending", job_id)
        return RedirectResponse(url="/pending", status_code=303)

    @router.post("/actions/reject/{job_id}")
    async def reject(job_id: str, reason: str = Form(default="")):
        try:
            store.reject(job_id, reason=(reason.strip() or None))
            log.info("rejected %s via web UI", job_id)
        except KeyError:
            log.warning("reject: %s not pending", job_id)
        return RedirectResponse(url="/pending", status_code=303)

    @router.post("/actions/kill/{job_id}")
    async def kill(job_id: str):
        ok = runner.request_cancel(job_id)
        if not ok:
            log.warning("kill: %s not currently running", job_id)
        return RedirectResponse(url="/running", status_code=303)

    return router


# ----------------------------------------------------------------------------
# Formatting helpers (registered as Jinja globals).


def _relative_time(ts: float | None) -> str:
    if not ts:
        return "—"
    delta = max(0.0, time.time() - ts)
    return _duration(delta) + " ago"


def _absolute_time(ts: float | None) -> str:
    if not ts:
        return "—"
    lt = time.localtime(ts)
    return time.strftime("%Y-%m-%d %H:%M:%S", lt)


def _duration(seconds: float | None) -> str:
    if seconds is None:
        return "—"
    seconds = max(0.0, float(seconds))
    if seconds < 60:
        return f"{seconds:.0f}s"
    if seconds < 3600:
        m, s = divmod(int(seconds), 60)
        return f"{m}m {s}s"
    if seconds < 86400:
        h, rem = divmod(int(seconds), 3600)
        m = rem // 60
        return f"{h}h {m}m"
    d, rem = divmod(int(seconds), 86400)
    h = rem // 3600
    return f"{d}d {h}h"


def _human_bytes(n: int | None) -> str:
    if n is None:
        return "—"
    n = int(n)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if n < 1024 or unit == "TiB":
            return f"{n:.1f} {unit}" if unit != "B" else f"{n} B"
        n /= 1024
    return f"{n:.1f} TiB"


def _sql_preview(sql: str, width: int = 90) -> str:
    flat = " ".join(sql.split())
    if len(flat) <= width:
        return flat
    return flat[: width - 1] + "…"


def _make_host_path_translator(cfg: JobsConfig):
    """Return a Jinja helper that maps container paths (as stored in job
    records) to the equivalent host path the user can open directly."""
    container_root = str(cfg.result_root).rstrip("/")
    host_root = (cfg.host_result_root or container_root).rstrip("/")

    def host_path(p: str | None) -> str:
        if not p:
            return ""
        if p.startswith(container_root):
            return host_root + p[len(container_root):]
        return p

    return host_path
