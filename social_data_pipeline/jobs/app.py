"""FastAPI app factory: wires UI, MCP, runner together in one process."""

from __future__ import annotations

import contextlib
import logging
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from .config import JobsConfig, load_config
from .mcp_tools import build_mcp
from .runner import Runner
from .store import Store
from .web import build_router


log = logging.getLogger(__name__)


STATIC_DIR = Path(__file__).parent / "static"


def build_app(cfg: JobsConfig | None = None) -> FastAPI:
    if cfg is None:
        cfg = load_config()

    store = Store(cfg.jobs_dir)
    runner = Runner(cfg, store)
    mcp = build_mcp(cfg, store, runner)

    @contextlib.asynccontextmanager
    async def lifespan(app: FastAPI):
        # Start MCP session manager + background runner for the lifetime of
        # the ASGI application.
        async with mcp.session_manager.run():
            runner.start()
            try:
                yield
            finally:
                runner.stop()

    app = FastAPI(title="SDP Jobs", lifespan=lifespan)

    STATIC_DIR.mkdir(parents=True, exist_ok=True)
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

    app.mount("/mcp", mcp.streamable_http_app())

    app.include_router(build_router(cfg, store, runner))

    app.state.cfg = cfg
    app.state.store = store
    app.state.runner = runner

    return app
