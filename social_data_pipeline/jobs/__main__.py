"""Entry point: `python -m social_data_pipeline.jobs` boots the FastAPI app."""

from __future__ import annotations

import logging
import os

import uvicorn

from .app import build_app
from .config import load_config


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("JOBS_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    cfg = load_config()
    app = build_app(cfg)
    host = os.environ.get("JOBS_HOST", "0.0.0.0")
    uvicorn.run(app, host=host, port=cfg.port, log_level="info")


if __name__ == "__main__":
    main()
