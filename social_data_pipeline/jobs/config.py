"""Load and validate config/jobs/config.yaml."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

import yaml


Backend = Literal["postgres", "starrocks"]


@dataclass
class Target:
    name: str
    backend: Backend
    database: str


@dataclass
class JobsConfig:
    port: int
    jobs_dir: Path
    result_root: Path  # inside this container — host path is JOBS_RESULT_ROOT
    host_result_root: str  # for display in the UI / status output
    max_concurrent: int
    default_timeout_seconds: int
    history_retention: int
    targets: dict[str, Target] = field(default_factory=dict)

    def targets_for(self, backend: Backend) -> list[Target]:
        return [t for t in self.targets.values() if t.backend == backend]

    def has_backend(self, backend: Backend) -> bool:
        return any(t.backend == backend for t in self.targets.values())


def load_config(path: Path | str = "/app/config/jobs/config.yaml") -> JobsConfig:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"jobs config missing: {p}")
    raw = yaml.safe_load(p.read_text()) or {}

    targets_raw = raw.get("targets") or {}
    targets: dict[str, Target] = {}
    for name, spec in targets_raw.items():
        backend = spec.get("backend")
        if backend not in ("postgres", "starrocks"):
            raise ValueError(
                f"target {name!r}: backend must be 'postgres' or 'starrocks', got {backend!r}"
            )
        db = (spec.get("database") or "").strip()
        if backend == "postgres" and not db:
            raise ValueError(f"target {name!r}: postgres targets require a 'database'")
        # StarRocks targets may leave database empty — the connection then
        # has no default schema and agents must fully-qualify table names.
        targets[name] = Target(name=name, backend=backend, database=db)

    if not targets:
        raise ValueError("config/jobs/config.yaml has no targets configured")

    jobs_dir = Path(os.environ.get("JOBS_DIR", "/data/jobs"))
    # Inside the container the results directory is always <jobs_dir>/results —
    # the docker-compose mount makes JOBS_RESULT_ROOT (host) resolve here.
    result_root = jobs_dir / "results"
    host_result_root = os.environ.get("JOBS_RESULT_ROOT") or raw.get("result_root") or str(result_root)

    return JobsConfig(
        port=int(raw.get("port", 8050)),
        jobs_dir=jobs_dir,
        result_root=result_root,
        host_result_root=host_result_root,
        max_concurrent=int(raw.get("max_concurrent", 1)),
        default_timeout_seconds=int(raw.get("default_timeout_seconds", 4 * 3600)),
        history_retention=int(raw.get("history_retention", 500)),
        targets=targets,
    )


def auth_enabled(backend: Backend) -> bool:
    """True when the backend's auth flag is on in the runtime env."""
    var = {"postgres": "POSTGRES_AUTH_ENABLED", "starrocks": "STARROCKS_AUTH_ENABLED"}[backend]
    return os.environ.get(var, "").lower() in ("1", "true", "yes")


def admin_password(backend: Backend) -> str | None:
    """Admin password from the process env, or None if unset."""
    var = {"postgres": "POSTGRES_PASSWORD", "starrocks": "STARROCKS_ROOT_PASSWORD"}[backend]
    val = os.environ.get(var, "")
    return val or None
