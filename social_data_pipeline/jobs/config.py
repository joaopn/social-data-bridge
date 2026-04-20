"""Load and validate config/jobs/config.yaml."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

import yaml


Backend = Literal["postgres", "starrocks", "mongodb"]


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
    # Per-backend default timeouts, in seconds. 0 means "no timeout" where
    # supported (PG native, Mongo skips maxTimeMS); StarRocks doesn't accept
    # 0 and is capped at 259200 (72h). See timeout_for().
    default_timeouts: dict[str, int]
    history_retention: int
    targets: dict[str, Target] = field(default_factory=dict)

    def targets_for(self, backend: Backend) -> list[Target]:
        return [t for t in self.targets.values() if t.backend == backend]

    def has_backend(self, backend: Backend) -> bool:
        return any(t.backend == backend for t in self.targets.values())

    def timeout_for(self, backend: str) -> int:
        """Default timeout (seconds) for a backend, 0 = no limit."""
        return int(self.default_timeouts.get(backend, 0))


def load_config(path: Path | str = "/app/config/jobs/config.yaml") -> JobsConfig:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"jobs config missing: {p}")
    raw = yaml.safe_load(p.read_text()) or {}

    targets_raw = raw.get("targets") or {}
    targets: dict[str, Target] = {}
    for name, spec in targets_raw.items():
        backend = spec.get("backend")
        if backend not in ("postgres", "starrocks", "mongodb"):
            raise ValueError(
                f"target {name!r}: backend must be 'postgres', 'starrocks', or 'mongodb'; got {backend!r}"
            )
        db = (spec.get("database") or "").strip()
        if backend == "postgres" and not db:
            raise ValueError(f"target {name!r}: postgres targets require a 'database'")
        # StarRocks targets may leave database empty — the connection has no
        # default schema and agents must fully-qualify table names.
        # Mongo targets may leave database empty too — the agent then supplies
        # a `database` argument on every submit_mongo_query call.
        targets[name] = Target(name=name, backend=backend, database=db)

    if not targets:
        raise ValueError("config/jobs/config.yaml has no targets configured")

    jobs_dir = Path(os.environ.get("JOBS_DIR", "/data/jobs"))
    # Inside the container the results directory is always <jobs_dir>/results —
    # the docker-compose mount makes JOBS_RESULT_ROOT (host) resolve here.
    result_root = jobs_dir / "results"
    host_result_root = os.environ.get("JOBS_RESULT_ROOT") or raw.get("result_root") or str(result_root)

    # Per-backend timeouts. If only a legacy `default_timeout_seconds` key is
    # present (older config), apply it across backends with the SR cap.
    default_timeouts: dict[str, int] = {}
    raw_timeouts = raw.get("default_timeouts") or {}
    if isinstance(raw_timeouts, dict) and raw_timeouts:
        for backend in ("postgres", "starrocks", "mongodb"):
            val = raw_timeouts.get(backend)
            if val is not None:
                default_timeouts[backend] = int(val)
    else:
        legacy = raw.get("default_timeout_seconds")
        if legacy is not None:
            legacy_int = int(legacy)
            default_timeouts["postgres"] = legacy_int
            default_timeouts["mongodb"] = legacy_int
            default_timeouts["starrocks"] = min(legacy_int, 259200) if legacy_int > 0 else 259200
    # Fill in anything still missing with safe defaults: unlimited for PG/Mongo,
    # the SR maximum (72h) for SR.
    default_timeouts.setdefault("postgres", 0)
    default_timeouts.setdefault("mongodb", 0)
    default_timeouts.setdefault("starrocks", 259200)

    return JobsConfig(
        port=int(raw.get("port", 8050)),
        jobs_dir=jobs_dir,
        result_root=result_root,
        host_result_root=host_result_root,
        max_concurrent=int(raw.get("max_concurrent", 1)),
        default_timeouts=default_timeouts,
        history_retention=int(raw.get("history_retention", 500)),
        targets=targets,
    )


def auth_enabled(backend: Backend) -> bool:
    """True when the backend's auth flag is on in the runtime env."""
    var = {
        "postgres": "POSTGRES_AUTH_ENABLED",
        "starrocks": "STARROCKS_AUTH_ENABLED",
        "mongodb": "MONGO_AUTH_ENABLED",
    }[backend]
    return os.environ.get(var, "").lower() in ("1", "true", "yes")


def admin_password(backend: Backend) -> str | None:
    """Admin password from the process env, or None if unset."""
    var = {
        "postgres": "POSTGRES_PASSWORD",
        "starrocks": "STARROCKS_ROOT_PASSWORD",
        "mongodb": "MONGO_ADMIN_PASSWORD",
    }[backend]
    val = os.environ.get(var, "")
    return val or None
