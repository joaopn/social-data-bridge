"""Mount-coherence helpers for `db start` / `source add|remove` / `run`.

The pipeline lets the user mutate sources independently of database server
lifecycle:

    sdp db start postgres        # generates docker-compose.override.yml
                                 # with per-source mounts as they exist NOW
    sdp source add reddit        # writes config/sources/reddit/, but does
                                 # NOT touch override.yml or restart PG
    sdp run postgres_ingest -s reddit
                                 # ingest container expects PG to see
                                 # /data/parsed/reddit, but PG is still
                                 # running with the old mount set

Without a guard, the mismatch surfaces deep inside `pg_parquet`'s
COPY-from-file with an opaque "could not stat" error. This module is the
source of truth for "is the running DB container's mount set in sync with
the configured sources?".

Two surfaces consume it:

1. `cmd_source_add` / `cmd_source_remove` — after writing/deleting source
   config, compare the *file* (`docker-compose.override.yml`) against the
   new source set. If a running PG/SR container is now out of sync, warn
   the operator with the exact `db stop && db start` recovery line.

2. `cmd_run` (for `*_ingest` / `*_ml` profiles) — probe the running DB
   container with `docker inspect` and verify the per-source mounts the
   profile is about to need. Fail-fast with the same recovery line
   instead of letting the orchestrator container die deep in COPY.

The two callers use different *sources* (file vs. live container) on
purpose. The override file is the cheaper proxy at warning time
(operator just changed something locally, fastest signal); the inspect
probe is the authoritative check at run time (catches edge cases like a
manually edited override or a container started before the override was
last regenerated).
"""

from __future__ import annotations

# Per-server: which source-level profiles cause a source to need mounts on
# this server. Mirrors `_resolve_server_data_mounts` in sdp.py — kept in one
# place so adding a new ingest profile only needs editing this table.
SERVICE_PROFILES = {
    "postgres": frozenset({"postgres_ingest", "postgres_ml"}),
    "starrocks": frozenset({"sr_ingest", "sr_ml"}),
}

# Profile → DB service whose mount set the profile depends on at run time.
PROFILE_TO_SERVICE = {
    "postgres_ingest": "postgres",
    "postgres_ml": "postgres",
    "sr_ingest": "starrocks",
    "sr_ml": "starrocks",
}

# Substrings identifying mounts that come from `db setup` (tablespaces, SR
# storage, jobs export) rather than from per-source data. Drift detection
# ignores these — they're set-and-forget and not affected by `source add`.
_NON_SOURCE_MOUNT_MARKERS = (
    "/data/tablespace/",
    "/data/deploy/starrocks/",
    ":/jobs_export",
)


def expected_source_mounts(sources_info, service):
    """Build the per-source mount set a service should have.

    Args:
        sources_info: list of dicts ``{name, profiles, paths}`` — the same
            shape returned by walking ``list_sources()`` +
            ``load_source_config()`` + ``get_source_profiles()``.
        service: ``"postgres"`` or ``"starrocks"``.

    Returns:
        set[str] of mount strings in the form
        ``"<host_path>:/data/<parsed|output>/<source>:ro"``. Matches the
        format `_resolve_server_data_mounts` writes to the override.
    """
    needed = SERVICE_PROFILES.get(service, frozenset())
    out = set()
    for s in sources_info:
        if not (set(s.get("profiles", [])) & needed):
            continue
        paths = s.get("paths", {}) or {}
        for key, container_base in (("parsed", "/data/parsed"), ("output", "/data/output")):
            host_path = paths.get(key, "")
            if host_path:
                out.add(f"{host_path}:{container_base}/{s['name']}:ro")
    return out


def parse_override_source_mounts(override_data, service):
    """Pull the per-source mounts out of a parsed override dict.

    Filters away tablespace / SR storage / jobs_export mounts so the result
    is comparable to ``expected_source_mounts``.

    Args:
        override_data: parsed ``docker-compose.override.yml`` (or ``None``
            / ``{}`` if the file is missing).
        service: ``"postgres"`` or ``"starrocks"``.

    Returns:
        set[str] of mount strings.
    """
    services = (override_data or {}).get("services", {}) or {}
    svc = services.get(service) or {}
    out = set()
    for vol in svc.get("volumes", []) or []:
        v = str(vol)
        if any(marker in v for marker in _NON_SOURCE_MOUNT_MARKERS):
            continue
        out.add(v)
    return out


def compute_mount_drift(override_data, sources_info, services=("postgres", "starrocks")):
    """Compare expected per-source mounts to those in the override file.

    Args:
        override_data: parsed override.yml (dict) or None.
        sources_info: see ``expected_source_mounts``.
        services: which services to check (default both PG and SR).

    Returns:
        dict mapping service name → ``{"missing": [...], "extra": [...]}``.
        Only services with drift appear; empty dict means coherent. Both
        lists are sorted for stable output.
    """
    drift = {}
    for svc in services:
        expected = expected_source_mounts(sources_info, svc)
        actual = parse_override_source_mounts(override_data, svc)
        missing = sorted(expected - actual)
        extra = sorted(actual - expected)
        if missing or extra:
            drift[svc] = {"missing": missing, "extra": extra}
    return drift


def expected_runtime_mounts_for_source(source_name, source_paths):
    """Per-source mount destinations a running DB needs to see.

    Used by ``cmd_run`` to validate a live container's mount set. Returns a
    dict of ``destination → expected host source``, e.g.
    ``{"/data/parsed/reddit": "/abs/host/parsed/reddit"}``.

    Only ``parsed`` and ``output`` paths from the source config matter for
    server-side reads (see ``_pg_server_path`` / ``_sr_server_path`` in
    `db/{postgres,starrocks}/ingest.py`).
    """
    out = {}
    for key, container_base in (("parsed", "/data/parsed"), ("output", "/data/output")):
        host_path = (source_paths or {}).get(key, "")
        if host_path:
            out[f"{container_base}/{source_name}"] = host_path
    return out


def runtime_mount_drift(actual_mounts, source_name, source_paths):
    """Compare expected per-source destinations against a live container's mounts.

    Args:
        actual_mounts: list of dicts shaped like docker inspect's
            ``Mounts`` array (each item has ``Destination`` and
            ``Source`` keys at minimum).
        source_name: source being run.
        source_paths: ``paths`` block from the source's platform.yaml.

    Returns:
        list of destination paths that are missing from the container.
        Empty list when the container is in sync.
    """
    expected = expected_runtime_mounts_for_source(source_name, source_paths)
    actual_destinations = {
        m.get("Destination") for m in (actual_mounts or [])
        if m.get("Destination")
    }
    return sorted(d for d in expected if d not in actual_destinations)
