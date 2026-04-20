"""Query scheduler (jobs) configuration for Social Data Pipeline.

Interactive setup-jobs questionnaire. Writes:
- config/jobs/config.yaml
- .env additions (JOBS_PORT, JOBS_RESULT_ROOT)
- docker-compose.override.yml additions (/jobs_export mount on postgres/starrocks)
- config/starrocks/fe.local.conf (enable_outfile_to_local=true, if SR target)
"""

from __future__ import annotations

import sys
from pathlib import Path

try:
    import yaml
except ImportError:
    print("Error: PyYAML is required. Install with: pip install pyyaml")
    sys.exit(1)

from social_data_pipeline.setup.utils import (
    ROOT,
    CONFIG_DIR,
    ask,
    ask_bool,
    ask_int,
    load_db_setup,
    load_env,
    section_header,
    update_env_file,
    write_files,
)


JOBS_EXPORT_CONTAINER_PATH = "/jobs_export"
DEFAULT_RESULT_ROOT = "./data/jobs/results"


# ============================================================================
# Load existing

def _load_existing_jobs_config() -> dict:
    jobs_yaml = CONFIG_DIR / "jobs" / "config.yaml"
    if not jobs_yaml.exists():
        return {}
    try:
        return yaml.safe_load(jobs_yaml.read_text()) or {}
    except (OSError, yaml.YAMLError):
        return {}


def _existing_targets_for(existing: dict, backend: str) -> dict:
    targets = (existing.get("targets") or {})
    return {name: spec for name, spec in targets.items()
            if (spec or {}).get("backend") == backend}


# ============================================================================
# Questionnaire

def run_questionnaire(db_setup: dict) -> dict:
    existing = _load_existing_jobs_config()
    databases = set(db_setup.get("databases", []))
    eligible = databases & {"postgres", "starrocks"}
    if not eligible:
        print("  Error: jobs scheduler requires postgres and/or starrocks configured.")
        print("  Run `python sdp.py db setup` first.\n")
        sys.exit(1)

    section_header("Query Scheduler Configuration")
    print(f"  Eligible database backends: {', '.join(sorted(eligible))}")
    print()

    settings: dict = {}
    settings["port"] = ask_int(
        "Web UI / MCP port", existing.get("port", 8050), tag="jobs_port"
    )
    settings["result_root"] = (
        ask(
            "Result root path (folder where job result files land)",
            existing.get("result_root") or DEFAULT_RESULT_ROOT,
            tag="jobs_result_root",
        ).strip()
        or DEFAULT_RESULT_ROOT
    )
    settings["max_concurrent"] = ask_int(
        "Max concurrent running jobs",
        existing.get("max_concurrent", 5),
        tag="jobs_max_concurrent",
    )
    # StarRocks caps query_timeout at 259200 s (72 h); the setup enforces
    # that limit for every backend so the configured value is always
    # accepted by every adapter.
    existing_hours = int(existing.get("default_timeout_seconds", 72 * 3600)) // 3600 or 72
    while True:
        timeout_hours = ask_int(
            "Default query timeout (hours, max 72 — StarRocks limit)",
            existing_hours,
            tag="jobs_timeout_hours",
        )
        if 1 <= timeout_hours <= 72:
            break
        print(
            f"    Error: timeout must be between 1 and 72 hours "
            f"(StarRocks caps query_timeout at 72h). Got {timeout_hours}."
        )
        existing_hours = 72
    settings["default_timeout_seconds"] = int(timeout_hours) * 3600
    settings["history_retention"] = ask_int(
        "History retention (keep last N jobs in UI)",
        existing.get("history_retention", 500),
        tag="jobs_history_retention",
    )

    targets: dict[str, dict] = {}

    print()
    print("  A 'target' is a named connection profile. Agents reference it")
    print("  when submitting queries, e.g. submit_postgres_query(target='reddit_pg', ...).")
    print("  One target = one database on one backend. You can configure several.")
    print()

    env = load_env()
    pg_db_default = env.get("DB_NAME") or "datasets"

    if "postgres" in eligible:
        existing_pg = _existing_targets_for(existing, "postgres")
        default_name = next(iter(existing_pg), "pg_main")
        want_pg = ask_bool(
            "Add a PostgreSQL target?",
            bool(existing_pg) or True,
            tag="jobs_pg_target_enable",
        )
        if want_pg:
            name = ask(
                "  Target label (agents pass this as target=<name>)",
                default_name,
                tag="jobs_pg_target_name",
            ).strip()
            db_default = (
                (existing.get("targets") or {}).get(name, {}).get("database")
                or pg_db_default
            )
            database = ask(
                "  PostgreSQL database this target connects to",
                db_default,
                tag="jobs_pg_target_db",
            ).strip()
            targets[name] = {"backend": "postgres", "database": database}

    if "starrocks" in eligible:
        existing_sr = _existing_targets_for(existing, "starrocks")
        default_name = next(iter(existing_sr), "sr_main")
        want_sr = ask_bool(
            "Add a StarRocks target?",
            bool(existing_sr) or True,
            tag="jobs_sr_target_enable",
        )
        if want_sr:
            name = ask(
                "  Target label (agents pass this as target=<name>)",
                default_name,
                tag="jobs_sr_target_name",
            ).strip()
            # No default database on SR targets: agents must fully-qualify
            # every table reference (e.g. `reddit.comments`). Keeps queries
            # explicit about which database they hit and avoids coupling
            # the target to a single source.
            targets[name] = {"backend": "starrocks", "database": ""}

    if not targets:
        print("\n  Error: at least one target must be configured.\n")
        sys.exit(1)

    settings["targets"] = targets
    settings["_backends"] = sorted({t["backend"] for t in targets.values()})
    return settings


# ============================================================================
# Config generators

def generate_jobs_yaml(settings: dict) -> str:
    out = {
        "port": settings["port"],
        "result_root": settings["result_root"],
        "max_concurrent": settings["max_concurrent"],
        "default_timeout_seconds": settings["default_timeout_seconds"],
        "history_retention": settings["history_retention"],
        "targets": settings["targets"],
    }
    return yaml.dump(out, default_flow_style=False, sort_keys=False)


def compute_override_update(settings: dict) -> tuple[str, list[str]]:
    """Produce the new docker-compose.override.yml content + list of services
    touched. Does not write anything."""
    override_path = ROOT / "docker-compose.override.yml"
    mount = f"{settings['result_root']}:{JOBS_EXPORT_CONTAINER_PATH}:rw"
    services_touched: list[str] = []

    data: dict = {}
    if override_path.exists():
        try:
            data = yaml.safe_load(override_path.read_text()) or {}
        except yaml.YAMLError:
            data = {}
    data.setdefault("services", {})

    svc_map = {"postgres": "postgres", "starrocks": "starrocks"}
    for backend in settings["_backends"]:
        svc = svc_map.get(backend)
        if not svc:
            continue
        services_touched.append(svc)
        svc_block = data["services"].setdefault(svc, {})
        volumes = [
            v for v in (svc_block.get("volumes") or [])
            if f":{JOBS_EXPORT_CONTAINER_PATH}" not in str(v)
        ]
        volumes.append(mount)
        svc_block["volumes"] = volumes

    header = (
        "# Auto-generated by sdp — volume mounts for database servers.\n"
        "# Setup mounts (tablespaces, SR storage, jobs export) + per-source\n"
        "# data mounts from sdp db start.\n"
        "\n"
    )
    body = yaml.dump(data, default_flow_style=False, sort_keys=False)
    return header + body, services_touched


def enable_starrocks_outfile_local() -> bool:
    """Ensure config/starrocks/fe.local.conf has enable_outfile_to_local=true."""
    fe_local = CONFIG_DIR / "starrocks" / "fe.local.conf"
    if not fe_local.exists():
        return False
    text = fe_local.read_text()
    lines = text.splitlines()
    found = False
    out_lines = []
    for line in lines:
        stripped = line.lstrip().lstrip("#").lstrip()
        if stripped.startswith("enable_outfile_to_local"):
            out_lines.append("enable_outfile_to_local = true")
            found = True
        else:
            out_lines.append(line)
    if not found:
        if out_lines and out_lines[-1].strip() != "":
            out_lines.append("")
        out_lines.append("# Enabled by `sdp db setup-jobs` for local file export")
        out_lines.append("enable_outfile_to_local = true")
    new = "\n".join(out_lines).rstrip() + "\n"
    if new == text:
        return False
    fe_local.write_text(new)
    return True


# ============================================================================
# Summary + main

def print_summary(settings, files_to_write, services_with_mount, sr_fe_applicable):
    section_header("Jobs Scheduler Configuration Summary")
    print(f"  Port:             {settings['port']}")
    print(f"  Result root:      {settings['result_root']}")
    print(f"  Max concurrent:   {settings['max_concurrent']}")
    print(f"  Timeout:          {settings['default_timeout_seconds'] // 3600} hour(s)")
    print(f"  History limit:    {settings['history_retention']}")
    print("  Targets:")
    for name, spec in settings["targets"].items():
        print(f"    - {name}: backend={spec['backend']} database={spec['database']}")
    print()
    print("  Files to write:")
    for path, _ in files_to_write:
        rel = path.relative_to(ROOT)
        print(f"    {rel}{' (exists, will backup)' if path.exists() else ''}")
    print("    .env (update)")
    print("    docker-compose.override.yml (merge /jobs_export mount)")
    if sr_fe_applicable:
        print("    config/starrocks/fe.local.conf (enable_outfile_to_local=true)")
    if services_with_mount:
        print(f"\n  Services receiving /jobs_export mount: {', '.join(services_with_mount)}")
    print()


def main():
    print()
    print("  Social Data Pipeline - Query Scheduler Configuration")
    print("  ====================================================")
    print()
    print("  Configure the jobs scheduler: a queue for long-running and")
    print("  write-capable queries with a web UI for approval + history.")
    print("  Press Enter to accept defaults shown in [brackets].")
    print()

    db_setup = load_db_setup()
    if not db_setup or not db_setup.get("databases"):
        print("  Error: No databases configured. Run first: python sdp.py db setup")
        sys.exit(1)

    settings = run_questionnaire(db_setup)

    files_to_write = [(CONFIG_DIR / "jobs" / "config.yaml", generate_jobs_yaml(settings))]
    override_content, services_with_mount = compute_override_update(settings)
    sr_fe_applicable = (
        "starrocks" in settings["_backends"]
        and (CONFIG_DIR / "starrocks" / "fe.local.conf").exists()
    )

    print_summary(settings, files_to_write, services_with_mount, sr_fe_applicable)

    if not ask_bool("Write these files?", True, tag="jobs_write_files"):
        print("\n  Aborted. No files written.\n")
        sys.exit(0)

    print()
    write_files(files_to_write)

    update_env_file({
        "JOBS_PORT": str(settings["port"]),
        "JOBS_RESULT_ROOT": settings["result_root"],
    })
    print("  Updated:   .env")

    (ROOT / "docker-compose.override.yml").write_text(override_content)
    print("  Updated:   docker-compose.override.yml")

    if sr_fe_applicable and enable_starrocks_outfile_local():
        print("  Updated:   config/starrocks/fe.local.conf")

    result_root_path = settings["result_root"]
    result_root = (
        ROOT / result_root_path
        if not result_root_path.startswith("/")
        else Path(result_root_path)
    )
    result_root.mkdir(parents=True, exist_ok=True)
    print(f"  Ensured:   {result_root_path}/")

    (ROOT / "data" / "jobs").mkdir(parents=True, exist_ok=True)

    print()
    print("  Done! Jobs scheduler has been configured.")
    print()
    print("  Next steps:")
    print("    python sdp.py db stop    # stop DBs (if running — picks up new mounts)")
    print("    python sdp.py db start   # start DBs + MCPs + jobs (jobs image auto-built)")
    print()
    print(f"  Web UI:     http://localhost:{settings['port']}/")
    print(f"  MCP URL:    http://localhost:{settings['port']}/mcp")
    print()


if __name__ == "__main__":
    main()
