#!/usr/bin/env python3
"""Social Data Pipeline CLI.

Unified entrypoint for database management, source configuration, and pipeline execution.

Usage:
    sdp.py db setup                          Configure databases (PostgreSQL, MongoDB)
    sdp.py db setup-mcp                      Configure MCP servers for databases
    sdp.py db start [service]                Start services (postgres|mongo|postgres-mcp|mongo-mcp|all)
    sdp.py db stop [service]                 Stop services (postgres|mongo|postgres-mcp|mongo-mcp|all)
    sdp.py db status                         Show database config and health
    sdp.py db unsetup                        Remove database config (and optionally data)
    sdp.py db unsetup-mcp                    Remove MCP configuration
    sdp.py db recover-password               Reset database admin password
    sdp.py db create-indexes [--source <name>] Interactively create database indexes

    sdp.py source add <name> [--hf ID]        Add a new source (--hf for HF datasets)
    sdp.py source download <name>            Download HF dataset files
    sdp.py source configure <name>           Reconfigure existing source (platform-specific)
    sdp.py source add-classifiers <name>     Add ML classifiers for a source
    sdp.py source remove <name>              Remove source config
    sdp.py source list                       List configured sources
    sdp.py source status [name]              Show source processing/ingestion status
    sdp.py source error-logs [name]          Show database ingestion error logs

    sdp.py run <profile> [--source <name>]   Run pipeline for a source (--build to rebuild, --filter to select files)

    sdp.py setup                             Legacy: full interactive setup (core + classifiers + platform)
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
from getpass import getpass
from pathlib import Path

ROOT = Path(__file__).resolve().parent
CONFIG_DIR = ROOT / "config"

VALID_PROFILES = ["parse", "lingua", "ml", "postgres_ingest", "postgres_ml", "mongo_ingest"]

# Map profile names to docker compose service names (only where they differ)
PROFILE_SERVICE_MAP = {
    "ml": "ml-gpu",
    "postgres_ingest": "postgres-ingest",
    "postgres_ml": "postgres-ml",
    "mongo_ingest": "mongo-ingest",
}


# ============================================================================
# Helpers
# ============================================================================

def load_env():
    """Load .env file into a dict (does not modify os.environ)."""
    env_path = ROOT / ".env"
    if not env_path.exists():
        return {}
    env = {}
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" in line:
            key, _, value = line.partition("=")
            env[key.strip()] = value.strip()
    return env


def docker_compose(*args):
    """Run docker compose with the given arguments."""
    cmd = ["docker", "compose"] + list(args)
    print(f"  $ {' '.join(cmd)}")
    return subprocess.run(cmd, cwd=ROOT)


def _get_configured_db_services():
    """Determine which database services are configured."""
    db_dir = CONFIG_DIR / "db"
    services = []
    if (db_dir / "postgres.yaml").exists():
        services.append("postgres")
    if (db_dir / "mongo.yaml").exists():
        services.append("mongo")
    return services


def _get_configured_mcp_services():
    """Determine which MCP services are configured."""
    mcp_path = CONFIG_DIR / "db" / "mcp.yaml"
    if not mcp_path.exists():
        return []
    try:
        import yaml
        config = yaml.safe_load(mcp_path.read_text()) or {}
    except Exception:
        return []
    services = []
    if config.get("postgres", {}).get("enabled"):
        services.append("postgres_mcp")
    if config.get("mongo", {}).get("enabled"):
        services.append("mongo_mcp")
    return services


def _load_mcp_config():
    """Load MCP config from config/db/mcp.yaml. Returns dict or empty."""
    mcp_path = CONFIG_DIR / "db" / "mcp.yaml"
    if not mcp_path.exists():
        return {}
    try:
        import yaml
        return yaml.safe_load(mcp_path.read_text()) or {}
    except Exception:
        return {}


def _is_auth_enabled():
    """Check if database authentication is enabled via .env."""
    env = load_env()
    return env.get("POSTGRES_AUTH_ENABLED") == "true" or env.get("MONGO_AUTH_ENABLED") == "true"


def _load_db_yaml(name):
    """Load a config/db/<name>.yaml file. Returns dict or empty."""
    path = CONFIG_DIR / "db" / f"{name}.yaml"
    if not path.exists():
        return {}
    try:
        import yaml
        return yaml.safe_load(path.read_text()) or {}
    except Exception:
        return {}


def _prompt_db_password(label="Database admin password", tag=None):
    """Prompt for the database admin password. Returns the password string."""
    prefix = ""
    if tag and os.environ.get('SDP_TAGGED_MODE'):
        prefix = f"[{tag}] "
    pw = getpass(f"  {prefix}{label}: ")
    if not pw:
        print("  Error: Password cannot be empty.")
        sys.exit(1)
    return pw


def _set_auth_env(password):
    """Set authentication env vars for docker compose subprocess."""
    os.environ["POSTGRES_PASSWORD"] = password
    os.environ["MONGO_ADMIN_PASSWORD"] = password


# ============================================================================
# sdp db setup
# ============================================================================

def cmd_db_setup(args):
    """Run interactive database configuration."""
    from social_data_pipeline.setup.db import main as db_main
    db_main()
    return 0


# ============================================================================
# sdp db setup-mcp / unsetup-mcp
# ============================================================================

def cmd_db_setup_mcp(args):
    """Configure MCP servers for databases."""
    from social_data_pipeline.setup.mcp import main as mcp_main
    mcp_main()
    return 0


def cmd_db_unsetup_mcp(args):
    """Remove MCP configuration and stop MCP containers."""
    mcp_path = CONFIG_DIR / "db" / "mcp.yaml"
    if not mcp_path.exists():
        print("\n  No MCP configuration found.\n")
        return 0

    # Stop MCP containers
    mcp_services = _get_configured_mcp_services()
    if mcp_services:
        configured = _get_configured_db_services()
        compose_args = []
        for svc in configured:
            mcp_profile = f"{svc}_mcp"
            if mcp_profile in mcp_services:
                compose_args += ["--profile", svc, "--profile", mcp_profile]
        if compose_args:
            print("  Stopping MCP containers...")
            docker_compose(*compose_args, "down")

    # Remove config file
    mcp_path.unlink()
    print(f"  Removed:   config/db/mcp.yaml")

    # Remove MCP env vars from .env
    env_path = ROOT / ".env"
    if env_path.exists():
        mcp_keys = {"POSTGRES_MCP_PORT", "POSTGRES_MCP_ACCESS_MODE",
                     "MONGO_MCP_PORT", "MONGO_MCP_READ_ONLY",
                     "POSTGRES_MCP_USER", "MONGO_MCP_USER"}
        lines = env_path.read_text().splitlines()
        new_lines = []
        for line in lines:
            stripped = line.lstrip("# ").strip()
            key = stripped.split("=", 1)[0] if "=" in stripped else stripped
            if key not in mcp_keys:
                new_lines.append(line)
        env_path.write_text("\n".join(new_lines) + "\n")
        print(f"  Updated:   .env")

    print("\n  MCP configuration removed. Databases are not affected.\n")
    return 0


# ============================================================================
# sdp db start / stop
# ============================================================================

def cmd_db_start(args):
    """Start configured database server(s)."""
    configured = _get_configured_db_services()
    if not configured:
        print("  No databases configured. Run: python sdp.py db setup")
        return 1

    service = args.service
    mcp_services = _get_configured_mcp_services()

    # MCP-only target (e.g. "postgres-mcp" → profile "postgres_mcp")
    if service and service.endswith("-mcp"):
        mcp_profile = service.replace("-", "_")
        parent_db = service.rsplit("-", 1)[0]
        if mcp_profile not in mcp_services:
            print(f"  Error: '{service}' is not configured. Run: python sdp.py db setup-mcp")
            return 1
        # Verify parent DB is running
        ps_result = docker_compose("ps", "--format", "{{.Names}}", "--filter",
                                   f"status=running")
        if ps_result.returncode != 0:
            return ps_result.returncode
        # Prompt for admin password if auth is enabled (needed for init container)
        if _is_auth_enabled():
            password = _prompt_db_password(tag="sdp_db_password")
            _set_auth_env(password)
        # Start only the MCP profile (init container runs first via depends_on)
        mcp_args = ["--profile", parent_db, "--profile", mcp_profile, "up", "-d"]
        result = docker_compose(*mcp_args)
        return result.returncode

    # Database target (with auto-bundled MCP)
    if service:
        if service not in configured:
            print(f"  Warning: '{service}' is not configured, starting anyway.")
        targets = [service]
    else:
        targets = configured

    # Prompt for admin password if auth is enabled
    password = None
    if _is_auth_enabled():
        password = _prompt_db_password(tag="sdp_db_password")
        _set_auth_env(password)

    # Determine which MCP profiles to start
    mcp_targets = []
    for svc in targets:
        mcp_profile = f"{svc}_mcp"
        if mcp_profile in mcp_services:
            mcp_targets.append(mcp_profile)

    # Start databases first (without MCP), wait for healthchecks
    db_args = []
    for svc in targets:
        db_args += ["--profile", svc]
    db_args += ["up", "-d", "--wait"]
    result = docker_compose(*db_args)
    if result.returncode != 0:
        return result.returncode

    # Start MCP servers (init containers handle user creation via depends_on)
    if mcp_targets:
        mcp_args = []
        for svc in targets:
            mcp_args += ["--profile", svc]
        for mp in mcp_targets:
            mcp_args += ["--profile", mp]
        mcp_args += ["up", "-d"]
        result = docker_compose(*mcp_args)

    return result.returncode


def cmd_db_stop(args):
    """Stop configured database server(s)."""
    configured = _get_configured_db_services()
    if not configured:
        print("  No databases configured. Run: python sdp.py db setup")
        return 1

    service = args.service
    mcp_services = _get_configured_mcp_services()

    # MCP-only target (e.g. "postgres-mcp" → profile "postgres_mcp")
    if service and service.endswith("-mcp"):
        mcp_profile = service.replace("-", "_")
        parent_db = service.rsplit("-", 1)[0]
        if mcp_profile not in mcp_services:
            print(f"  Error: '{service}' is not configured.")
            return 1
        profiles = ["--profile", parent_db, "--profile", mcp_profile]
        result = docker_compose(*profiles, "stop", service)
        if result.returncode != 0:
            return result.returncode
        result = docker_compose(*profiles, "rm", "-f", service)
        return result.returncode

    # Database target (stops DB + its MCP)
    if service:
        targets = [service]
    else:
        targets = configured

    compose_args = []
    for svc in targets:
        compose_args += ["--profile", svc]
        mcp_profile = f"{svc}_mcp"
        if mcp_profile in mcp_services:
            compose_args += ["--profile", mcp_profile]
    compose_args += ["down"]

    result = docker_compose(*compose_args)
    return result.returncode


# ============================================================================
# sdp db status
# ============================================================================

def cmd_db_status(args):
    """Show database configuration and health."""
    env = load_env()
    configured = _get_configured_db_services()

    print()
    print("  Social Data Pipeline - Database Status")
    print("  =====================================")

    if not configured:
        print("\n  No databases configured. Run: python sdp.py db setup\n")
        return 0

    # Authentication status
    auth_enabled = env.get("POSTGRES_AUTH_ENABLED") == "true" or env.get("MONGO_AUTH_ENABLED") == "true"
    print(f"\n  Authentication: {'enabled' if auth_enabled else 'disabled'}")
    if auth_enabled:
        pg_user = env.get("POSTGRES_USER", "postgres") if "postgres" in configured else None
        mongo_user = env.get("MONGO_ADMIN_USER") if "mongo" in configured else None
        if pg_user or mongo_user:
            rw_parts = []
            if pg_user:
                rw_parts.append(f"postgres: {pg_user}")
            if mongo_user:
                rw_parts.append(f"mongo: {mongo_user}")
            print(f"    RW user:       {', '.join(rw_parts)}")
        ro_user = env.get("POSTGRES_RO_USER") or env.get("MONGO_RO_USER")
        if ro_user:
            # Check if RO credentials file exists in any data path
            ro_cred_path = None
            for path_key, default in [("PGDATA_PATH", "./data/database/postgres"),
                                      ("MONGO_DATA_PATH", "./data/database/mongo")]:
                dp = Path(env.get(path_key, default))
                if not dp.is_absolute():
                    dp = ROOT / dp
                cred_file = dp / ".ro_credentials"
                if cred_file.exists():
                    ro_cred_path = cred_file
                    break
            if ro_cred_path:
                print(f"    RO user:       {ro_user} (password in {ro_cred_path})")
            else:
                print(f"    RO user:       {ro_user} (no password)")
        mcp_user = env.get("POSTGRES_MCP_USER") or env.get("MONGO_MCP_USER")
        if mcp_user:
            print(f"    MCP user:      {mcp_user}")

    # Check which services are running
    running_services = set()
    result = subprocess.run(
        ["docker", "compose", "ps", "--format", "json", "--filter", "status=running"],
        capture_output=True, text=True, cwd=ROOT,
    )
    if result.returncode == 0 and result.stdout.strip():
        for line in result.stdout.strip().splitlines():
            try:
                container = json.loads(line)
                svc = container.get("Service")
                if svc:
                    running_services.add(svc)
            except json.JSONDecodeError:
                pass

    # PostgreSQL
    if "postgres" in configured:
        pgdata_path = env.get("PGDATA_PATH", "./data/database/postgres")
        pgdata = Path(pgdata_path)
        if not pgdata.is_absolute():
            pgdata = ROOT / pgdata

        print(f"\n  PostgreSQL:")
        print(f"    Path:      {pgdata_path}")
        print(f"    Name:      {env.get('DB_NAME', 'datasets')}")
        print(f"    Port:      {env.get('POSTGRES_PORT', '5432')}")
        print(f"    Running:   {'yes' if 'postgres' in running_services else 'no'}")

        state_dir = pgdata / "state_tracking"
        _print_ingestion_state(state_dir, "_postgres_", "postgres")

    # MongoDB
    if "mongo" in configured:
        mongo_data_path = env.get("MONGO_DATA_PATH", "./data/database/mongo")
        mongo_data = Path(mongo_data_path)
        if not mongo_data.is_absolute():
            mongo_data = ROOT / mongo_data

        print(f"\n  MongoDB:")
        print(f"    Path:      {mongo_data_path}")
        print(f"    Port:      {env.get('MONGO_PORT', '27017')}")
        print(f"    Running:   {'yes' if 'mongo' in running_services else 'no'}")

        state_dir = mongo_data / "state_tracking"
        _print_ingestion_state(state_dir, "_mongo_", "mongo")

    # Export path
    export_path = env.get("DB_EXPORT_PATH")
    if export_path:
        print(f"\n  Export:")
        print(f"    Host path:      {export_path}")
        print(f"    Container path: /export")

    # MCP servers
    mcp_config = _load_mcp_config()
    if mcp_config:
        pg_mcp = mcp_config.get("postgres", {})
        if pg_mcp.get("enabled"):
            port = pg_mcp.get("port", 8000)
            access = pg_mcp.get("access_mode", "restricted")
            running = "yes" if "postgres-mcp" in running_services else "no"
            print(f"\n  PostgreSQL MCP:")
            print(f"    Port:      {port}")
            print(f"    Access:    {access}")
            print(f"    Endpoint:  http://<server_ip>:{port}/sse (type: sse)")
            print(f"    Running:   {running}")

        mongo_mcp = mcp_config.get("mongo", {})
        if mongo_mcp.get("enabled"):
            port = mongo_mcp.get("port", 3000)
            read_only = mongo_mcp.get("read_only", True)
            running = "yes" if "mongo-mcp" in running_services else "no"
            print(f"\n  MongoDB MCP:")
            print(f"    Port:      {port}")
            print(f"    Read-only: {read_only}")
            print(f"    Endpoint:  http://<server_ip>:{port}/mcp (type: http)")
            print(f"    Running:   {running}")

    print()
    return 0


def _load_classifier_suffixes():
    """Load classifier name → table suffix mapping from postgres_ml services config."""
    import yaml
    services_path = CONFIG_DIR / "postgres_ml" / "services.yaml"
    if not services_path.exists():
        return {}
    try:
        cfg = yaml.safe_load(services_path.read_text()) or {}
        return {
            name: cls.get("suffix", f"_{name}")
            for name, cls in cfg.get("classifiers", {}).items()
        }
    except (OSError, yaml.YAMLError):
        return {}


def _print_ingestion_state(state_dir, label_split, db_type):
    """Print ingestion state from JSON files in a state directory."""
    from social_data_pipeline.setup.utils import load_source_config

    if not state_dir.exists():
        print(f"\n    No ingestion data yet.")
        return

    state_files = sorted(state_dir.glob("*.json"))
    if not state_files:
        print(f"\n    No ingestion data yet.")
        return

    # Cache platform configs per source
    platform_cache = {}
    classifier_suffixes = None  # lazy-loaded

    # Collect entries grouped by source
    source_entries = {}  # {source: [(label, count, latest, in_progress, failed, last_updated), ...]}

    for sf in state_files:
        try:
            sdata = json.loads(sf.read_text())
        except (json.JSONDecodeError, OSError):
            continue

        name = sf.stem
        processed = sdata.get("processed", [])
        failed = sdata.get("failed", [])
        in_progress = sdata.get("in_progress")
        last_updated = sdata.get("last_updated", "")

        parts = name.split(label_split)
        if len(parts) == 2:
            source = parts[0]
            profile_and_dt = parts[1]  # e.g., "ingest_comments" or "ml_comments"
            profile, _, data_type = profile_and_dt.partition("_")
        else:
            source = name
            profile = ""
            data_type = name

        # Load platform config for this source
        if source not in platform_cache:
            platform_cache[source] = load_source_config(source) or {}
        pconfig = platform_cache[source]

        if source not in source_entries:
            source_entries[source] = []

        if profile == "ml" and db_type == "postgres":
            # Group ML entries by classifier from processed entry prefixes
            if classifier_suffixes is None:
                classifier_suffixes = _load_classifier_suffixes()

            # Group processed entries by classifier (prefix before '/')
            by_classifier = {}
            for entry in processed:
                if "/" in entry:
                    cls_name, dataset = entry.split("/", 1)
                else:
                    cls_name, dataset = "unknown", entry
                by_classifier.setdefault(cls_name, []).append(dataset)

            # Group failed entries by classifier too
            failed_by_classifier = {}
            for f in failed:
                fname = f.get("filename", "") if isinstance(f, dict) else str(f)
                if "/" in fname:
                    cls_name = fname.split("/", 1)[0]
                else:
                    cls_name = "unknown"
                failed_by_classifier.setdefault(cls_name, []).append(f)

            schema = pconfig.get("db_schema", source)
            for cls_name, datasets in sorted(by_classifier.items()):
                suffix = classifier_suffixes.get(cls_name, f"_{cls_name}")
                table = f"{schema}.{data_type}{suffix}"
                count = len(datasets)
                latest = datasets[-1] if datasets else "-"
                cls_failed = failed_by_classifier.get(cls_name, [])
                tag = f"classifier: {cls_name}"
                source_entries[source].append(
                    (tag, table, count, latest, in_progress, cls_failed, last_updated)
                )
        else:
            # Main data (ingest profile)
            if db_type == "postgres":
                schema = pconfig.get("db_schema", source)
                table = f"{schema}.{data_type}"
            else:
                if "mongo_db_name" in pconfig:
                    table = pconfig["mongo_db_name"]
                else:
                    template = pconfig.get("mongo_db_name_template", "{platform}_{data_type}")
                    platform = pconfig.get("platform", source).replace("/", "_")
                    table = template.format(platform=platform, data_type=data_type)

            count = len(processed)
            latest = processed[-1] if processed else "-"
            tag = "main data" if db_type == "postgres" else ""
            source_entries[source].append(
                (tag, table, count, latest, in_progress, failed, last_updated)
            )

    # Print grouped by source
    print(f"\n    Ingestion status:")
    for source, entries in sorted(source_entries.items()):
        print(f"      platform: {source}")
        for tag, table, count, latest, in_progress, failed, last_updated in entries:
            line = f"        - {table}: {count} datasets"
            if tag:
                line += f" ({tag})"
            if in_progress:
                line += f" [in progress: {in_progress}]"
            if failed:
                line += f" [{len(failed)} failed]"
            print(line)
            details = []
            if latest != "-":
                details.append(f"latest: {latest}")
            if last_updated:
                details.append(f"updated: {last_updated[:10]}")
            if details:
                print(f"            {', '.join(details)}")


# ============================================================================
# sdp db unsetup
# ============================================================================

# Config files generated by db setup
DB_GENERATED_FILES = [
    "config/db/postgres.yaml",
    "config/db/mongo.yaml",
    "config/db/mcp.yaml",
    "config/postgres/postgresql.local.conf",
    "config/postgres/pg_hba.local.conf",
    "docker-compose.override.yml",
    ".env",
]


def cmd_db_unsetup(args):
    """Remove database configuration and optionally database data."""
    env = load_env()

    print()
    print("  Social Data Pipeline - Database Unsetup")
    print("  ======================================")
    print()

    # --- Find config files to remove ---
    removed = []
    for rel in DB_GENERATED_FILES:
        path = ROOT / rel
        if path.exists():
            removed.append(rel)

    # Also find .bak files
    backups = []
    for rel in DB_GENERATED_FILES:
        bak = ROOT / (rel + ".bak")
        if bak.exists():
            backups.append(rel + ".bak")

    if removed or backups:
        print("  Database config files to remove:")
        for rel in removed + backups:
            print(f"    {rel}")
        print()
    else:
        print("  No database config files found.\n")

    # --- PostgreSQL data removal (double confirmation) ---
    pgdata_path = env.get("PGDATA_PATH", "")
    db_removed = False

    tablespace_paths = {}
    try:
        import yaml
        for cfg_name in ("pipeline.yaml", "user.yaml"):
            cfg_file = CONFIG_DIR / "postgres" / cfg_name
            if cfg_file.exists():
                cfg = yaml.safe_load(cfg_file.read_text()) or {}
                ts = cfg.get("tablespaces") or cfg.get("pipeline", {}).get("tablespaces")
                if ts and isinstance(ts, dict):
                    tablespace_paths.update(ts)
        # Also check config/db/postgres.yaml
        pg_db_config = CONFIG_DIR / "db" / "postgres.yaml"
        if pg_db_config.exists():
            cfg = yaml.safe_load(pg_db_config.read_text()) or {}
            ts = cfg.get("tablespaces")
            if ts and isinstance(ts, dict):
                tablespace_paths.update(ts)
    except Exception:
        pass

    if pgdata_path:
        pgdata = Path(pgdata_path)
        if not pgdata.is_absolute():
            pgdata = ROOT / pgdata
        if pgdata.exists():
            print(f"  Database directory: {pgdata_path}")
            if tablespace_paths:
                print(f"  Tablespace directories:")
                for ts_name, ts_path in tablespace_paths.items():
                    print(f"    {ts_name}: {ts_path}")
            print()
            confirm1 = input("  Delete the PostgreSQL database? This CANNOT be undone [y/N]: ").strip().lower()
            if confirm1 in ("y", "yes"):
                confirm2 = input("  Are you SURE? All database data will be permanently lost [y/N]: ").strip().lower()
                if confirm2 in ("y", "yes"):
                    print()
                    print("  Stopping PostgreSQL...")
                    docker_compose("--profile", "postgres", "down", "--timeout", "30")
                    print("  Fixing directory permissions...")
                    # Mount the parent directory so we can chown both the parent and pgdata
                    db_parent = pgdata.resolve().parent
                    volume_args = ["-v", f"{db_parent}:/dbparent"]
                    chown_paths = ["/dbparent"]
                    for ts_name, ts_path in tablespace_paths.items():
                        ts_dir = Path(ts_path)
                        if not ts_dir.is_absolute():
                            ts_dir = ROOT / ts_dir
                        if ts_dir.exists():
                            volume_args += ["-v", f"{ts_dir.resolve()}:/tablespace/{ts_name}"]
                            chown_paths.append(f"/tablespace/{ts_name}")
                    uid_gid = f"{os.getuid()}:{os.getgid()}"
                    subprocess.run(
                        ["docker", "run", "--rm"] + volume_args +
                        ["postgres:18", "chown", "-R", uid_gid] + chown_paths,
                        cwd=ROOT,
                    )
                    print(f"  Removing {pgdata}...")
                    shutil.rmtree(pgdata)
                    for ts_name, ts_path in tablespace_paths.items():
                        ts_dir = Path(ts_path)
                        if not ts_dir.is_absolute():
                            ts_dir = ROOT / ts_dir
                        if ts_dir.exists():
                            print(f"  Removing tablespace '{ts_name}' at {ts_dir}...")
                            shutil.rmtree(ts_dir)
                    db_removed = True
                    print("  Database removed.")
                else:
                    print("  Database removal skipped.")
            else:
                print("  Database removal skipped.")
            print()

    # --- MongoDB data removal ---
    mongo_data_path = env.get("MONGO_DATA_PATH", "")
    mongo_removed = False

    if mongo_data_path:
        mongo_data = Path(mongo_data_path)
        if not mongo_data.is_absolute():
            mongo_data = ROOT / mongo_data
        if mongo_data.exists():
            print(f"  MongoDB data directory: {mongo_data_path}")
            print()
            confirm1 = input("  Delete the MongoDB data? This CANNOT be undone [y/N]: ").strip().lower()
            if confirm1 in ("y", "yes"):
                confirm2 = input("  Are you SURE? All MongoDB data will be permanently lost [y/N]: ").strip().lower()
                if confirm2 in ("y", "yes"):
                    print()
                    print("  Stopping MongoDB...")
                    docker_compose("--profile", "mongo", "down", "--timeout", "30")
                    print("  Fixing directory permissions...")
                    mongo_parent = mongo_data.resolve().parent
                    uid_gid = f"{os.getuid()}:{os.getgid()}"
                    subprocess.run(
                        ["docker", "run", "--rm",
                         "-v", f"{mongo_parent}:/dbparent",
                         "mongo:8", "chown", "-R", uid_gid, "/dbparent"],
                        cwd=ROOT,
                    )
                    print(f"  Removing {mongo_data}...")
                    shutil.rmtree(mongo_data)
                    mongo_removed = True
                    print("  MongoDB data removed.")
                else:
                    print("  MongoDB data removal skipped.")
            else:
                print("  MongoDB data removal skipped.")
            print()

    # --- Remove RO credentials from data directories ---
    for data_path_str in (pgdata_path, mongo_data_path):
        if not data_path_str:
            continue
        dp = Path(data_path_str)
        if not dp.is_absolute():
            dp = ROOT / dp
        cred_file = dp / ".ro_credentials"
        if cred_file.exists():
            try:
                cred_file.unlink()
                print(f"  Removed {cred_file}")
            except PermissionError:
                # File owned by container user (root) — fix via docker
                subprocess.run(
                    ["docker", "run", "--rm",
                     "-v", f"{cred_file.resolve().parent}:/data",
                     "alpine", "rm", "-f", "/data/.ro_credentials"],
                    cwd=ROOT, capture_output=True,
                )
                if not cred_file.exists():
                    print(f"  Removed {cred_file}")
                else:
                    print(f"  [!] Could not remove {cred_file} — run: sudo rm {cred_file}")

    # --- Remove config files ---
    if removed or backups:
        for rel in removed + backups:
            (ROOT / rel).unlink()
        print(f"  Removed {len(removed) + len(backups)} config file(s).")
    else:
        print("  Nothing to remove.")

    # --- Print remaining data paths ---
    data_paths = {}
    if not db_removed and pgdata_path:
        pgdata = Path(pgdata_path)
        if not pgdata.is_absolute():
            pgdata = ROOT / pgdata
        if pgdata.exists():
            data_paths["PostgreSQL data"] = pgdata_path
            for ts_name, ts_path in tablespace_paths.items():
                data_paths[f"Tablespace '{ts_name}'"] = ts_path
    if not mongo_removed and mongo_data_path:
        mongo_data = Path(mongo_data_path)
        if not mongo_data.is_absolute():
            mongo_data = ROOT / mongo_data
        if mongo_data.exists():
            data_paths["MongoDB data"] = mongo_data_path

    if data_paths:
        print()
        print("  Data files were NOT removed. Delete manually if needed:")
        for label, path in data_paths.items():
            print(f"    {label}: {path}")

    print()
    print("  Database unsetup complete.\n")
    return 0


# ============================================================================
# sdp db recover-password
# ============================================================================

def cmd_db_recover_password(args):
    """Reset database admin password by temporarily using trust auth."""
    env = load_env()

    if not _is_auth_enabled():
        print("\n  Authentication is not enabled. Nothing to recover.\n")
        return 0

    print()
    print("  Social Data Pipeline - Password Recovery")
    print("  ========================================")
    print()
    print("  This will temporarily restart PostgreSQL with trust auth,")
    print("  set a new admin password, and restore scram-sha-256 auth.")
    print()

    _tag = lambda t: f"[{t}] " if os.environ.get('SDP_TAGGED_MODE') else ""
    new_password = getpass(f"  {_tag('sdp_recover_password')}New admin password: ")
    if not new_password:
        print("  Error: Password cannot be empty.")
        return 1
    confirm = getpass(f"  {_tag('sdp_recover_password_confirm')}Confirm new password: ")
    if new_password != confirm:
        print("  Error: Passwords do not match.")
        return 1

    configured = _get_configured_db_services()

    # --- PostgreSQL recovery ---
    if "postgres" in configured:
        pg_config = _load_db_yaml("postgres")
        if pg_config.get("auth"):
            print("\n  Recovering PostgreSQL password...")

            pg_hba_local = CONFIG_DIR / "postgres" / "pg_hba.local.conf"
            pg_hba_backup = CONFIG_DIR / "postgres" / "pg_hba.local.conf.recovery_bak"

            if not pg_hba_local.exists():
                print("  Error: pg_hba.local.conf not found. Is auth configured?")
                return 1

            # Backup current pg_hba and replace with trust-only version
            shutil.copy2(pg_hba_local, pg_hba_backup)
            trust_hba = (
                "# TEMPORARY — trust auth for password recovery\n"
                "local   all   all   trust\n"
                "host    all   all   0.0.0.0/0   trust\n"
            )
            pg_hba_local.write_text(trust_hba)

            # Restart postgres with trust auth
            print("  Restarting PostgreSQL with trust auth...")
            docker_compose("--profile", "postgres", "restart")

            # Wait for healthy
            print("  Waiting for PostgreSQL to be ready...")
            port = env.get("POSTGRES_PORT", "5432")
            db_name = env.get("DB_NAME", "datasets")
            subprocess.run(
                ["docker", "compose", "exec", "postgres",
                 "pg_isready", "-U", "postgres", "-d", db_name, "-p", port, "--timeout=30"],
                cwd=ROOT,
            )

            # Set new password (use PGPASSWORD env to avoid shell interpolation of password)
            escaped_pw = new_password.replace("'", "''")
            alter_cmd = f"ALTER USER postgres WITH PASSWORD '{escaped_pw}'"
            result = subprocess.run(
                ["docker", "compose", "exec", "postgres",
                 "psql", "-U", "postgres", "-p", port, "-c", alter_cmd],
                cwd=ROOT,
            )
            if result.returncode != 0:
                print("  Error: Failed to set new password.")
                # Restore backup
                shutil.copy2(pg_hba_backup, pg_hba_local)
                pg_hba_backup.unlink(missing_ok=True)
                docker_compose("--profile", "postgres", "restart")
                return 1

            # Restore original pg_hba
            shutil.copy2(pg_hba_backup, pg_hba_local)
            pg_hba_backup.unlink(missing_ok=True)

            # Restart with proper auth
            print("  Restoring scram-sha-256 auth and restarting...")
            docker_compose("--profile", "postgres", "restart")
            print("  PostgreSQL password updated successfully.")

    # --- MongoDB recovery ---
    if "mongo" in configured:
        mongo_config = _load_db_yaml("mongo")
        if mongo_config.get("auth"):
            print("\n  Recovering MongoDB password...")
            mongo_admin_user = env.get("MONGO_ADMIN_USER", "admin")

            # Stop mongo
            print("  Stopping MongoDB...")
            docker_compose("--profile", "mongo", "down", "--timeout", "30")

            # Start mongo without auth temporarily
            mongo_data_path = env.get("MONGO_DATA_PATH", "./data/database/mongo")
            mongo_cache = env.get("MONGO_CACHE_SIZE_GB", "2")

            print("  Starting MongoDB without auth for recovery...")
            result = subprocess.run(
                ["docker", "run", "--rm", "-d",
                 "--name", "sdp-mongo-recovery",
                 "-v", f"{mongo_data_path}/db:/data/db",
                 "mongo:8",
                 "mongod", "--bind_ip", "0.0.0.0",
                 "--wiredTigerCacheSizeGB", mongo_cache],
                cwd=ROOT, capture_output=True, text=True,
            )
            if result.returncode != 0:
                print(f"  Error: Failed to start recovery container: {result.stderr}")
                return 1

            # Wait for mongod to be ready
            import time
            for _ in range(15):
                check = subprocess.run(
                    ["docker", "exec", "sdp-mongo-recovery",
                     "mongosh", "--quiet", "--eval", "db.adminCommand('ping')"],
                    capture_output=True, text=True,
                )
                if check.returncode == 0:
                    break
                time.sleep(2)

            # Update password (escape single quotes for JS string literal)
            escaped_user = mongo_admin_user.replace("'", "\\'")
            escaped_pw = new_password.replace("\\", "\\\\").replace("'", "\\'")
            update_script = (
                "db = db.getSiblingDB('admin');"
                f"db.changeUserPassword('{escaped_user}', '{escaped_pw}');"
                "print('Password updated');"
            )
            result = subprocess.run(
                ["docker", "exec", "sdp-mongo-recovery",
                 "mongosh", "--quiet", "--eval", update_script],
                cwd=ROOT,
            )

            # Stop recovery container
            subprocess.run(["docker", "stop", "sdp-mongo-recovery"],
                         capture_output=True)

            if result.returncode != 0:
                print("  Error: Failed to update MongoDB password.")
                return 1

            print("  MongoDB password updated successfully.")

            # Restart mongo with auth
            print("  Restarting MongoDB with auth...")
            os.environ["MONGO_ADMIN_PASSWORD"] = new_password
            docker_compose("--profile", "mongo", "up", "-d")

    print("\n  Password recovery complete.")
    print("  IMPORTANT: Remember your new password — it is not stored anywhere.\n")
    return 0


# ============================================================================
# sdp db create-indexes
# ============================================================================

def _format_duration(seconds):
    """Format seconds into a human-readable duration string."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes = int(seconds // 60)
    secs = seconds % 60
    return f"{minutes}m {secs:.0f}s"


def _psql_query(port, db_name, query, password=None):
    """Run a psql query via docker compose exec and return rows as lists of strings."""
    env = dict(os.environ)
    if password:
        env["PGPASSWORD"] = password
    result = subprocess.run(
        ["docker", "compose", "exec", "-T", "postgres",
         "psql", "-U", "postgres", "-p", str(port), "-d", db_name,
         "-t", "-A", "-F", "\t", "-c", query],
        cwd=ROOT, capture_output=True, text=True, env=env,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip())
    lines = [line for line in result.stdout.strip().splitlines() if line.strip()]
    return [line.split("\t") for line in lines]


def _psql_exec(port, db_name, statement, password=None):
    """Execute a psql statement via docker compose exec. Returns (success, stderr)."""
    env = dict(os.environ)
    if password:
        env["PGPASSWORD"] = password
    result = subprocess.run(
        ["docker", "compose", "exec", "-T", "postgres",
         "psql", "-U", "postgres", "-p", str(port), "-d", db_name,
         "-c", statement],
        cwd=ROOT, capture_output=True, text=True, env=env,
    )
    return result.returncode == 0, result.stderr.strip()


def _mongosh_eval(port, script, password=None):
    """Run a mongosh script via docker compose exec and return stdout."""
    cmd = ["docker", "compose", "exec", "-T", "mongo", "mongosh", "--quiet"]
    env_vars = load_env()
    if password:
        mongo_user = env_vars.get("MONGO_ADMIN_USER", "admin")
        cmd += ["-u", mongo_user, "-p", password, "--authenticationDatabase", "admin"]
    cmd += ["--eval", script]
    result = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip())
    return result.stdout.strip()


def _interactive_pg_indexes(source, platform_config, password):
    """Interactive PostgreSQL index creation. Returns {table: [new_fields]} for config persistence."""
    from social_data_pipeline.setup.utils import ask_multi_select, ask_list, section_header
    import time
    import yaml

    section_header("PostgreSQL Index Creation")

    db_yaml = _load_db_yaml("postgres")
    port = int(db_yaml.get("port", 5432))
    db_name = db_yaml.get("name", "datasets")
    schema = platform_config.get("db_schema", source)

    # Load parallel_index_workers from source postgres config if available
    source_pg_path = CONFIG_DIR / "sources" / source / "postgres.yaml"
    parallel_workers = 8
    if source_pg_path.exists():
        try:
            source_pg = yaml.safe_load(source_pg_path.read_text()) or {}
            parallel_workers = source_pg.get("processing", {}).get("parallel_index_workers", 8)
        except Exception:
            pass

    try:
        rows = _psql_query(port, db_name,
            f"SELECT table_name FROM information_schema.tables "
            f"WHERE table_schema = '{schema}' AND table_type = 'BASE TABLE' ORDER BY table_name",
            password)
        tables = [r[0] for r in rows]
    except Exception as e:
        print(f"\n  Could not connect to PostgreSQL: {e}")
        print("  Is it running? (sdp db start)\n")
        return {}

    if not tables:
        print(f"\n  No tables found in schema '{schema}'.\n")
        return {}

    print(f"  Schema: {schema}")
    selected_tables = ask_multi_select("Select tables to create indexes on", tables)

    created = {}  # {table: [fields]}

    for table in selected_tables:
        print(f"\n  --- {table} ---")

        # Get columns
        try:
            rows = _psql_query(port, db_name,
                f"SELECT column_name FROM information_schema.columns "
                f"WHERE table_schema = '{schema}' AND table_name = '{table}' "
                f"ORDER BY ordinal_position", password)
            columns = [r[0] for r in rows]
            if columns:
                print(f"  Columns: {', '.join(columns)}")
        except Exception:
            pass

        # Get existing indexes
        try:
            rows = _psql_query(port, db_name,
                f"SELECT indexname FROM pg_indexes "
                f"WHERE schemaname = '{schema}' AND tablename = '{table}' "
                f"ORDER BY indexname", password)
            existing = [r[0] for r in rows]
            if existing:
                print(f"  Existing indexes: {', '.join(existing)}")
            else:
                print("  Existing indexes: (none)")
        except Exception:
            print("  Existing indexes: (could not query)")

        fields = ask_list("New indexes (comma-separated field names, empty to skip)", default=[])
        if not fields:
            continue

        table_created = []
        for field in fields:
            index_name = f"idx_{table}_{field}"
            print(f"  Creating index: {index_name} ...", end=" ", flush=True)
            t_start = time.time()

            # Set session params for parallel index builds, then create index
            statement = (
                f"SET maintenance_work_mem = '2GB'; "
                f"SET max_parallel_maintenance_workers = {parallel_workers}; "
                f"CREATE INDEX IF NOT EXISTS {index_name} ON {schema}.{table} ({field});"
            )
            success, stderr = _psql_exec(port, db_name, statement, password)
            duration = time.time() - t_start

            if success:
                print(f"done ({_format_duration(duration)})")
                table_created.append(field)
            else:
                print(f"failed: {stderr}")

        if table_created:
            created[table] = table_created

    return created


def _interactive_mongo_indexes(source, platform_config, password):
    """Interactive MongoDB index creation. Returns {data_type: [new_fields]} for config persistence."""
    from social_data_pipeline.setup.utils import ask_multi_select, ask_list, section_header
    import json
    import time

    section_header("MongoDB Index Creation")

    data_types = platform_config.get("data_types", [])
    if not data_types:
        print("\n  No data types configured.\n")
        return {}

    # Determine platform string for db name resolution
    if source == "reddit":
        platform = "reddit"
    else:
        platform = f"custom/{source}"

    # Resolve db names per data_type using the same logic as the orchestrator
    def get_db_name(dt):
        if 'mongo_db_name' in platform_config:
            return platform_config['mongo_db_name']
        template = platform_config.get('mongo_db_name_template', '{platform}_{data_type}')
        safe_platform = platform.replace('/', '_')
        return template.format(platform=safe_platform, data_type=dt)

    # Discover collections per data_type via mongosh
    all_dt_collections = {}  # {data_type: {db_name, collections}}

    for dt in data_types:
        db_name = get_db_name(dt)
        try:
            # Query _sdp_metadata for collections belonging to this data_type
            script = (
                f"use('{db_name}'); "
                f"let meta = db.getCollection('_sdp_metadata'); "
                f"let count = meta.estimatedDocumentCount(); "
                f"if (count > 0) {{ "
                f"  let docs = meta.distinct('collection', {{data_type: '{dt}'}}); "
                f"  print(JSON.stringify(docs)); "
                f"}} else {{ "
                f"  let colls = db.getCollectionNames().filter(n => !n.startsWith('_')); "
                f"  print(JSON.stringify(colls)); "
                f"}}"
            )
            output = _mongosh_eval(0, script, password)  # port unused, goes through docker exec
            collections = json.loads(output) if output else []
            collections.sort()
        except Exception as e:
            print(f"\n  Could not connect to MongoDB: {e}")
            print("  Is it running? (sdp db start)\n")
            return {}

        if collections:
            all_dt_collections[dt] = {"db_name": db_name, "collections": collections}

    if not all_dt_collections:
        print("\n  No collections found in MongoDB.\n")
        return {}

    # Display available collections
    print()
    for dt, info in all_dt_collections.items():
        print(f"  {info['db_name']}:")
        for coll in info['collections'][:5]:
            print(f"    - {coll}")
        remaining = len(info['collections']) - 5
        if remaining > 0:
            print(f"    ... and {remaining} more")

    # Ask which data types to index
    dt_options = [f"{dt} ({len(info['collections'])} collections)" for dt, info in all_dt_collections.items()]
    dt_keys = list(all_dt_collections.keys())
    selected_labels = ask_multi_select("Which data types to index?", dt_options)
    selected_dts = [dt_keys[dt_options.index(label)] for label in selected_labels]

    created = {}  # {data_type: [fields]}

    for dt in selected_dts:
        info = all_dt_collections[dt]
        db_name = info['db_name']
        collections = info['collections']
        n_colls = len(collections)

        print(f"\n  --- {dt} ({n_colls} collections) ---")

        # Show existing indexes from first collection
        if collections:
            try:
                script = (
                    f"use('{db_name}'); "
                    f"let idxs = db.getCollection('{collections[0]}').getIndexes(); "
                    f"let names = idxs.map(i => i.name).filter(n => n !== '_id_'); "
                    f"print(JSON.stringify(names));"
                )
                output = _mongosh_eval(0, script, password)
                existing = json.loads(output) if output else []
                if existing:
                    print(f"  Existing indexes (on {collections[0]}): {', '.join(existing)}")
                else:
                    print("  Existing indexes: (none)")
            except Exception:
                print("  Existing indexes: (could not query)")

        fields = ask_list("New indexes (comma-separated field names, empty to skip)", default=[])
        if not fields:
            continue

        dt_created = []
        for field in fields:
            print(f"  Creating index {field}_1 on {n_colls} collections...")
            t_start = time.time()
            success = 0
            for coll in collections:
                try:
                    script = (
                        f"use('{db_name}'); "
                        f"db.getCollection('{coll}').createIndex("
                        f"{{'{field}': 1}}, {{name: '{field}_1'}});"
                    )
                    _mongosh_eval(0, script, password)
                    success += 1
                except Exception as e:
                    print(f"    Warning: Failed on {coll}: {e}")
            duration = time.time() - t_start
            print(f"  Done: {field}_1 ({success}/{n_colls} collections, {_format_duration(duration)})")
            dt_created.append(field)

        if dt_created:
            created[dt] = dt_created

    return created


def _persist_indexes_to_config(source, pg_created, mongo_created):
    """Merge newly created indexes into platform.yaml."""
    import yaml

    platform_path = CONFIG_DIR / "sources" / source / "platform.yaml"
    config = yaml.safe_load(platform_path.read_text()) or {}

    if pg_created:
        indexes = config.setdefault("indexes", {})
        for table, fields in pg_created.items():
            existing = indexes.setdefault(table, [])
            for f in fields:
                if f not in existing:
                    existing.append(f)

    if mongo_created:
        mongo_indexes = config.setdefault("mongo_indexes", {})
        for dt, fields in mongo_created.items():
            existing = mongo_indexes.setdefault(dt, [])
            for f in fields:
                if f not in existing:
                    existing.append(f)

    platform_path.write_text(yaml.safe_dump(config, default_flow_style=False, sort_keys=False))
    print(f"  Updated: {platform_path}")


def cmd_db_create_indexes(args):
    """Interactively create database indexes for a source."""
    from social_data_pipeline.setup.utils import resolve_source, load_source_config, ask_choice, ask_bool

    source = resolve_source(args.source)
    platform_config = load_source_config(source)
    if not platform_config:
        print(f"\n  Error: Could not load platform config for '{source}'.\n")
        return 1

    configured = _get_configured_db_services()
    if not configured:
        print("\n  No databases configured. Run 'sdp db setup' first.\n")
        return 1

    # Filter to DBs that the source actually uses (has profile config for)
    from social_data_pipeline.setup.utils import get_source_profiles
    source_profiles = get_source_profiles(source)
    source_dbs = []
    if any(p in source_profiles for p in ("postgres_ingest", "postgres_ml")):
        source_dbs.append("postgres")
    if "mongo_ingest" in source_profiles:
        source_dbs.append("mongo")
    available = [db for db in configured if db in source_dbs]

    if not available:
        print(f"\n  Source '{source}' has no database profiles configured.\n")
        return 1

    # Determine which DB(s) to target
    if len(available) == 1:
        targets = available
        db_label = "PostgreSQL" if "postgres" in available else "MongoDB"
        print(f"\n  Database: {db_label}")
    else:
        choice = ask_choice("Which database?", ["PostgreSQL", "MongoDB", "Both"], default="Both", tag="sdp_idx_database")
        if choice == "PostgreSQL":
            targets = ["postgres"]
        elif choice == "MongoDB":
            targets = ["mongo"]
        else:
            targets = available

    # Prompt for password if needed
    env = load_env()
    password = None
    needs_pg_auth = "postgres" in targets and env.get("POSTGRES_AUTH_ENABLED") == "true"
    needs_mongo_auth = "mongo" in targets and env.get("MONGO_AUTH_ENABLED") == "true"
    if needs_pg_auth or needs_mongo_auth:
        password = _prompt_db_password(tag="sdp_db_password")

    pg_created = {}
    mongo_created = {}

    if "postgres" in targets:
        pg_created = _interactive_pg_indexes(source, platform_config, password)

    if "mongo" in targets:
        mongo_created = _interactive_mongo_indexes(source, platform_config, password)

    # Summary
    total = sum(len(v) for v in pg_created.values()) + sum(len(v) for v in mongo_created.values())
    if total == 0:
        print("\n  No new indexes created.\n")
        return 0

    print(f"\n  Created {total} new index(es).")

    if ask_bool("Save new indexes to platform.yaml?", default=False, tag="sdp_idx_save"):
        _persist_indexes_to_config(source, pg_created, mongo_created)

    print()
    return 0


# ============================================================================
# sdp source add
# ============================================================================

def cmd_source_add(args):
    """Add a new source (interactive setup)."""
    from social_data_pipeline.setup.source import main as source_main
    source_main(source_name=args.name, hf_dataset_id=getattr(args, 'hf_dataset', None))
    return 0


# ============================================================================
# sdp source download
# ============================================================================

def cmd_source_download(args):
    """Download HF dataset files for a source."""
    import os
    from social_data_pipeline.setup.utils import load_source_config
    from social_data_pipeline.setup.hf import (
        fetch_parquet_urls, download_hf_files, organize_hf_downloads, HFAPIError,
    )

    source_name = args.name
    source_config = load_source_config(source_name)
    if source_config is None:
        print(f"\n  Error: Source '{source_name}' not found in config/sources/\n")
        return 1

    hf_dataset = source_config.get('hf_dataset')
    if not hf_dataset:
        print(f"\n  Error: Source '{source_name}' has no hf_dataset configured.")
        print(f"  Use 'sdp source add {source_name} --hf <dataset_id>' to set up an HF source.\n")
        return 1

    token = getattr(args, 'token', None) or os.environ.get('HF_TOKEN')
    dumps_dir = source_config.get('paths', {}).get('dumps')
    extracted_dir = source_config.get('paths', {}).get('extracted')
    if not dumps_dir or not extracted_dir:
        print(f"\n  Error: No dumps/extracted path configured for source '{source_name}'.\n")
        return 1

    config_map = source_config.get('hf_config_map', {})
    if not config_map:
        # Fallback: each data_type maps to a config with the same name
        data_types = source_config.get('data_types', [])
        config_map = {dt: [dt] for dt in data_types}

    # Filter by --data-type if specified
    if args.data_type:
        if args.data_type not in config_map:
            print(f"\n  Error: data type '{args.data_type}' not found. "
                  f"Available: {', '.join(config_map.keys())}\n")
            return 1
        config_map = {args.data_type: config_map[args.data_type]}

    print(f"\n  Source:  {source_name}")
    print(f"  Dataset: {hf_dataset}")
    print(f"  Dumps:   {dumps_dir}")
    print(f"  Extract: {extracted_dir}")
    print(f"  Data types: {', '.join(config_map.keys())}")

    try:
        # Phase 1: Download 1-to-1 mirror to dumps/
        print(f"\n  --- Downloading from HF Hub ---\n")
        parquet_urls = fetch_parquet_urls(hf_dataset, token=token)
        download_hf_files(parquet_urls, dumps_dir, dataset_id=hf_dataset, token=token)

        # Phase 2: Organize into extracted/<data_type>/ using config_map
        print(f"\n  --- Organizing into data types ---\n")
        organize_hf_downloads(dumps_dir, extracted_dir, config_map)

    except HFAPIError as e:
        print(f"\n  Error: {e}\n")
        return 1

    print(f"\n  Next step: python sdp.py run parse --source {source_name}\n")
    return 0


# ============================================================================
# sdp source configure
# ============================================================================

def cmd_source_configure(args):
    """Reconfigure an existing source (platform-specific customization)."""
    from social_data_pipeline.setup.utils import load_source_config

    source_name = args.name
    source_config = load_source_config(source_name)
    if source_config is None:
        print(f"\n  Error: Source '{source_name}' not found in config/sources/\n")
        return 1

    platform = source_config.get("platform", "")
    if platform == "reddit" or source_name == "reddit":
        from social_data_pipeline.setup.reddit import main as reddit_main
        from social_data_pipeline.setup.source import main as source_main
        from social_data_pipeline.setup.utils import (
            get_source_profiles, load_db_setup, ask_bool,
        )
        reddit_main(source_name=source_name)
        # After reddit platform config, offer to add any missing profile configs.
        # Use source add (with new-profiles-only defaults) so postgres/mongo/ml
        # can be added without touching existing working configurations.
        db_setup = load_db_setup()
        if db_setup:
            existing = get_source_profiles(source_name)
            databases = db_setup.get("databases", [])
            available = ["parse", "lingua", "ml"]
            if "postgres" in databases:
                available += ["postgres_ingest", "postgres_ml"]
            if "mongo" in databases:
                available += ["mongo_ingest"]
            missing = [p for p in available if p not in existing]
            if missing:
                print(f"\n  Profiles not yet configured: {', '.join(missing)}")
                if ask_bool("Add missing profile configurations now?", True, tag="sdp_add_missing_profiles"):
                    source_main(source_name=source_name)
    else:
        # For custom platforms, re-run source setup (also updates .env paths)
        from social_data_pipeline.setup.source import main as source_main
        source_main(source_name=source_name)

    return 0


# ============================================================================
# sdp source add-classifiers
# ============================================================================

def cmd_source_add_classifiers(args):
    """Add ML classifiers for a source."""
    from social_data_pipeline.setup.classifiers import main as classifiers_main
    classifiers_main(source_name=args.name)
    return 0


# ============================================================================
# sdp source remove
# ============================================================================

def cmd_source_remove(args):
    """Remove source configuration."""
    source_dir = CONFIG_DIR / "sources" / args.name
    if not source_dir.exists():
        print(f"\n  Error: Source '{args.name}' not found in config/sources/\n")
        return 1

    print(f"\n  Source: {args.name}")
    print(f"  Directory: config/sources/{args.name}/")

    # List files
    files = sorted(source_dir.glob("*"))
    if files:
        print(f"  Files:")
        for f in files:
            print(f"    {f.name}")
    print()

    confirm = input(f"  Remove source '{args.name}' configuration? [y/N]: ").strip().lower()
    if confirm not in ("y", "yes"):
        print("  Aborted.\n")
        return 0

    shutil.rmtree(source_dir)
    print(f"\n  Removed config/sources/{args.name}/")
    print("  Note: Data files (dumps, csv, output) were NOT removed.\n")
    return 0


# ============================================================================
# sdp source list
# ============================================================================

def cmd_source_list(args):
    """List configured sources."""
    from social_data_pipeline.setup.utils import list_sources, get_source_profiles

    sources = list_sources()
    if not sources:
        print("\n  No sources configured. Run: python sdp.py source add <name>\n")
        return 0

    print(f"\n  Configured sources:\n")
    for source in sources:
        profiles = get_source_profiles(source)
        profiles_str = ", ".join(profiles) if profiles else "none"
        print(f"    {source}")
        print(f"      profiles: {profiles_str}")
    print()
    return 0


# ============================================================================
# sdp source status
# ============================================================================

def cmd_source_status(args):
    """Show source processing/ingestion status."""
    from social_data_pipeline.setup.utils import list_sources, load_source_config, get_source_profiles

    source_name = args.name

    if source_name:
        sources = [source_name]
    else:
        sources = list_sources()

    if not sources:
        print("\n  No sources configured. Run: python sdp.py source add <name>\n")
        return 0

    env = load_env()
    print()
    print("  Social Data Pipeline - Source Status")
    print("  ===================================")

    for source in sources:
        config = load_source_config(source)
        if config is None:
            print(f"\n  Source '{source}': not found")
            continue

        platform = config.get("platform", source)
        data_types = config.get("data_types", [])
        profiles = get_source_profiles(source)

        print(f"\n  Source: {source}")
        print(f"    Platform:    {platform}")
        print(f"    Data types:  {', '.join(data_types)}")
        print(f"    Profiles:    {', '.join(profiles) if profiles else 'none'}")
        paths = config.get("paths", {})
        print(f"    Paths:")
        print(f"      Dumps:     {paths.get('dumps', f'./data/dumps/{source}')}")
        print(f"      Extracted: {paths.get('extracted', f'./data/extracted/{source}')}")
        print(f"      Parsed:    {paths.get('parsed', f'./data/parsed/{source}')}")
        print(f"      Output:    {paths.get('output', f'./data/output/{source}')}")

        # Show ingestion state for this source
        has_postgres = any(p.startswith("postgres") for p in profiles)
        if has_postgres:
            pgdata_path = env.get("PGDATA_PATH", "./data/database/postgres")
            pgdata = Path(pgdata_path)
            if not pgdata.is_absolute():
                pgdata = ROOT / pgdata
            state_dir = pgdata / "state_tracking"
            _print_source_ingestion_state(state_dir, source, "postgres")

        has_mongo = "mongo_ingest" in profiles
        if has_mongo:
            mongo_data_path = env.get("MONGO_DATA_PATH", "./data/database/mongo")
            mongo_data = Path(mongo_data_path)
            if not mongo_data.is_absolute():
                mongo_data = ROOT / mongo_data
            state_dir = mongo_data / "state_tracking"
            _print_source_ingestion_state(state_dir, source, "mongo")

    print()
    return 0


def _print_source_ingestion_state(state_dir, source, db_type):
    """Print ingestion state for a specific source from state files."""
    if not state_dir.exists():
        return

    prefix = f"{source}_{db_type}_"
    state_files = sorted(f for f in state_dir.glob("*.json") if f.stem.startswith(prefix))

    # Also check legacy naming (PLATFORM prefix)
    if not state_files:
        state_files = sorted(state_dir.glob(f"*_{db_type}_*.json"))

    if not state_files:
        return

    print(f"\n    {db_type.title()} ingestion:")
    for sf in state_files:
        try:
            sdata = json.loads(sf.read_text())
        except (json.JSONDecodeError, OSError):
            continue

        name = sf.stem
        processed = sdata.get("processed", [])
        failed = sdata.get("failed", [])
        in_progress = sdata.get("in_progress")
        last_updated = sdata.get("last_updated", "")

        # Extract data type from state filename
        parts = name.split(f"_{db_type}_")
        label = parts[1] if len(parts) == 2 else name

        count = len(processed)
        latest = processed[-1] if processed else "-"

        line = f"      {label}: {count} datasets"
        if latest != "-":
            line += f" (latest: {latest})"
        if in_progress:
            line += f" [in progress: {in_progress}]"
        if failed:
            line += f" [{len(failed)} failed]"
        print(line)

        if last_updated:
            print(f"        last updated: {last_updated}")


# ============================================================================
# sdp source error-logs
# ============================================================================

INGESTION_PROFILES = ["postgres_ingest", "postgres_ml", "mongo_ingest"]


def cmd_source_error_logs(args):
    """Show database ingestion error logs for sources."""
    from social_data_pipeline.setup.utils import list_sources, load_source_config, get_source_profiles

    source_name = args.name
    profile_filter = args.profile

    if source_name:
        sources = [source_name]
    else:
        sources = list_sources()

    if not sources:
        print("\n  No sources configured. Run: python sdp.py source add <name>\n")
        return 0

    env = load_env()
    print()
    print("  Social Data Pipeline - Error Logs")
    print("  ==================================")

    any_errors = False

    for source in sources:
        config = load_source_config(source)
        if config is None:
            print(f"\n  Source '{source}': not found")
            continue

        profiles = get_source_profiles(source)
        source_has_errors = False

        # Determine which ingestion profiles to check
        check_profiles = [profile_filter] if profile_filter else INGESTION_PROFILES

        for profile in check_profiles:
            if profile not in profiles:
                continue

            # Resolve state directory
            if profile.startswith("postgres"):
                pgdata_path = env.get("PGDATA_PATH", "./data/database/postgres")
                pgdata = Path(pgdata_path)
                if not pgdata.is_absolute():
                    pgdata = ROOT / pgdata
                state_dir = pgdata / "state_tracking"
            else:
                mongo_data_path = env.get("MONGO_DATA_PATH", "./data/database/mongo")
                mongo_data = Path(mongo_data_path)
                if not mongo_data.is_absolute():
                    mongo_data = ROOT / mongo_data
                state_dir = mongo_data / "state_tracking"

            # Print failed entries from state files
            failed_files, found = _print_failed_entries(state_dir, source, profile, source_has_errors)
            if found:
                if not source_has_errors:
                    source_has_errors = True
                any_errors = True

                # For mongo_ingest, also show relevant mongoimport log sections
                if profile == "mongo_ingest" and failed_files:
                    log_dir = state_dir.parent / "logs"
                    if log_dir.exists():
                        _print_mongoimport_log_sections(log_dir, failed_files)

        if not source_has_errors and source_name:
            print(f"\n  Source: {source}")
            print("    No errors found.")

    if not any_errors and not source_name:
        print("\n  No errors found for any source.")

    print()
    return 0


def _print_failed_entries(state_dir, source, profile, header_printed):
    """Print failed entries from state files. Returns (failed_filenames, found_any)."""
    if not state_dir.exists():
        return [], False

    prefix = f"{source}_{profile}_"
    state_files = sorted(f for f in state_dir.glob("*.json") if f.stem.startswith(prefix))

    # Legacy fallback: match by profile substring
    if not state_files:
        state_files = sorted(state_dir.glob(f"*_{profile}_*.json"))

    if not state_files:
        return [], False

    all_failed = []
    found_any = False

    for sf in state_files:
        try:
            sdata = json.loads(sf.read_text())
        except (json.JSONDecodeError, OSError):
            continue

        failed = sdata.get("failed", [])
        if not failed:
            continue

        if not found_any and not header_printed:
            print(f"\n  Source: {source}")
        found_any = True

        # Extract data type from filename: {source}_{profile}_{data_type}
        parts = sf.stem.split(f"_{profile}_")
        label = parts[1] if len(parts) == 2 else sf.stem

        print(f"\n    {profile} errors ({label}): {len(failed)} failed")
        for entry in failed:
            filename = entry.get("filename", "unknown")
            error = entry.get("error", "no error message")
            timestamp = entry.get("timestamp", "")
            ts_display = f" [{timestamp}]" if timestamp else ""
            print(f"\n      {filename}{ts_display}")
            # Strip container log path from error message (we show logs inline)
            display_error = error.split(". See log:")[0] if ". See log:" in error else error
            for line in display_error.split('\n'):
                print(f"        {line}")
            all_failed.append(filename)

    return all_failed, found_any


def _print_mongoimport_log_sections(log_dir, failed_filenames):
    """Print mongoimport log sections matching failed filenames."""
    import re
    separator = re.compile(r'^={10,}$')
    failed_set = set(failed_filenames)
    log_files = sorted(log_dir.glob("mongoimport_*.log"))
    if not log_files:
        return

    printed_header = False

    for lf in log_files:
        try:
            lines = lf.read_text().splitlines()
        except OSError:
            continue

        if not lines:
            continue

        # Find all separator line indices
        sep_indices = [i for i, line in enumerate(lines) if separator.match(line)]

        # Find File: lines and their enclosing header blocks
        for idx, line in enumerate(lines):
            if not line.startswith("File:"):
                continue

            filepath = line.split(":", 1)[1].strip()
            file_id = Path(filepath).name
            if file_id not in failed_set:
                continue

            # Find the header block boundaries (opening and closing ==== lines)
            # The File: line is inside a header block between two ==== lines
            opening_sep = None
            closing_sep = None
            for si in sep_indices:
                if si < idx:
                    opening_sep = si
                elif si > idx and closing_sep is None:
                    closing_sep = si

            if opening_sep is None:
                continue

            # The output block is between the previous ==== and this header's opening ====
            # Find the ==== line before opening_sep (end of previous header)
            prev_sep = None
            for si in sep_indices:
                if si < opening_sep:
                    prev_sep = si
            output_start = (prev_sep + 1) if prev_sep is not None else 0
            output_end = opening_sep

            # Also grab any output after the closing ==== (in case flush order is normal)
            post_start = (closing_sep + 1) if closing_sep is not None else len(lines)
            next_sep = None
            for si in sep_indices:
                if si > (closing_sep if closing_sep is not None else len(lines)):
                    next_sep = si
                    break
            post_end = next_sep if next_sep is not None else len(lines)

            # Collect output lines (before header + after header)
            output_before = lines[output_start:output_end]
            output_after = lines[post_start:post_end]

            # Filter out blank-only blocks
            output_lines = [l for l in output_before if l.strip()]
            if output_after:
                output_lines += [l for l in output_after if l.strip()]

            if not printed_header:
                print(f"\n    Mongoimport logs for failed files ({lf}):")
                printed_header = True

            # Print header block
            header_block = lines[opening_sep:((closing_sep + 1) if closing_sep is not None else len(lines))]
            print()
            for hl in header_block:
                print(f"      {hl}")

            # Print output
            if output_lines:
                for ol in output_lines:
                    print(f"      {ol}")
            else:
                print(f"      (no mongoimport output found)")


# ============================================================================
# sdp run
# ============================================================================

def cmd_run(args):
    """Run a pipeline profile for a source."""
    from social_data_pipeline.setup.utils import resolve_source, load_source_config

    profile = args.profile
    if profile not in VALID_PROFILES:
        print(f"  Error: Unknown profile '{profile}'.")
        print(f"  Valid profiles: {', '.join(VALID_PROFILES)}")
        return 1

    # Resolve source (auto-selects if only one exists)
    source = resolve_source(args.source)
    source_config = load_source_config(source)

    # Determine PLATFORM from source config
    if source == "reddit":
        platform = "reddit"
    else:
        platform = f"custom/{source}"

    # Prompt for admin password if auth enabled and profile accesses a database
    db_profiles = {"postgres_ingest", "postgres_ml", "mongo_ingest"}
    if profile in db_profiles and _is_auth_enabled():
        password = _prompt_db_password(tag="sdp_db_password")
        _set_auth_env(password)

    # Build per-source environment (read paths from source config, with defaults)
    paths = source_config.get("paths", {}) if source_config else {}
    env_overrides = {
        "PLATFORM": platform,
        "SOURCE": source,
        "DUMPS_PATH": paths.get("dumps", f"./data/dumps/{source}"),
        "PARSED_PATH": paths.get("parsed", f"./data/parsed/{source}"),
        "EXTRACTED_PATH": paths.get("extracted", f"./data/extracted/{source}"),
        "OUTPUT_PATH": paths.get("output", f"./data/output/{source}"),
    }

    # Pre-create data directories so Docker doesn't create them as root:root
    for key in ("DUMPS_PATH", "PARSED_PATH", "EXTRACTED_PATH", "OUTPUT_PATH"):
        data_dir = Path(env_overrides[key])
        if not data_dir.is_absolute():
            data_dir = ROOT / data_dir
        data_dir.mkdir(parents=True, exist_ok=True)

    # Pass file filter if provided
    if args.filter:
        env_overrides["FILE_FILTER"] = args.filter

    # Set env vars for docker compose
    for key, value in env_overrides.items():
        os.environ[key] = value

    service = PROFILE_SERVICE_MAP.get(profile, profile)
    compose_args = ["--profile", profile, "run", "--rm"]
    if args.build:
        compose_args.append("--build")
    compose_args.append(service)

    print(f"  Source: {source} (platform: {platform})")
    if args.filter:
        print(f"  Filter: {args.filter}")
    result = docker_compose(*compose_args)
    return result.returncode


# ============================================================================
# Argument parser
# ============================================================================

def build_parser():
    parser = argparse.ArgumentParser(
        prog="sdp.py",
        description="Social Data Pipeline CLI",
    )
    parser.add_argument("--tag", action="store_true",
                        help="Prefix interactive prompts with [tag] identifiers for automation")
    subparsers = parser.add_subparsers(dest="command", help="Command group")

    # ---- sdp db ----
    db_parser = subparsers.add_parser("db", help="Database management")
    db_sub = db_parser.add_subparsers(dest="db_command", help="Database command")

    db_setup_p = db_sub.add_parser("setup", help="Configure databases (PostgreSQL, MongoDB)")
    db_setup_p.set_defaults(func=cmd_db_setup)

    db_setup_mcp_p = db_sub.add_parser("setup-mcp", help="Configure MCP servers for databases")
    db_setup_mcp_p.set_defaults(func=cmd_db_setup_mcp)

    db_start_p = db_sub.add_parser("start", help="Start database services")
    db_start_p.add_argument("service", nargs="?",
                            choices=["postgres", "mongo", "postgres-mcp", "mongo-mcp"],
                            help="Specific service (default: all configured)")
    db_start_p.set_defaults(func=cmd_db_start)

    db_stop_p = db_sub.add_parser("stop", help="Stop database services")
    db_stop_p.add_argument("service", nargs="?",
                           choices=["postgres", "mongo", "postgres-mcp", "mongo-mcp"],
                           help="Specific service (default: all configured)")
    db_stop_p.set_defaults(func=cmd_db_stop)

    db_status_p = db_sub.add_parser("status", help="Show database status")
    db_status_p.set_defaults(func=cmd_db_status)

    db_unsetup_p = db_sub.add_parser("unsetup", help="Remove database configuration")
    db_unsetup_p.set_defaults(func=cmd_db_unsetup)

    db_unsetup_mcp_p = db_sub.add_parser("unsetup-mcp", help="Remove MCP configuration")
    db_unsetup_mcp_p.set_defaults(func=cmd_db_unsetup_mcp)

    db_recover_p = db_sub.add_parser("recover-password", help="Reset database admin password")
    db_recover_p.set_defaults(func=cmd_db_recover_password)

    db_indexes_p = db_sub.add_parser("create-indexes", help="Interactively create database indexes")
    db_indexes_p.add_argument("--source", "-s", dest="source",
                               help="Source name (auto-selects if only one configured)")
    db_indexes_p.set_defaults(func=cmd_db_create_indexes)

    # ---- sdp source ----
    source_parser = subparsers.add_parser("source", help="Source management")
    source_sub = source_parser.add_subparsers(dest="source_command", help="Source command")

    src_add_p = source_sub.add_parser("add", help="Add a new source")
    src_add_p.add_argument("name", help="Source name (e.g. reddit, twitter_academic)")
    src_add_p.add_argument("--hf", dest="hf_dataset", default=None,
                           help="Hugging Face dataset ID (e.g. user/dataset-name)")
    src_add_p.set_defaults(func=cmd_source_add)

    src_download_p = source_sub.add_parser("download", help="Download HF dataset files")
    src_download_p.add_argument("name", help="Source name")
    src_download_p.add_argument("--token", help="HF token for private datasets (fallback: HF_TOKEN env var)")
    src_download_p.add_argument("--data-type", help="Download only this data type")
    src_download_p.set_defaults(func=cmd_source_download)

    src_configure_p = source_sub.add_parser("configure", help="Reconfigure existing source")
    src_configure_p.add_argument("name", help="Source name")
    src_configure_p.set_defaults(func=cmd_source_configure)

    src_classifiers_p = source_sub.add_parser("add-classifiers", help="Add ML classifiers for a source")
    src_classifiers_p.add_argument("name", help="Source name")
    src_classifiers_p.set_defaults(func=cmd_source_add_classifiers)

    src_remove_p = source_sub.add_parser("remove", help="Remove source configuration")
    src_remove_p.add_argument("name", help="Source name")
    src_remove_p.set_defaults(func=cmd_source_remove)

    src_list_p = source_sub.add_parser("list", help="List configured sources")
    src_list_p.set_defaults(func=cmd_source_list)

    src_status_p = source_sub.add_parser("status", help="Show source status")
    src_status_p.add_argument("name", nargs="?", help="Source name (default: all sources)")
    src_status_p.set_defaults(func=cmd_source_status)

    src_errors_p = source_sub.add_parser("error-logs", help="Show database ingestion error logs")
    src_errors_p.add_argument("name", nargs="?", help="Source name (default: all sources)")
    src_errors_p.add_argument("--profile", "-p",
        choices=["postgres_ingest", "postgres_ml", "mongo_ingest"],
        help="Filter by ingestion profile")
    src_errors_p.set_defaults(func=cmd_source_error_logs)

    # ---- sdp run ----
    run_parser = subparsers.add_parser("run", help="Run a pipeline profile")
    run_parser.add_argument("profile", choices=VALID_PROFILES, help="Pipeline profile to run")
    run_parser.add_argument("--source", "-s", dest="source",
                            help="Source name (auto-selects if only one configured)")
    run_parser.add_argument("--build", action="store_true", help="Rebuild Docker image")
    run_parser.add_argument("--filter", "-f", dest="filter",
                            help="Filter files by pattern (fnmatch glob on file ID, e.g. '*2024*', 'RS_2024-*')")
    run_parser.set_defaults(func=cmd_run)

    return parser


# ============================================================================
# Main
# ============================================================================

def main():
    parser = build_parser()
    args = parser.parse_args()

    if args.tag:
        os.environ['SDP_TAGGED_MODE'] = '1'

    if not args.command:
        parser.print_help()
        return 0

    # Handle subcommand groups that need their own help
    if args.command == "db" and not getattr(args, "db_command", None):
        parser.parse_args(["db", "-h"])
        return 0
    if args.command == "source" and not getattr(args, "source_command", None):
        parser.parse_args(["source", "-h"])
        return 0

    if hasattr(args, "func"):
        return args.func(args)

    parser.print_help()
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main() or 0)
    except KeyboardInterrupt:
        print("\n\n  Aborted.\n")
        sys.exit(1)
