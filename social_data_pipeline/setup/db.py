"""Database configuration for Social Data Pipeline.

Configures PostgreSQL, MongoDB, and StarRocks settings (port, tablespaces,
PGTune, cache, FE/BE memory).
Generates .env, config/db/*.yaml, postgresql.local.conf, fe.local.conf, be.local.conf,
docker-compose.override.yml.

This is a global, one-time configuration independent of any source.
"""

import os
import secrets
import subprocess
import sys
from getpass import getpass
from pathlib import Path

try:
    import yaml
except ImportError:
    print("Error: PyYAML is required. Install with: pip install pyyaml")
    sys.exit(1)

from social_data_pipeline.setup.utils import (
    ROOT, CONFIG_DIR,
    detect_hardware, load_env,
    ask, ask_int, ask_bool, ask_choice, ask_multi_select, ask_multi_line,
    section_header, write_files,
)


def ask_password(label: str, tag=None) -> str:
    """Prompt for a password with confirmation. Uses getpass for hidden input."""
    from social_data_pipeline.setup.utils import _tag_prefix
    prefix = _tag_prefix(tag)
    while True:
        pw1 = getpass(f"  {prefix}{label}: ")
        if not pw1:
            print("    Password cannot be empty.")
            continue
        confirm_tag = f"{tag}_confirm" if tag else None
        confirm_prefix = _tag_prefix(confirm_tag)
        pw2 = getpass(f"  {confirm_prefix}Confirm {label.lower()}: ")
        if pw1 != pw2:
            print("    Passwords do not match. Try again.")
            continue
        return pw1


def generate_password() -> str:
    """Generate a random 32-character URL-safe password."""
    return secrets.token_urlsafe(24)


# ============================================================================
# Load existing configuration
# ============================================================================

def _extract_existing_pgtune():
    """Extract pgtune content from existing postgresql.local.conf, if any."""
    local_conf = CONFIG_DIR / "postgres" / "postgresql.local.conf"
    if not local_conf.exists():
        return ""
    try:
        content = local_conf.read_text()
    except OSError:
        return ""
    marker = "# PASTE PGTUNE OUTPUT BELOW THIS LINE"
    if marker not in content:
        return ""
    idx = content.index(marker)
    after_marker = content[content.index("\n", idx) + 1:]
    return after_marker.strip()


def _load_existing_db_config():
    """Load existing database configuration for use as defaults on re-run."""
    existing = {}
    env = load_env()

    # Load scalar values from .env
    if env.get("DATA_PATH"):
        existing["data_path"] = env["DATA_PATH"]
    if env.get("PGDATA_PATH"):
        existing["pgdata_path"] = env["PGDATA_PATH"]
    if env.get("DB_NAME"):
        existing["db_name"] = env["DB_NAME"]
    if env.get("MONGO_DATA_PATH"):
        existing["mongo_data_path"] = env["MONGO_DATA_PATH"]
    if env.get("STARROCKS_DATA_PATH"):
        existing["starrocks_data_path"] = env["STARROCKS_DATA_PATH"]
    for env_key, setting_key in [
        ("POSTGRES_PORT", "pg_port"),
        ("MONGO_PORT", "mongo_port"),
        ("MONGO_CACHE_SIZE_GB", "mongo_cache_size_gb"),
        ("STARROCKS_PORT", "starrocks_port"),
        ("STARROCKS_FE_HTTP_PORT", "starrocks_fe_http_port"),
    ]:
        if env.get(env_key):
            try:
                existing[setting_key] = int(env[env_key])
            except (ValueError, TypeError):
                pass
    for env_key, setting_key in [
        ("POSTGRES_MEM_LIMIT", "pg_mem_limit"),
        ("MONGO_MEM_LIMIT", "mongo_mem_limit"),
        ("STARROCKS_MEM_LIMIT", "starrocks_mem_limit"),
    ]:
        if env.get(env_key):
            try:
                existing[setting_key] = int(env[env_key].rstrip("g"))
            except (ValueError, TypeError):
                pass

    # Load from YAML configs (fill gaps not covered by .env)
    pg_yaml = CONFIG_DIR / "db" / "postgres.yaml"
    if pg_yaml.exists():
        try:
            pg = yaml.safe_load(pg_yaml.read_text()) or {}
            if pg.get("port") is not None:
                existing.setdefault("pg_port", pg["port"])
            if pg.get("name"):
                existing.setdefault("db_name", pg["name"])
            if pg.get("tablespaces"):
                existing["tablespaces"] = pg["tablespaces"]
            if pg.get("auth"):
                existing["auth_enabled"] = True
            if pg.get("ro_username"):
                existing["ro_username"] = pg["ro_username"]
        except (OSError, yaml.YAMLError):
            pass

    mongo_yaml = CONFIG_DIR / "db" / "mongo.yaml"
    if mongo_yaml.exists():
        try:
            mg = yaml.safe_load(mongo_yaml.read_text()) or {}
            if mg.get("port") is not None:
                existing.setdefault("mongo_port", mg["port"])
            if mg.get("cache_size_gb") is not None:
                existing.setdefault("mongo_cache_size_gb", mg["cache_size_gb"])
            if mg.get("validate_before_import"):
                existing["mongo_validate"] = mg["validate_before_import"]
            if mg.get("auth"):
                existing["auth_enabled"] = True
            if mg.get("ro_username"):
                existing.setdefault("ro_username", mg["ro_username"])
        except (OSError, yaml.YAMLError):
            pass

    sr_yaml = CONFIG_DIR / "db" / "starrocks.yaml"
    if sr_yaml.exists():
        try:
            sr = yaml.safe_load(sr_yaml.read_text()) or {}
            if sr.get("port") is not None:
                existing.setdefault("starrocks_port", sr["port"])
            if sr.get("fe_http_port") is not None:
                existing.setdefault("starrocks_fe_http_port", sr["fe_http_port"])
            if sr.get("fe_jvm_heap") is not None:
                existing.setdefault("sr_fe_jvm_heap", sr["fe_jvm_heap"])
            if sr.get("be_mem_limit") is not None:
                existing.setdefault("sr_be_mem_limit", sr["be_mem_limit"])
            if sr.get("storage_paths"):
                existing["starrocks_storage_paths"] = sr["storage_paths"]
        except (OSError, yaml.YAMLError):
            pass

    # Determine databases from existing config files
    databases = []
    if pg_yaml.exists():
        databases.append("postgres")
    if mongo_yaml.exists():
        databases.append("mongo")
    if sr_yaml.exists():
        databases.append("starrocks")
    if databases:
        existing["databases"] = databases

    return existing


# ============================================================================
# Interactive questionnaire
# ============================================================================

def run_questionnaire(hw):
    """Run the database configuration questionnaire. Returns settings dict."""
    existing = _load_existing_db_config()
    settings = {}

    # --- Print hardware summary ---
    section_header("Hardware Detected")
    cores = hw["cpu_cores"]
    ram = hw["ram_gb"]
    print(f"  CPU cores: {cores or 'unknown'}")
    print(f"  RAM:       {ram or 'unknown'} GB")
    print()

    # ---- Data base path ----
    section_header("Data Path")
    print("  Base directory for all data (dumps, parsed, output, databases).")
    print()
    data_path = ask("Data base path", existing.get("data_path", "./data"), tag="db_data_path")
    settings["data_path"] = data_path

    # ---- Database selection ----
    section_header("Database Selection")

    all_databases = ["postgres", "mongo", "starrocks"]
    databases = ask_multi_select("Databases:", all_databases, existing.get("databases", ["postgres"]), tag="db_databases")
    settings["databases"] = databases

    has_postgres = "postgres" in databases
    has_mongo = "mongo" in databases
    has_starrocks = "starrocks" in databases

    # ---- Paths (database data dirs) ----
    section_header("Database Paths")
    if has_postgres:
        settings["pgdata_path"] = ask("PostgreSQL data path", existing.get("pgdata_path", f"{data_path}/database/postgres"), tag="db_pgdata_path")
    if has_mongo:
        settings["mongo_data_path"] = ask("MongoDB data path", existing.get("mongo_data_path", f"{data_path}/database/mongo"), tag="db_mongo_data_path")
    if has_starrocks:
        settings["starrocks_data_path"] = ask("StarRocks data path", existing.get("starrocks_data_path", f"{data_path}/database/starrocks"), tag="db_sr_data_path")

    # ---- PostgreSQL ----
    if has_postgres:
        section_header("PostgreSQL Configuration")

        settings["db_name"] = ask("Database name", existing.get("db_name", "datasets"), tag="db_name")
        settings["pg_port"] = ask_int("PostgreSQL port", existing.get("pg_port", 5432), tag="db_pg_port")

        # Tablespace configuration
        if ask_bool("Use tablespaces? (spread tables across multiple disks)", bool(existing.get("tablespaces")), tag="db_tablespaces"):
            print()
            print("  Note: Check documentation for expected disk usage per data type.")
            print()
            existing_ts = existing.get("tablespaces", {})
            ts_tablespaces = {}

            if existing_ts:
                print("  Current tablespaces:")
                for name, path in existing_ts.items():
                    print(f"    {name}: {path}")
                print()
                if ask_bool("Keep existing tablespaces?", True, tag="db_ts_keep"):
                    ts_tablespaces = dict(existing_ts)

            if not ts_tablespaces:
                while True:
                    ts_name = ask("Tablespace name (e.g. nvme1)", tag="db_ts_name")
                    if not ts_name or ts_name == "pgdata":
                        print("    'pgdata' is reserved for the default PostgreSQL data directory.")
                        continue
                    ts_path = ask(f"Host path for '{ts_name}' (directory on disk)", tag="db_ts_path")
                    if ts_path:
                        ts_tablespaces[ts_name] = ts_path
                    if not ask_bool("Add another tablespace?", False, tag="db_ts_more"):
                        break

            if ts_tablespaces:
                settings["tablespaces"] = ts_tablespaces
            print()

        fs = ask_choice(
            "Filesystem for PostgreSQL data:",
            ["standard", "zfs"],
            default="standard",
            tag="db_filesystem",
        )
        settings["filesystem"] = fs

        print()
        print("  For PostgreSQL memory tuning, provide your PGTune output.")
        print("  Generate at: https://pgtune.leopard.in.ua/")
        print(f"    DB Version: 18 | OS: linux | DB Type: dw | Storage: ssd")
        if hw["ram_gb"]:
            print(f"    Total Memory: {hw['ram_gb']} GB | CPUs: {hw['cpu_cores']}")
        print()
        existing_pgtune = _extract_existing_pgtune()
        pgtune_choices = ["paste", "file", "skip"]
        pgtune_default = "paste"
        if existing_pgtune:
            pgtune_choices = ["keep", "paste", "file", "skip"]
            pgtune_default = "keep"
        pgtune_method = ask_choice(
            "PGTune output:",
            pgtune_choices,
            default=pgtune_default,
            tag="db_pgtune_method",
        )
        if pgtune_method == "keep":
            settings["pgtune_output"] = existing_pgtune
        elif pgtune_method == "paste":
            settings["pgtune_output"] = ask_multi_line("Paste PGTune output below:", tag="db_pgtune_paste")
        elif pgtune_method == "file":
            pgtune_path = ask("Path to file with PGTune output", tag="db_pgtune_file")
            try:
                settings["pgtune_output"] = Path(pgtune_path).expanduser().read_text()
            except (OSError, ValueError) as e:
                print(f"    Warning: Could not read {pgtune_path}: {e}")
                settings["pgtune_output"] = ""
        else:
            settings["pgtune_output"] = ""

        if fs == "zfs" and hw["ram_gb"]:
            arc_max_gb = int(hw["ram_gb"] // 2)
            arc_max_bytes = arc_max_gb * 1024 ** 3
            print()
            print(f"  NOTE: ZFS ARC cache competes with PostgreSQL for RAM.")
            print(f"  To avoid memory pressure, limit ARC (suggested ~{arc_max_gb}GB for this system):")
            print(f"    echo {arc_max_bytes} > /sys/module/zfs/parameters/zfs_arc_max")
            print(f"  Persist in /etc/modprobe.d/zfs.conf:")
            print(f"    options zfs zfs_arc_max={arc_max_bytes}")

        print()
        suggested_pg_mem = int(hw["ram_gb"] * 0.6) if hw["ram_gb"] else 0
        pg_mem = ask_int("PostgreSQL container memory limit (GB, 0=unlimited)", existing.get("pg_mem_limit", suggested_pg_mem), tag="db_pg_mem_limit")
        if pg_mem > 0:
            settings["pg_mem_limit"] = pg_mem

    # ---- MongoDB ----
    if has_mongo:
        section_header("MongoDB Configuration")

        settings["mongo_port"] = ask_int("MongoDB port", existing.get("mongo_port", 27017), tag="db_mongo_port")
        settings["mongo_cache_size_gb"] = ask_int("MongoDB WiredTiger cache size (GB)", existing.get("mongo_cache_size_gb", 2), tag="db_mongo_cache")

        mongo_cache = settings.get("mongo_cache_size_gb", 2)
        suggested_mongo_mem = max(2, mongo_cache * 2)
        mongo_mem = ask_int("MongoDB container memory limit (GB, 0=unlimited)", existing.get("mongo_mem_limit", suggested_mongo_mem), tag="db_mongo_mem_limit")
        if mongo_mem > 0:
            settings["mongo_mem_limit"] = mongo_mem

        print()
        print("  Pre-import file validation prevents partial ingestion of corrupt files.")
        print("  mongoimport is not atomic — without validation, truncated or malformed")
        print("  files leave partial data permanently in the database.")
        print()
        print("    full: validates every JSON line before import (one sequential read per file)")
        print("    tail: checks only the last 8KB (catches truncation, not malformed lines)")
        print("    none: skip validation")
        settings["mongo_validate"] = ask_choice(
            "Pre-import file validation",
            ["full", "tail", "none"],
            default=existing.get("mongo_validate", "full"),
            tag="db_mongo_validate",
        )

    # ---- StarRocks ----
    if has_starrocks:
        section_header("StarRocks Configuration")

        settings["starrocks_port"] = ask_int("StarRocks MySQL protocol port", existing.get("starrocks_port", 9030), tag="db_sr_port")
        settings["starrocks_fe_http_port"] = ask_int("StarRocks FE HTTP port (admin/Stream Load)", existing.get("starrocks_fe_http_port", 8030), tag="db_sr_fe_http_port")

        print()
        print("  StarRocks runs FE (query planner) and BE (storage engine) in one container.")
        print("  Allocate memory to each component separately.")
        print()

        # FE JVM heap
        default_fe_heap = max(2, min(8, int(ram // 8))) if ram else 4
        settings["sr_fe_jvm_heap"] = ask(
            "FE JVM heap size (GB)",
            existing.get("sr_fe_jvm_heap", default_fe_heap),
            tag="db_sr_fe_heap",
        )
        fe_heap = float(settings["sr_fe_jvm_heap"])

        # Container memory limit (asked before BE so the BE default can use it)
        print()
        if ram:
            suggested_sr_mem = int(max(fe_heap + 4, ram * 0.6))
        else:
            suggested_sr_mem = 0
        sr_mem_str = ask(
            "StarRocks container memory limit (GB, 0=unlimited)",
            existing.get("starrocks_mem_limit", suggested_sr_mem),
            tag="db_sr_mem_limit",
        )
        sr_mem = float(sr_mem_str)
        if sr_mem > 0:
            settings["starrocks_mem_limit"] = sr_mem_str

        # BE memory limit (default: container_limit - fe_heap - 2GB headroom, or 50% RAM)
        if sr_mem > 0:
            default_be_mem = int(max(2, sr_mem - fe_heap - 2))
        elif ram:
            default_be_mem = int(max(2, ram * 0.5))
        else:
            default_be_mem = 8
        settings["sr_be_mem_limit"] = ask(
            "BE memory limit (GB)",
            existing.get("sr_be_mem_limit", default_be_mem),
            tag="db_sr_be_mem",
        )

        # Multi-disk storage
        if ask_bool("Use multiple disks for StarRocks storage?", bool(existing.get("starrocks_storage_paths")), tag="db_sr_multidisk"):
            existing_paths = existing.get("starrocks_storage_paths", [])
            if existing_paths:
                print()
                print("  Current storage paths:")
                for p in existing_paths:
                    print(f"    {p}")
                print()
                if ask_bool("Keep existing storage paths?", True, tag="db_sr_keep_paths"):
                    settings["starrocks_storage_paths"] = list(existing_paths)

            if "starrocks_storage_paths" not in settings:
                storage_paths = []
                while True:
                    sp = ask("Host path for StarRocks storage (e.g. /mnt/nvme1/starrocks)", tag="db_sr_storage_path")
                    if sp:
                        storage_paths.append(sp)
                    if not ask_bool("Add another storage path?", False, tag="db_sr_more_paths"):
                        break
                if storage_paths:
                    settings["starrocks_storage_paths"] = storage_paths

    # ---- Authentication ----
    if has_postgres or has_mongo or has_starrocks:
        section_header("Authentication")
        print("  Enable database authentication to require passwords for connections.")
        print("  Recommended for multi-user or remote servers.")
        print()

        if ask_bool("Enable database authentication?", existing.get("auth_enabled", False), tag="db_auth"):
            settings["auth_enabled"] = True

            print()
            print("  Choose an admin password for database access.")
            print("  This password is NOT stored anywhere — you will be prompted when needed.")
            print()
            settings["db_password"] = ask_password("Admin password", tag="db_password")

            print()
            if ask_bool("Create a read-only user? (required for MCP servers)", True, tag="db_ro_user"):
                ro_username = ask("Read-only username", existing.get("ro_username", "readonly"), tag="db_ro_username")
                settings["ro_username"] = ro_username
                print()
                if ask_bool("Auto-generate read-only password?", True, tag="db_ro_auto_password"):
                    settings["ro_password"] = secrets.token_urlsafe(24)
                else:
                    settings["ro_password"] = ask_password("Read-only password", tag="db_ro_password")

    return settings


# ============================================================================
# Config generators
# ============================================================================

def generate_env(settings):
    """Generate .env file content with database and global settings.

    Preserves existing env vars not managed by db setup (e.g. MCP ports,
    HF_TOKEN) by reading the current .env and appending unmanaged keys.
    """
    lines = [
        "# ===== DATA PATH =====",
        f"DATA_PATH={settings.get('data_path', './data')}",
        "",
        "# ===== HUGGINGFACE CONFIGURATION (ml profile) =====",
        "# Set HF_HOME to specify a custom cache directory for Hugging Face models and datasets.",
        "# HF_HOME=",
        "# Set HF_TOKEN to avoid rate limits and download private models.",
        "# HF_TOKEN=",
    ]

    if "pgdata_path" in settings:
        lines += [
            "",
            "# ===== POSTGRESQL CONFIGURATION =====",
            f"PGDATA_PATH={settings['pgdata_path']}",
            f"DB_NAME={settings.get('db_name', 'datasets')}",
            f"POSTGRES_PORT={settings.get('pg_port', 5432)}",
        ]
        if settings.get("pg_mem_limit"):
            lines.append(f"POSTGRES_MEM_LIMIT={settings['pg_mem_limit']}g")

    if "mongo_data_path" in settings:
        lines += [
            "",
            "# ===== MONGODB CONFIGURATION =====",
            f"MONGO_DATA_PATH={settings['mongo_data_path']}",
            f"MONGO_PORT={settings.get('mongo_port', 27017)}",
            f"MONGO_CACHE_SIZE_GB={settings.get('mongo_cache_size_gb', 2)}",
        ]
        if settings.get("mongo_mem_limit"):
            lines.append(f"MONGO_MEM_LIMIT={settings['mongo_mem_limit']}g")

    if "starrocks_data_path" in settings:
        lines += [
            "",
            "# ===== STARROCKS CONFIGURATION =====",
            f"STARROCKS_DATA_PATH={settings['starrocks_data_path']}",
            f"STARROCKS_PORT={settings.get('starrocks_port', 9030)}",
            f"STARROCKS_FE_HTTP_PORT={settings.get('starrocks_fe_http_port', 8030)}",
        ]
        if settings.get("starrocks_mem_limit"):
            lines.append(f"STARROCKS_MEM_LIMIT={settings['starrocks_mem_limit']}g")

    if settings.get("auth_enabled"):
        lines += [
            "",
            "# ===== AUTHENTICATION =====",
            "POSTGRES_AUTH_ENABLED=true",
            "MONGO_AUTH_ENABLED=true",
            "MONGO_ADMIN_USER=admin",
        ]
        if settings.get("ro_username"):
            lines += [
                f"POSTGRES_RO_USER={settings['ro_username']}",
                f"MONGO_RO_USER={settings['ro_username']}",
            ]

    # Preserve existing env vars not managed by this function
    new_content = "\n".join(lines) + "\n"
    managed_keys = set()
    for line in lines:
        stripped = line.lstrip("# ").strip()
        if "=" in stripped and not stripped.startswith("="):
            managed_keys.add(stripped.split("=", 1)[0])

    env_path = ROOT / ".env"
    if env_path.exists():
        preserved = []
        for line in env_path.read_text().splitlines():
            stripped = line.lstrip("# ").strip()
            if "=" in stripped and not stripped.startswith("="):
                key = stripped.split("=", 1)[0]
                if key not in managed_keys:
                    preserved.append(line)
        if preserved:
            new_content += "\n".join(preserved) + "\n"

    return new_content


def generate_db_postgres_yaml(settings):
    """Generate config/db/postgres.yaml content."""
    config = {
        "port": settings.get("pg_port", 5432),
        "name": settings.get("db_name", "datasets"),
    }
    if "tablespaces" in settings:
        config["tablespaces"] = settings["tablespaces"]
    if settings.get("auth_enabled"):
        config["auth"] = True
    if settings.get("ro_username"):
        config["ro_username"] = settings["ro_username"]
    return yaml.dump(config, default_flow_style=False, sort_keys=False)


def generate_db_mongo_yaml(settings):
    """Generate config/db/mongo.yaml content."""
    config = {
        "port": settings.get("mongo_port", 27017),
        "cache_size_gb": settings.get("mongo_cache_size_gb", 2),
        "validate_before_import": settings.get("mongo_validate", "full"),
    }
    if settings.get("auth_enabled"):
        config["auth"] = True
    if settings.get("ro_username"):
        config["ro_username"] = settings["ro_username"]
    return yaml.dump(config, default_flow_style=False, sort_keys=False)


def generate_db_starrocks_yaml(settings):
    """Generate config/db/starrocks.yaml content."""
    config = {
        "port": settings.get("starrocks_port", 9030),
        "fe_http_port": settings.get("starrocks_fe_http_port", 8030),
        "fe_jvm_heap": settings.get("sr_fe_jvm_heap", 4),
        "be_mem_limit": settings.get("sr_be_mem_limit", 8),
    }
    if settings.get("starrocks_storage_paths"):
        config["storage_paths"] = settings["starrocks_storage_paths"]
    if settings.get("auth_enabled"):
        config["auth"] = True
    if settings.get("ro_username"):
        config["ro_username"] = settings["ro_username"]
    return yaml.dump(config, default_flow_style=False, sort_keys=False)


def _replace_conf_value(content, key, value):
    """Replace a key = value line in a .conf file (properties format)."""
    import re
    pattern = rf"^(\s*#?\s*){re.escape(key)}\s*=.*$"
    replacement = f"{key} = {value}"
    new_content, count = re.subn(pattern, replacement, content, flags=re.MULTILINE)
    if count == 0:
        new_content = content.rstrip("\n") + f"\n{replacement}\n"
    return new_content


def generate_starrocks_fe_conf(settings):
    """Generate config/starrocks/fe.local.conf from base fe.conf with tuned JVM heap."""
    base_path = CONFIG_DIR / "starrocks" / "fe.conf"
    content = base_path.read_text()

    jvm_heap = settings.get("sr_fe_jvm_heap", 4)
    content = _replace_conf_value(content, "jvm_heap_size", f"{jvm_heap}g")

    return content


def generate_starrocks_be_conf(settings):
    """Generate config/starrocks/be.local.conf from base be.conf with storage paths and memory."""
    base_path = CONFIG_DIR / "starrocks" / "be.conf"
    content = base_path.read_text()

    # Multi-disk storage paths
    storage_paths = settings.get("starrocks_storage_paths")
    if storage_paths:
        container_paths = [
            f"/data/starrocks/be/storage_{i}" for i in range(len(storage_paths))
        ]
        content = _replace_conf_value(
            content, "storage_root_path", ";".join(container_paths)
        )

    # BE memory limit (absolute GB value)
    be_mem = settings.get("sr_be_mem_limit")
    if be_mem:
        content = _replace_conf_value(content, "mem_limit", f"{be_mem}G")

    return content


def generate_docker_compose_override(settings):
    """Generate docker-compose.override.yml with extra volume mounts.

    Handles PostgreSQL tablespace volumes and StarRocks multi-disk storage.
    """
    tablespaces = settings.get("tablespaces", {})
    pg_lines = []
    for ts_name, host_path in tablespaces.items():
        if ts_name != "pgdata":
            pg_lines.append(f"      - {host_path}:/data/tablespace/{ts_name}")

    sr_storage = settings.get("starrocks_storage_paths", [])
    sr_lines = []
    for i, host_path in enumerate(sr_storage):
        sr_lines.append(f"      - {host_path}:/data/starrocks/be/storage_{i}")

    if not pg_lines and not sr_lines:
        return None

    content = "# Auto-generated by sdp db setup — extra volume mounts.\n\nservices:\n"
    if pg_lines:
        content += "  postgres:\n    volumes:\n" + "\n".join(pg_lines) + "\n"
    if sr_lines:
        if pg_lines:
            content += "\n"
        content += "  starrocks:\n    volumes:\n" + "\n".join(sr_lines) + "\n"

    return content


def generate_postgresql_local_conf(settings):
    """Generate postgresql.local.conf by copying base, toggling ZFS, appending pgtune."""
    base_path = CONFIG_DIR / "postgres" / "postgresql.conf"
    try:
        base_content = base_path.read_text()
    except PermissionError:
        print(f"\n  Error: Cannot read {base_path}")
        print(f"  The config/ directory may be missing the execute bit (needed for traversal).")
        print(f"  Try: chmod 755 config/ config/*/")
        sys.exit(1)

    # Split at the pgtune marker
    pgtune_marker = "# PASTE PGTUNE OUTPUT BELOW THIS LINE"
    if pgtune_marker in base_content:
        marker_idx = base_content.index(pgtune_marker)
        marker_line_end = base_content.index("\n", marker_idx) + 1
        content = base_content[:marker_line_end]
    else:
        content = base_content

    # Toggle ZFS settings
    is_zfs = settings.get("filesystem") == "zfs"
    if is_zfs:
        new_lines = []
        in_zfs_block = False
        seen_zfs_header = False
        for line in content.splitlines():
            if "ZFS optimizations" in line:
                seen_zfs_header = True
                new_lines.append(line)
                continue
            if seen_zfs_header and not in_zfs_block and line.startswith("#=="):
                in_zfs_block = True
                new_lines.append(line)
                continue
            if in_zfs_block and line.startswith("#=="):
                in_zfs_block = False
                new_lines.append(line)
                continue
            if in_zfs_block:
                if line.startswith("# # "):
                    new_lines.append("#" + line[3:])
                elif line.startswith("# ") and "=" in line:
                    new_lines.append(line[2:])
                else:
                    new_lines.append(line)
            else:
                new_lines.append(line)
        content = "\n".join(new_lines) + "\n"

    # Append pgtune output
    pgtune = settings.get("pgtune_output", "").strip()
    if pgtune:
        content += pgtune + "\n"

    return content


def generate_pg_hba_local_conf(settings):
    """Generate pg_hba.local.conf with scram-sha-256 for auth-enabled setup."""
    lines = [
        "# Auto-generated by sdp db setup — authentication enabled",
        "# TYPE  DATABASE  USER  ADDRESS  METHOD",
        "",
        "# Local (unix socket) — trust for container-internal access",
        "local   all       all                    trust",
        "",
        "# Localhost IPv4",
        "host    all       all   127.0.0.1/32     scram-sha-256",
        "",
        "# All users — require password from Docker networks",
        "host    all       all   172.16.0.0/12    scram-sha-256",
        "host    all       all   192.168.0.0/16   scram-sha-256",
    ]

    return "\n".join(lines) + "\n"


# ============================================================================
# Summary
# ============================================================================

def print_summary(settings, files_to_write):
    """Print a summary of database settings and files to be written."""
    section_header("Database Configuration Summary")

    data_path = settings.get("data_path", "./data")
    print(f"  Data path:   {data_path}")

    databases = settings["databases"]
    print(f"  Databases:   {', '.join(databases)}")
    print()

    if "postgres" in databases:
        print(f"  PostgreSQL:")
        print(f"    DB name:             {settings.get('db_name', 'datasets')}")
        print(f"    Port:                {settings.get('pg_port', 5432)}")
        print(f"    Data path:           {settings.get('pgdata_path', './data/database/postgres')}")
        print(f"    Filesystem:          {settings.get('filesystem', 'standard')}")
        if "tablespaces" in settings:
            print(f"    Tablespaces:")
            for ts_name, ts_path in settings["tablespaces"].items():
                print(f"      {ts_name}: {ts_path}")
        print(f"    PGTune:              {'provided' if settings.get('pgtune_output') else 'not provided'}")
        print()

    if "mongo" in databases:
        print(f"  MongoDB:")
        print(f"    Port:                {settings.get('mongo_port', 27017)}")
        print(f"    Cache size:          {settings.get('mongo_cache_size_gb', 2)} GB")
        print(f"    Data path:           {settings.get('mongo_data_path', './data/database/mongo')}")
        print()

    if "starrocks" in databases:
        print(f"  StarRocks:")
        print(f"    Port (MySQL):        {settings.get('starrocks_port', 9030)}")
        print(f"    FE HTTP port:        {settings.get('starrocks_fe_http_port', 8030)}")
        print(f"    FE JVM heap:         {settings.get('sr_fe_jvm_heap', 4)} GB")
        print(f"    BE memory limit:     {settings.get('sr_be_mem_limit', 8)} GB")
        print(f"    Data path:           {settings.get('starrocks_data_path', './data/database/starrocks')}")
        if settings.get("starrocks_storage_paths"):
            print(f"    Storage paths:")
            for sp in settings["starrocks_storage_paths"]:
                print(f"      {sp}")
        if settings.get("starrocks_mem_limit"):
            print(f"    Container limit:     {settings['starrocks_mem_limit']} GB")
        print()

    if settings.get("auth_enabled"):
        print(f"  Authentication:  enabled")
        if settings.get("ro_username"):
            print(f"    RO user:         {settings['ro_username']} (auto-generated password)")
        print()

    print("  Files to write:")
    for path, _ in files_to_write:
        rel = path.relative_to(ROOT)
        exists = path.exists()
        status = " (exists, will backup)" if exists else ""
        print(f"    {rel}{status}")
    print()


# ============================================================================
# Credential writing
# ============================================================================

def _write_ro_credentials(settings):
    """Write read-only user credentials to database data volumes.

    Creates .ro_credentials files (chmod 600) in the database data paths.
    Format: username:password (single line). Called during db setup when
    a read-only user is configured with auth enabled.
    """
    ro_username = settings["ro_username"]
    ro_password = settings["ro_password"]
    credentials = f"{ro_username}:{ro_password}"
    written = []

    def _write_cred_file(data_path: Path):
        cred_file = data_path / ".ro_credentials"
        cred_file.parent.mkdir(parents=True, exist_ok=True)
        try:
            cred_file.write_text(credentials + "\n")
            os.chmod(cred_file, 0o600)
        except PermissionError:
            abs_parent = cred_file.resolve().parent
            subprocess.run(
                ["docker", "run", "--rm", "-i",
                 "-v", f"{abs_parent}:/data",
                 "alpine", "sh", "-c",
                 "cat > /data/.ro_credentials && chmod 600 /data/.ro_credentials"],
                input=(credentials + "\n").encode(),
                check=True, capture_output=True,
            )
        written.append(str(cred_file))

    if "pgdata_path" in settings:
        _write_cred_file(Path(settings["pgdata_path"]))
    if "mongo_data_path" in settings:
        _write_cred_file(Path(settings["mongo_data_path"]))
    if "starrocks_data_path" in settings:
        _write_cred_file(Path(settings["starrocks_data_path"]))

    return written


# ============================================================================
# Main
# ============================================================================

def main():
    print()
    print("  Social Data Pipeline - Database Configuration")
    print("  =============================================")
    print()
    print("  Configure database infrastructure (PostgreSQL, MongoDB, StarRocks).")
    print("  Press Enter to accept defaults shown in [brackets].")
    print()

    hw = detect_hardware()
    settings = run_questionnaire(hw)

    # Build file list
    files_to_write = []

    # .env
    files_to_write.append((ROOT / ".env", generate_env(settings)))

    # config/db/postgres.yaml
    if "postgres" in settings["databases"]:
        files_to_write.append((
            CONFIG_DIR / "db" / "postgres.yaml",
            generate_db_postgres_yaml(settings),
        ))
        # postgresql.local.conf
        files_to_write.append((
            CONFIG_DIR / "postgres" / "postgresql.local.conf",
            generate_postgresql_local_conf(settings),
        ))

        # pg_hba.local.conf (auth-enabled only)
        if settings.get("auth_enabled"):
            files_to_write.append((
                CONFIG_DIR / "postgres" / "pg_hba.local.conf",
                generate_pg_hba_local_conf(settings),
            ))

    # config/db/mongo.yaml
    if "mongo" in settings["databases"]:
        files_to_write.append((
            CONFIG_DIR / "db" / "mongo.yaml",
            generate_db_mongo_yaml(settings),
        ))

    # config/db/starrocks.yaml + conf files
    if "starrocks" in settings["databases"]:
        files_to_write.append((
            CONFIG_DIR / "db" / "starrocks.yaml",
            generate_db_starrocks_yaml(settings),
        ))
        files_to_write.append((
            CONFIG_DIR / "starrocks" / "fe.local.conf",
            generate_starrocks_fe_conf(settings),
        ))
        files_to_write.append((
            CONFIG_DIR / "starrocks" / "be.local.conf",
            generate_starrocks_be_conf(settings),
        ))

    # docker-compose.override.yml (tablespace volumes / SR multi-disk)
    if "tablespaces" in settings:
        override_content = generate_docker_compose_override(settings)
        if override_content:
            files_to_write.append((
                ROOT / "docker-compose.override.yml",
                override_content,
            ))

    # Summary and confirm
    print_summary(settings, files_to_write)

    if not ask_bool("Write these files?", True, tag="db_write_files"):
        print("\n  Aborted. No files written.\n")
        sys.exit(0)

    print()
    write_files(files_to_write)

    # Write RO user credentials to database data volumes
    if settings.get("auth_enabled") and settings.get("ro_username"):
        cred_files = _write_ro_credentials(settings)
        for cf in cred_files:
            print(f"  Written:   {cf} (chmod 600)")

    print(f"\n  Done! Database configuration has been generated.")

    if settings.get("auth_enabled"):
        print(f"\n  IMPORTANT: Remember your admin password — it is not stored anywhere.")
        print(f"  If lost, recover with: python sdp.py db recover-password")

    print(f"\n  Next step:")
    print(f"    python sdp.py source add <name>   # Add a data source")
    print()

    return settings
