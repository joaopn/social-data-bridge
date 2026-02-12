"""
CSV to PostgreSQL ingestion for social_data_bridge.
"""

import json
import os
import threading
import psycopg
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Dict

from ...core.config import load_yaml_file, ConfigurationError


# Mandatory fields always included (in this order at start of columns)
MANDATORY_FIELDS = ['dataset', 'id', 'retrieved_utc']

# Mandatory field SQL definitions
MANDATORY_FIELD_SQL = {
    'dataset': 'character(7) NOT NULL',
    'id': 'character varying(7) PRIMARY KEY',
    'retrieved_utc': 'integer'
}

# All TEXT fields use STORAGE EXTERNAL (uncompressed TOAST)
# This disables PostgreSQL compression - use filesystem compression (ZFS, BTRFS) instead


def configure_ingestion_session(
    cur,
    quiet: bool = False,
    parallel_workers: int = 8
) -> Dict[str, any]:
    """
    Configure PostgreSQL session for optimal bulk ingestion.

    Uses ONLY session-level settings that automatically clean up when the
    connection closes. No ALTER SYSTEM calls - if the script crashes, the
    server state is unaffected.

    Settings applied (session-level only):
    - wal_compression = 'zstd' (3-4x WAL size reduction)
    - maintenance_work_mem = shared_buffers / (1 + parallel_workers)
    - max_parallel_maintenance_workers (for parallel index builds)
    - maintenance_io_concurrency = 500 (for NVMe)
    - synchronous_commit = off
    - temp_file_limit = -1

    Note: max_wal_size cannot be set at session level. For large ingestions,
    users should increase it in postgresql.conf (recommend 4x shared_buffers).

    Args:
        cur: Active psycopg cursor. The connection MUST stay open during the
             entire ingestion for settings to remain active.
        quiet: If True, suppress log output (useful for repeated calls).
        parallel_workers: max_parallel_maintenance_workers per index build.

    Returns:
        Dict with applied settings for logging/verification
    """
    settings = {}

    # Get current hardware constraints
    cur.execute("SELECT setting::bigint FROM pg_settings WHERE name = 'shared_buffers'")
    shared_buffers_pages = cur.fetchone()[0]
    shared_buffers_gb = (shared_buffers_pages * 8192) / (1024**3)  # pages to GB

    # Calculate maintenance memory to stay within shared_buffers
    # Each index build uses: maintenance_work_mem * (1 + parallel_workers)
    # Sequential builds: only 1 index at a time
    total_processes = 1 + parallel_workers
    maintenance_mem_mb = max(256, int((shared_buffers_gb * 1024) / total_processes))

    # Session-level settings (auto-cleanup when connection closes)
    cur.execute("SET synchronous_commit = 'off'")
    cur.execute("SET temp_file_limit = '-1'")
    cur.execute(f"SET maintenance_work_mem = '{maintenance_mem_mb}MB'")
    cur.execute(f"SET max_parallel_maintenance_workers = {parallel_workers}")
    cur.execute("SET wal_compression = 'zstd'")
    cur.execute("SET maintenance_io_concurrency = 500")

    settings['maintenance_work_mem'] = f'{maintenance_mem_mb}MB'
    settings['max_parallel_maintenance_workers'] = parallel_workers
    settings['wal_compression'] = 'zstd'
    settings['maintenance_io_concurrency'] = 500

    if not quiet:
        # Log applied settings
        print(f"[sdb] Session configured for ingestion:")
        print(f"      shared_buffers = {shared_buffers_gb:.1f}GB (detected)")
        for key, value in settings.items():
            print(f"      {key} = {value}")

        # Warn if max_wal_size is low (can't change at session level)
        cur.execute("SELECT setting FROM pg_settings WHERE name = 'max_wal_size'")
        max_wal_size = cur.fetchone()[0]
        recommended_wal_gb = max(16, min(300, int(shared_buffers_gb * 4)))
        cur.execute("SELECT pg_size_bytes(%s)", (max_wal_size,))
        current_wal_bytes = cur.fetchone()[0]
        if current_wal_bytes < recommended_wal_gb * 1024**3:
            print(f"      [!] max_wal_size = {max_wal_size} (consider {recommended_wal_gb}GB in postgresql.conf)")

    return settings


# =============================================================================
# ALTER SYSTEM Management for SET LOGGED
# =============================================================================
# SET LOGGED writes an entire UNLOGGED table to WAL. For multi-TB tables this
# can exceed max_wal_size many times over, triggering a checkpoint storm that
# crashes the server. These functions temporarily bump max_wal_size and
# checkpoint_timeout via ALTER SYSTEM, using a disk flag file to guarantee
# original settings are always restored — even after crashes.
#
# Flag file lifecycle:
#   1. Pipeline startup → check for flag → restore if found (crash recovery)
#   2. Before ALTER SYSTEM → write flag with original values
#   3. After SET LOGGED → restore original values → delete flag
#   4. On crash → next startup handles step 1
# =============================================================================

_ALTER_SYSTEM_FLAG = '.alter_system_pending.json'
_SAFE_ALTER_SYSTEM_PARAMS = frozenset({'max_wal_size', 'checkpoint_timeout'})
_set_logged_lock = threading.Lock()


def _get_flag_path(pgdata_path: str) -> str:
    """Get path to the ALTER SYSTEM flag file in the shared state directory."""
    state_dir = os.path.join(pgdata_path, 'state_tracking')
    os.makedirs(state_dir, exist_ok=True)
    return os.path.join(state_dir, _ALTER_SYSTEM_FLAG)


def _restore_settings_from_flag(
    flag_path: str,
    dbname: str,
    host: str,
    port: int,
    user: str
):
    """
    Read a flag file, restore original settings via ALTER SYSTEM, reload config,
    and delete the flag file.
    """
    with open(flag_path, 'r') as f:
        flag_data = json.load(f)

    original = flag_data.get('original_settings', {})
    if not original:
        os.remove(flag_path)
        return

    with psycopg.connect(dbname=dbname, user=user, host=host, port=port, autocommit=True) as conn:
        with conn.cursor() as cur:
            for param, value in original.items():
                if param not in _SAFE_ALTER_SYSTEM_PARAMS:
                    continue
                safe_value = str(value).replace("'", "''")
                cur.execute(f"ALTER SYSTEM SET {param} = '{safe_value}'")
            cur.execute("SELECT pg_reload_conf()")

    os.remove(flag_path)


def restore_alter_system_if_needed(
    pgdata_path: str,
    dbname: str,
    host: str = '127.0.0.1',
    port: int = 5432,
    user: str = 'postgres'
) -> bool:
    """
    Check for ALTER SYSTEM flag file and restore original settings if found.

    Call at pipeline startup to handle crashes that left modified server
    settings. Safe to call multiple times — no-ops when no flag exists.

    Returns True if settings were restored. Raises on failure to restore
    (flag file is preserved for manual recovery).
    """
    flag_path = _get_flag_path(pgdata_path)
    if not os.path.exists(flag_path):
        return False

    try:
        with open(flag_path, 'r') as f:
            flag_data = json.load(f)
    except (json.JSONDecodeError, IOError):
        print("[sdb] WARNING: Corrupt ALTER SYSTEM flag file, removing")
        try:
            os.remove(flag_path)
        except OSError:
            pass
        return False

    original = flag_data.get('original_settings', {})
    if not original:
        try:
            os.remove(flag_path)
        except OSError:
            pass
        return False

    table = flag_data.get('table', 'unknown')
    timestamp = flag_data.get('timestamp', 'unknown')
    print(f"[sdb] ALTER SYSTEM flag found (crash during SET LOGGED on {table} at {timestamp})")
    print(f"[sdb] Restoring original PostgreSQL settings:")
    for param, value in original.items():
        if param in _SAFE_ALTER_SYSTEM_PARAMS:
            print(f"      {param} = {value}")

    try:
        _restore_settings_from_flag(flag_path, dbname, host, port, user)
        print("[sdb] Settings restored successfully")
        return True
    except Exception as e:
        print(f"[sdb] ERROR: Failed to restore settings: {e}")
        print(f"[sdb] Flag file: {flag_path}")
        print(f"[sdb] Manual fix: check postgresql.auto.conf, then SELECT pg_reload_conf()")
        raise


def _prepare_wal_for_set_logged(
    full_table: str,
    pgdata_path: str,
    dbname: str,
    host: str,
    port: int,
    user: str
) -> Optional[str]:
    """
    Check table size and temporarily adjust WAL settings for SET LOGGED.

    If the estimated WAL output exceeds the current max_wal_size, temporarily
    bumps max_wal_size and checkpoint_timeout via ALTER SYSTEM to prevent
    checkpoint storms. Writes a flag file with original settings BEFORE any
    changes for crash recovery.

    Returns:
        Flag file path if settings were changed, None if no changes needed.

    Raises:
        RuntimeError: If insufficient disk space for estimated WAL.
    """
    with psycopg.connect(dbname=dbname, user=user, host=host, port=port) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_total_relation_size(%s)", (full_table,))
            table_size = cur.fetchone()[0]

            cur.execute("""
                SELECT
                    pg_size_bytes(current_setting('max_wal_size')),
                    current_setting('max_wal_size'),
                    current_setting('checkpoint_timeout'),
                    EXTRACT(EPOCH FROM current_setting('checkpoint_timeout')::interval)::bigint
            """)
            wal_bytes, wal_setting, timeout_setting, timeout_secs = cur.fetchone()

    table_gb = table_size / (1024**3)

    # SET LOGGED writes the full table to WAL. With wal_compression=zstd
    # (set by configure_ingestion_session), expect 2-4x compression on text.
    # Use 2x as conservative lower bound for headroom.
    estimated_wal = table_size // 2
    estimated_wal_gb = estimated_wal / (1024**3)

    print(f"[sdb] Pre-flight for SET LOGGED on {full_table}:")
    print(f"      Table size: {table_gb:.1f} GB")
    print(f"      Estimated WAL (zstd): ~{estimated_wal_gb:.0f} GB")
    print(f"      Current max_wal_size: {wal_setting}")

    if estimated_wal <= wal_bytes:
        print("      max_wal_size sufficient, no adjustment needed")
        return None

    # Check available disk space (WAL lives on the same partition as pgdata)
    stat = os.statvfs(pgdata_path)
    available = stat.f_bavail * stat.f_frsize
    available_gb = available / (1024**3)
    print(f"      Available disk: {available_gb:.0f} GB")

    if estimated_wal > int(available * 0.8):
        raise RuntimeError(
            f"Insufficient disk for SET LOGGED on {full_table}.\n"
            f"  Table: {table_gb:.1f} GB, estimated WAL: ~{estimated_wal_gb:.0f} GB\n"
            f"  Available: {available_gb:.0f} GB (need ~{estimated_wal_gb / 0.8:.0f} GB)\n"
            f"  Options:\n"
            f"    1. Free disk space or use a larger volume\n"
            f"    2. Set fast_initial_load: false in postgres pipeline config\n"
            f"       (uses standard ON CONFLICT ingestion — slower but no SET LOGGED needed)"
        )

    # New max_wal_size = estimated WAL (allows ~1 size-triggered checkpoint)
    new_wal_mb = int(estimated_wal / (1024**2))
    new_wal_setting = f'{new_wal_mb}MB'

    # Scale checkpoint_timeout proportionally to prevent time-based storms
    scale = estimated_wal / max(1, wal_bytes)
    new_timeout_secs = int(timeout_secs * scale)
    new_timeout_min = max(1, new_timeout_secs // 60)
    new_timeout_setting = f'{new_timeout_min}min'

    print(f"      Adjusting: max_wal_size {wal_setting} -> {new_wal_setting}")
    print(f"      Adjusting: checkpoint_timeout {timeout_setting} -> {new_timeout_setting}")

    # Write flag file BEFORE any ALTER SYSTEM changes
    flag_path = _get_flag_path(pgdata_path)
    flag_data = {
        'original_settings': {
            'max_wal_size': wal_setting,
            'checkpoint_timeout': timeout_setting,
        },
        'applied_settings': {
            'max_wal_size': new_wal_setting,
            'checkpoint_timeout': new_timeout_setting,
        },
        'table': full_table,
        'timestamp': datetime.now(timezone.utc).isoformat(),
    }
    with open(flag_path, 'w') as f:
        json.dump(flag_data, f, indent=2)

    # Apply via ALTER SYSTEM + reload
    try:
        with psycopg.connect(dbname=dbname, user=user, host=host, port=port, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(f"ALTER SYSTEM SET max_wal_size = '{new_wal_setting}'")
                cur.execute(f"ALTER SYSTEM SET checkpoint_timeout = '{new_timeout_setting}'")
                cur.execute("SELECT pg_reload_conf()")
        print("      Settings applied, configuration reloaded")
        return flag_path
    except Exception as e:
        print(f"[sdb] WARNING: ALTER SYSTEM failed ({e}), proceeding with current settings")
        try:
            os.remove(flag_path)
        except OSError:
            pass
        return None


def yaml_type_to_sql(type_def) -> str:
    """
    Convert YAML type definition to PostgreSQL type.
    
    Args:
        type_def: Can be 'integer', 'boolean', 'float', 'text', 
                  or ['char', N] / ['varchar', N]
    
    Returns:
        PostgreSQL column type string
    """
    if isinstance(type_def, list):
        type_name, length = type_def[0], type_def[1]
        if type_name == 'char':
            return f'character({length})'
        elif type_name == 'varchar':
            return f'character varying({length})'
    elif type_def == 'integer':
        return 'integer'
    elif type_def == 'bigint':
        return 'bigint'
    elif type_def == 'boolean':
        return 'boolean'
    elif type_def == 'float':
        return 'real'
    elif type_def == 'text':
        return 'TEXT STORAGE EXTERNAL'
    
    # Default to TEXT STORAGE EXTERNAL for unknown types (no compression)
    return 'TEXT STORAGE EXTERNAL'


def get_column_list(data_type: str, config_dir: str, csv_file: str = None) -> List[str]:
    """
    Get ordered list of columns for a data type.
    
    Order: [dataset, id, retrieved_utc, ...fields from YAML..., (lingua fields if applicable)]
    
    Args:
        data_type: Data type key (e.g., 'submissions', 'comments')
        config_dir: Directory containing field_list.yaml (platform-specific config dir)
        csv_file: Optional CSV file path - if contains 'lingua', lingua columns are appended
        
    Returns:
        List of column names in order
        
    Raises:
        ConfigurationError: If config file is missing or data type not configured
    """
    field_list = load_yaml_file(Path(config_dir) / "field_list.yaml")
    if field_list is None:
        raise ConfigurationError(f"Required config file not found: {config_dir}/field_list.yaml")
    
    yaml_fields = field_list.get(data_type, [])
    if not yaml_fields:
        raise ConfigurationError(f"No fields configured for data type: {data_type}")
    
    # Mandatory fields first, then YAML fields
    columns = MANDATORY_FIELDS + yaml_fields
    
    # Append lingua columns if this is a lingua file
    if csv_file and 'lingua' in csv_file:
        columns = columns + ['lang', 'lang_prob', 'lang2', 'lang2_prob']
    
    return columns


def get_create_table_query(
    data_type: str, 
    schema: str, 
    table: str,
    config_dir: str,
    csv_file: str = None,
    unlogged: bool = False,
    include_pk: bool = True
) -> str:
    """
    Generate CREATE TABLE query dynamically from YAML configuration.
    
    All TEXT fields use STORAGE EXTERNAL (uncompressed TOAST) for external
    filesystem compression (ZFS, BTRFS).
    
    Args:
        data_type: 'submissions' or 'comments'
        schema: Database schema name
        table: Table name
        config_dir: Directory containing config files
        csv_file: Optional CSV file path - if contains 'lingua', lingua columns are included
        unlogged: If True, create UNLOGGED table (faster, no WAL, no crash recovery)
        include_pk: If True, include PRIMARY KEY on id column
        
    Returns:
        CREATE TABLE SQL query
        
    Raises:
        ConfigurationError: If config files are missing
    """
    
    full_table = f"{schema}.{table}"
    
    # Load field types
    field_types = load_yaml_file(Path(config_dir) / "field_types.yaml")
    if field_types is None:
        raise ConfigurationError(f"Required config file not found: {config_dir}/field_types.yaml")
    
    # Get column list (includes lingua columns if csv_file is a lingua file)
    columns = get_column_list(data_type, config_dir, csv_file)
    
    # Build column definitions
    col_defs = []
    for col in columns:
        if col == 'id':
            # Handle id column with optional PRIMARY KEY
            if include_pk:
                col_defs.append(f"    {col} {MANDATORY_FIELD_SQL[col]}")
            else:
                col_defs.append(f"    {col} character varying(7)")
        elif col in MANDATORY_FIELD_SQL:
            col_defs.append(f"    {col} {MANDATORY_FIELD_SQL[col]}")
        elif col in field_types:
            sql_type = yaml_type_to_sql(field_types[col])
            col_defs.append(f"    {col} {sql_type}")
        else:
            # Default to TEXT for unknown fields
            col_defs.append(f"    {col} TEXT")
    
    columns_sql = ",\n".join(col_defs)
    
    # Build CREATE statement
    create_type = "UNLOGGED TABLE" if unlogged else "TABLE"
    
    return f"""
        CREATE {create_type} IF NOT EXISTS {full_table}
        (
{columns_sql}
        );"""


def get_ingest_query(
    data_type: str, 
    schema: str, 
    table: str, 
    check_duplicates: bool,
    config_dir: str,
    csv_file: str = None
) -> str:
    """
    Generate COPY/INSERT query dynamically from YAML configuration.
    
    Args:
        data_type: 'submissions' or 'comments'
        schema: Database schema name
        table: Table name
        check_duplicates: Whether to handle duplicate IDs
        config_dir: Directory containing config files
        csv_file: Optional CSV file path - if contains 'lingua', lingua columns are included
        
    Returns:
        SQL query for data ingestion
    """
    
    full_table = f"{schema}.{table}"
    temp_table = f"temp_{data_type}"
    
    # Get column list from YAML (includes lingua columns if csv_file is a lingua file)
    columns_list = get_column_list(data_type, config_dir, csv_file)
    columns = ", ".join(columns_list)
    
    # Fields to update on conflict (all except 'id' which is the primary key)
    update_fields = [col for col in columns_list if col != 'id']
    update_set = ",\n                ".join(
        f"{col} = EXCLUDED.{col}" for col in update_fields
    )
    
    # Build COPY options - add FORCE_NULL for lingua probability columns (empty string -> NULL)
    copy_options = "FORMAT csv, HEADER true, DELIMITER ','"
    if csv_file and 'lingua' in csv_file:
        copy_options += ", FORCE_NULL (lang_prob, lang2_prob)"
    
    if not check_duplicates:
        return f"""
            COPY {full_table}({columns})
            FROM '%s'
            WITH ({copy_options});
            """
    else:
        return f"""
            CREATE TEMPORARY TABLE {temp_table}
            AS SELECT * FROM {full_table} LIMIT 0;

            COPY {temp_table}({columns})
            FROM '%s'
            WITH ({copy_options});

            WITH latest_rows AS (
                SELECT DISTINCT ON (id)
                    {columns}
                FROM {temp_table}
                ORDER BY id, retrieved_utc DESC
            )
            INSERT INTO {full_table}
                ({columns})
            SELECT
                {columns}
            FROM
                latest_rows
            ON CONFLICT (id) DO UPDATE SET
                {update_set}
            WHERE
                {full_table}.retrieved_utc < EXCLUDED.retrieved_utc;

            DROP TABLE {temp_table};
            """


def execute_query(
    query: str,
    dbname: str,
    host: str = '127.0.0.1',
    port: int = 5432,
    user: str = 'postgres',
    args: List[str] = None
):
    """Execute a query, optionally with arguments."""
    
    with psycopg.connect(dbname=dbname, user=user, host=host, port=port) as conn:
        with conn.cursor() as curr:
            if args is None or len(args) == 0:
                try:
                    curr.execute(query)
                    conn.commit()
                except Exception as e:
                    logging.error("Exception occurred", exc_info=True)
                    raise
            else:
                for arg in args:
                    try:
                        curr.execute(query % arg)
                        conn.commit()
                    except Exception as e:
                        logging.error(f"Exception occurred with arg {arg}", exc_info=True)
                        raise


def index_exists(
    index_name: str,
    dbname: str,
    host: str = '127.0.0.1',
    port: int = 5432,
    user: str = 'postgres'
) -> bool:
    """Check if an index exists in the database."""
    query = "SELECT 1 FROM pg_indexes WHERE indexname = %s"
    with psycopg.connect(dbname=dbname, user=user, host=host, port=port) as conn:
        with conn.cursor() as curr:
            curr.execute(query, (index_name,))
            return curr.fetchone() is not None


def table_exists(
    table: str,
    schema: str,
    dbname: str,
    host: str = '127.0.0.1',
    port: int = 5432,
    user: str = 'postgres'
) -> bool:
    """Check if a table exists in the database."""
    query = """
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = %s AND table_name = %s
    """
    try:
        with psycopg.connect(dbname=dbname, user=user, host=host, port=port) as conn:
            with conn.cursor() as curr:
                curr.execute(query, (schema, table))
                return curr.fetchone() is not None
    except Exception:
        return False


def analyze_table(
    table: str,
    schema: str,
    dbname: str,
    host: str = '127.0.0.1',
    port: int = 5432,
    user: str = 'postgres'
):
    """Run ANALYZE on a table. Used after initial bulk load."""
    full_table = f"{schema}.{table}"
    
    with psycopg.connect(dbname=dbname, user=user, host=host, port=port, autocommit=True) as conn:
        with conn.cursor() as curr:
            curr.execute(f"ANALYZE {full_table}")
    
    print(f"[sdb] ANALYZE complete: {full_table}")


# =============================================================================
# Fast Initial Load Functions
# =============================================================================

def delete_duplicates(
    table: str,
    schema: str,
    dbname: str,
    host: str = '127.0.0.1',
    port: int = 5432,
    user: str = 'postgres',
    order_column: Optional[str] = 'retrieved_utc'
) -> int:
    """
    Delete duplicate rows keeping the best one per id.
    
    Uses ctid + ROW_NUMBER() window function. No temporary index needed -
    PostgreSQL's external merge sort handles this efficiently for rare duplicates.
    
    Args:
        table: Table name
        schema: Schema name
        dbname: Database name
        host: Database host
        port: Database port
        user: Database user
        order_column: Column to use for ordering duplicates (keeps highest value).
                      If None, arbitrary row is kept per id.
        
    Returns:
        Number of rows deleted
    """
    full_table = f"{schema}.{table}"
    
    # Build ORDER BY clause
    if order_column:
        order_by = f"id, {order_column} DESC"
    else:
        order_by = "id"
    
    query = f"""
        DELETE FROM {full_table}
        WHERE ctid IN (
            SELECT ctid FROM (
                SELECT ctid, ROW_NUMBER() OVER (
                    PARTITION BY id ORDER BY {order_by}
                ) as rn
                FROM {full_table}
            ) sub WHERE rn > 1
        );
    """
    
    print(f"[sdb] Removing duplicates from {full_table}...")
    
    with psycopg.connect(dbname=dbname, user=user, host=host, port=port) as conn:
        with conn.cursor() as curr:
            curr.execute(query)
            deleted_count = curr.rowcount
            conn.commit()
    
    print(f"[sdb] Deleted {deleted_count} duplicate rows from {full_table}")
    return deleted_count


def finalize_fast_load_table(
    table: str,
    schema: str,
    dbname: str,
    host: str = '127.0.0.1',
    port: int = 5432,
    user: str = 'postgres',
    fk_reference_table: Optional[str] = None,
    pgdata_path: Optional[str] = None
):
    """
    Finalize table after fast initial load: add PK, optionally add FK, VACUUM FREEZE, SET LOGGED.
    
    Steps:
    1. ADD PRIMARY KEY (id)
    2. ADD FOREIGN KEY (if fk_reference_table provided)
    3. VACUUM FREEZE (mark all tuples as frozen, update visibility map)
    4. SET LOGGED (ensure durability - triggers full WAL write)
    
    Args:
        table: Table name
        schema: Schema name
        dbname: Database name
        host: Database host
        port: Database port
        user: Database user
        fk_reference_table: If provided, add FK constraint referencing this table (same schema)
    """
    full_table = f"{schema}.{table}"
    
    # Add PRIMARY KEY
    print(f"[sdb] Adding PRIMARY KEY to {full_table}...")
    add_pk_query = f"ALTER TABLE {full_table} ADD PRIMARY KEY (id);"
    
    with psycopg.connect(dbname=dbname, user=user, host=host, port=port) as conn:
        with conn.cursor() as curr:
            curr.execute(add_pk_query)
            conn.commit()
    print(f"[sdb] PRIMARY KEY added to {full_table}")
    
    # Add FOREIGN KEY (if reference table provided)
    if fk_reference_table:
        ref_table = f"{schema}.{fk_reference_table}"
        if table_exists(fk_reference_table, schema, dbname, host, port, user):
            print(f"[sdb] Adding FOREIGN KEY to {full_table} -> {ref_table}...")
            fk_name = f"fk_{table}_id"
            add_fk_query = f"ALTER TABLE {full_table} ADD CONSTRAINT {fk_name} FOREIGN KEY (id) REFERENCES {ref_table}(id);"
            
            with psycopg.connect(dbname=dbname, user=user, host=host, port=port) as conn:
                with conn.cursor() as curr:
                    curr.execute(add_fk_query)
                    conn.commit()
            print(f"[sdb] FOREIGN KEY added to {full_table}")
        else:
            print(f"[sdb] Warning: Reference table {ref_table} not found, skipping FK constraint")
    
    # VACUUM FREEZE (requires autocommit)
    # Configure session for optimal performance (maintenance_io_concurrency, etc.)
    print(f"[sdb] Running VACUUM FREEZE on {full_table}...")
    with psycopg.connect(dbname=dbname, user=user, host=host, port=port, autocommit=True) as conn:
        with conn.cursor() as curr:
            configure_ingestion_session(curr)
            curr.execute(f"VACUUM FREEZE {full_table}")
    print(f"[sdb] VACUUM FREEZE complete on {full_table}")

    # SET LOGGED (ensure durability - triggers full WAL write)
    # For large tables, temporarily adjust server WAL settings to prevent
    # checkpoint storms that can crash the server and destroy UNLOGGED data.
    # A threading lock serializes SET LOGGED across concurrent data types
    # (parallel SET LOGGED would double I/O pressure with no benefit).
    set_logged_query = f"ALTER TABLE {full_table} SET LOGGED;"

    if pgdata_path:
        with _set_logged_lock:
            restore_alter_system_if_needed(pgdata_path, dbname, host, port, user)
            flag_path = _prepare_wal_for_set_logged(
                full_table, pgdata_path, dbname, host, port, user
            )

            print(f"[sdb] Setting table to LOGGED (WAL flush)...")
            try:
                with psycopg.connect(dbname=dbname, user=user, host=host, port=port) as conn:
                    with conn.cursor() as curr:
                        configure_ingestion_session(curr)
                        curr.execute(set_logged_query)
                        conn.commit()
                print(f"[sdb] Table {full_table} is now LOGGED and durable")
            finally:
                if flag_path and os.path.exists(flag_path):
                    try:
                        _restore_settings_from_flag(flag_path, dbname, host, port, user)
                        print("[sdb] WAL settings restored to original values")
                    except Exception as e:
                        print(f"[sdb] WARNING: Failed to restore settings: {e}")
                        print(f"[sdb] Flag file preserved: {flag_path}")
    else:
        print(f"[sdb] Setting table to LOGGED (WAL flush)...")
        with psycopg.connect(dbname=dbname, user=user, host=host, port=port) as conn:
            with conn.cursor() as curr:
                configure_ingestion_session(curr)
                curr.execute(set_logged_query)
                conn.commit()
        print(f"[sdb] Table {full_table} is now LOGGED and durable")


def fast_ingest_csv(
    csv_file: str,
    data_type: str,
    dbname: str,
    schema: str,
    table: str,
    host: str,
    port: int,
    user: str,
    config_dir: str
):
    """
    Fast ingest a single CSV file using blind COPY (no duplicate checking).
    
    Part of fast initial load - just appends data to existing UNLOGGED table.
    Deduplication happens after all files are loaded.
    
    Args:
        csv_file: Path to the CSV file
        data_type: 'submissions' or 'comments'
        dbname: Database name
        schema: Schema name
        table: Table name
        host: Database host
        port: Database port
        user: Database user
        config_dir: Directory containing YAML configuration files
    """
    print(f"[sdb] COPY: {csv_file}")
    
    # Use existing get_ingest_query with check_duplicates=False for blind COPY
    copy_query = get_ingest_query(data_type, schema, table, check_duplicates=False, config_dir=config_dir, csv_file=csv_file)
    execute_query(copy_query, dbname, host, port, user, args=[csv_file])


def create_fast_load_table(
    data_type: str,
    dbname: str,
    schema: str,
    table: str,
    host: str,
    port: int,
    user: str,
    config_dir: str,
    csv_file: str = None
):
    """
    Create UNLOGGED table for fast initial load (no PK, no indexes).
    
    Args:
        data_type: 'submissions' or 'comments'
        dbname: Database name
        schema: Schema name
        table: Table name
        host: Database host
        port: Database port
        user: Database user
        config_dir: Directory containing YAML configuration files
        csv_file: Optional CSV file path for lingua column detection
    """
    print(f"[sdb] Creating UNLOGGED table {schema}.{table} (no PK)...")
    
    # Ensure database and schema exist
    ensure_database_exists(dbname, host, port, user)
    ensure_schema_exists(schema, dbname, host, port, user)
    
    # Use existing get_create_table_query with unlogged=True, include_pk=False
    create_query = get_create_table_query(data_type, schema, table, config_dir, csv_file, unlogged=True, include_pk=False)
    execute_query(create_query, dbname, host, port, user)
    
    print(f"[sdb] Created UNLOGGED table {schema}.{table}")


def create_fast_load_classifier_table(
    table_name: str,
    data_type: str,
    schema: str,
    dbname: str,
    host: str,
    port: int,
    user: str,
    column_list: List[str],
    column_types: Dict[str, str]
):
    """
    Create UNLOGGED classifier table for fast initial load (no PK, no FK).
    
    Args:
        table_name: Classifier table name (e.g., 'submissions_lingua')
        data_type: 'submissions' or 'comments' (for FK reference later)
        schema: Schema name
        dbname: Database name
        host: Database host
        port: Database port
        user: Database user
        column_list: List of column names in CSV order
        column_types: Dict of column_name -> sql_type (excludes 'id')
    """
    print(f"[sdb] Creating UNLOGGED table {schema}.{table_name} (no PK, no FK)...")
    
    ensure_schema_exists(schema, dbname, host, port, user)
    
    create_query = get_classifier_create_table_query(
        table_name, data_type, schema, column_list, column_types,
        use_foreign_key=False, unlogged=True, include_pk=False
    )
    execute_query(create_query, dbname, host, port, user)
    
    print(f"[sdb] Created UNLOGGED table {schema}.{table_name}")


def fast_ingest_classifier_csv(
    csv_file: str,
    table_name: str,
    schema: str,
    dbname: str,
    host: str,
    port: int,
    user: str,
    column_list: List[str],
    nullable_cols: Optional[List[str]] = None
):
    """
    Fast ingest a single classifier CSV file using blind COPY (no duplicate checking).
    
    Args:
        csv_file: Path to the CSV file
        table_name: Classifier table name (e.g., 'submissions_lingua')
        schema: Schema name
        dbname: Database name
        host: Database host
        port: Database port
        user: Database user
        column_list: List of column names in table order
        nullable_cols: Columns that may have empty strings to treat as NULL
    """
    print(f"[sdb] COPY: {csv_file}")
    
    copy_query = get_classifier_ingest_query(
        table_name, schema, column_list, check_duplicates=False, nullable_cols=nullable_cols
    )
    execute_query(copy_query, dbname, host, port, user, args=[csv_file])


def create_index(
    field: str,
    table: str,
    schema: str,
    dbname: str,
    host: str = '127.0.0.1',
    port: int = 5432,
    user: str = 'postgres',
    quiet: bool = False,
    parallel_workers: int = 8
) -> bool:
    """Create an index on a table field. Returns True if created, False if already existed."""

    full_table = f"{schema}.{table}"
    index_name = f"idx_{table}_{field}"

    # Check if index already exists
    if index_exists(index_name, dbname, host, port, user):
        return False

    query = f"CREATE INDEX IF NOT EXISTS {index_name} ON {full_table} ({field});"

    print(f"[sdb] Creating index: {index_name}")

    with psycopg.connect(dbname=dbname, user=user, host=host, port=port) as conn:
        with conn.cursor() as curr:
            # Configure session for optimal index creation (maintenance_work_mem, etc.)
            configure_ingestion_session(curr, quiet=quiet, parallel_workers=parallel_workers)
            try:
                curr.execute(query)
                conn.commit()
                print(f"[sdb] Created index: {index_name}")
                return True
            except Exception as e:
                logging.error("Exception occurred", exc_info=True)
                raise


def ensure_database_exists(
    dbname: str,
    host: str = '127.0.0.1',
    port: int = 5432,
    user: str = 'postgres'
):
    """Create database if it doesn't exist."""
    
    with psycopg.connect(
        dbname='postgres',
        user=user,
        host=host,
        port=port,
        autocommit=True
    ) as conn:
        with conn.cursor() as curr:
            # Check if database exists first
            curr.execute(
                "SELECT 1 FROM pg_database WHERE datname = %s",
                (dbname,)
            )
            if curr.fetchone() is None:
                curr.execute(f"CREATE DATABASE {dbname}")
                print(f"[sdb] Created database: {dbname}")


def ensure_schema_exists(
    schema: str,
    dbname: str,
    host: str = '127.0.0.1',
    port: int = 5432,
    user: str = 'postgres'
):
    """Create schema if it doesn't exist. Thread-safe with exception handling."""
    
    if schema == 'public':
        return  # public schema always exists
    
    with psycopg.connect(
        dbname=dbname,
        user=user,
        host=host,
        port=port,
        autocommit=True
    ) as conn:
        with conn.cursor() as curr:
            try:
                curr.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            except psycopg.errors.UniqueViolation:
                # Schema was created by another concurrent connection - this is fine
                pass


def ingest_csv(
    csv_file: str,
    data_type: str,
    dbname: str,
    schema: str,
    table: Optional[str],
    host: str,
    port: int,
    user: str,
    check_duplicates: bool,
    create_indexes: bool,
    config_dir: str,
    index_fields: Optional[List[str]] = None
):
    """
    Ingest a CSV file into PostgreSQL.
    
    Args:
        csv_file: Path to the CSV file
        data_type: 'submissions' or 'comments'
        dbname: Database name
        schema: Schema name
        table: Table name (None to use data_type)
        host: Database host (hostname or IP)
        port: Database port
        user: Database user
        check_duplicates: Whether to handle duplicates
        create_indexes: Whether to create indexes after ingestion
        config_dir: Directory containing YAML configuration files
        index_fields: Fields to index (uses defaults if None)
    """
    if table is None:
        table = data_type
    
    print(f"[sdb] Starting ingestion: {csv_file} -> {schema}.{table}")
    
    # Ensure database and schema exist
    ensure_database_exists(dbname, host, port, user)
    ensure_schema_exists(schema, dbname, host, port, user)
    
    # Create table if needed (schema from YAML, includes lingua columns if applicable)
    create_query = get_create_table_query(data_type, schema, table, config_dir, csv_file)
    execute_query(create_query, dbname, host, port, user)
    
    # Ingest data (columns from YAML, includes lingua columns if applicable)
    ingest_query = get_ingest_query(data_type, schema, table, check_duplicates, config_dir, csv_file)
    execute_query(ingest_query, dbname, host, port, user, args=[csv_file])
    
    # Create indexes if requested
    if create_indexes:
        if index_fields is None:
            # No default index fields - must be configured per platform
            index_fields = []
        
        for field in index_fields:
            try:
                create_index(field, table, schema, dbname, host, port, user)
            except Exception as e:
                print(f"[sdb] Warning: Failed to create index on {field}: {e}")




# =============================================================================
# Classifier Table Functions
# =============================================================================

def infer_sql_type(values: List[str]) -> tuple:
    """
    Infer SQL type from a list of sample values.
    
    Priority: integer > real > boolean > text
    
    Args:
        values: List of string values from CSV
        
    Returns:
        Tuple of (sql_type, has_empty) where has_empty indicates column has empty values
    """
    has_int = False
    has_float = False
    has_bool = False
    has_empty = False
    
    for val in values:
        if not val or val == "":
            has_empty = True
            continue
        
        # Try integer
        try:
            int(val)
            has_int = True
            continue
        except ValueError:
            pass
        
        # Try float
        try:
            float(val)
            has_float = True
            continue
        except ValueError:
            pass
        
        # Try boolean
        if val.lower() in ('true', 'false'):
            has_bool = True
            continue
        
        # If we hit a non-numeric, non-boolean value, it's text
        return ('text', has_empty)
    
    # Determine type based on what we found
    if has_float:
        return ('real', has_empty)
    if has_int:
        return ('integer', has_empty)
    if has_bool:
        return ('boolean', has_empty)
    
    # All empty or no values - default to text
    return ('text', has_empty)


def infer_classifier_schema(
    csv_file: str,
    n_rows: int = 1000,
    column_overrides: Optional[Dict[str, str]] = None
) -> tuple:
    """
    Infer column list and types for classifier table from CSV data.
    
    Reads N rows from CSV and infers SQL types for all columns.
    Returns columns in CSV header order (important for COPY).
    
    Args:
        csv_file: Path to CSV file
        n_rows: Number of rows to sample for type inference
        column_overrides: Optional dict of column_name -> sql_type overrides
        
    Returns:
        Tuple of (column_list, column_types_dict, nullable_cols) where:
        - column_list: List of column names in CSV order
        - column_types_dict: Dict of column_name -> sql_type
        - nullable_cols: List of columns that have empty values (need FORCE_NULL)
    """
    import csv
    
    column_overrides = column_overrides or {}
    
    with open(csv_file, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        header = reader.fieldnames
        
        if not header:
            raise ValueError(f"CSV file has no header: {csv_file}")
        
        # Keep all columns in original order
        all_cols = list(header)
        
        if not all_cols:
            raise ValueError(f"No columns found in CSV: {csv_file}")
        
        # Collect sample values for each column (except 'id' which is always varchar)
        samples: Dict[str, List[str]] = {col: [] for col in all_cols if col != 'id'}
        
        for i, row in enumerate(reader):
            if i >= n_rows:
                break
            for col in samples:
                samples[col].append(row.get(col, ""))
    
    # Infer types (id is always varchar(7) PRIMARY KEY, handled separately)
    column_types = {}
    nullable_cols = []
    for col in all_cols:
        if col == 'id':
            continue  # id handled separately in table creation
        if col in column_overrides:
            column_types[col] = yaml_type_to_sql(column_overrides[col])
        else:
            sql_type, has_empty = infer_sql_type(samples[col])
            column_types[col] = sql_type
            # Track columns with empty values that aren't text (need FORCE_NULL for COPY)
            if has_empty and sql_type != 'text':
                nullable_cols.append(col)
    
    return all_cols, column_types, nullable_cols


def get_classifier_create_table_query(
    table_name: str,
    data_type: str,
    schema: str,
    column_list: List[str],
    column_types: Dict[str, str],
    use_foreign_key: bool = True,
    unlogged: bool = False,
    include_pk: bool = True
) -> str:
    """
    Generate CREATE TABLE query for a classifier output table.
    
    Classifier tables have:
    - id VARCHAR(7) PRIMARY KEY (unless include_pk=False)
    - Optional FOREIGN KEY to main table (submissions/comments)
    - All other columns from CSV with inferred types
    
    Args:
        table_name: Full table name (e.g., 'submissions_lingua')
        data_type: 'submissions' or 'comments' (for FK reference)
        schema: Database schema name
        column_list: List of column names in CSV order
        column_types: Dict of column_name -> sql_type (excludes 'id')
        use_foreign_key: If True, add FK constraint to main table
        unlogged: If True, create UNLOGGED table (faster, no WAL, no crash recovery)
        include_pk: If True, include PRIMARY KEY on id column
        
    Returns:
        CREATE TABLE SQL query
    """
    full_table = f"{schema}.{table_name}"
    main_table = f"{schema}.{data_type}"
    
    # Build column definitions in CSV order
    col_defs = []
    for col in column_list:
        if col == 'id':
            if include_pk:
                col_defs.append("    id character varying(7) PRIMARY KEY")
            else:
                col_defs.append("    id character varying(7)")
        else:
            sql_type = column_types.get(col, 'text')
            col_defs.append(f"    {col} {sql_type}")
    
    # Add foreign key constraint if enabled (only if also including PK)
    if use_foreign_key and include_pk:
        col_defs.append(f"    CONSTRAINT fk_{table_name}_id FOREIGN KEY (id) REFERENCES {main_table}(id)")
    
    columns_sql = ",\n".join(col_defs)
    
    # Build CREATE statement
    create_type = "UNLOGGED TABLE" if unlogged else "TABLE"
    
    return f"""
        CREATE {create_type} IF NOT EXISTS {full_table}
        (
{columns_sql}
        );"""


def get_classifier_ingest_query(
    table_name: str,
    schema: str,
    column_list: List[str],
    check_duplicates: bool,
    nullable_cols: Optional[List[str]] = None
) -> str:
    """
    Generate COPY/INSERT query for classifier output table.
    
    Mirrors the base table ingestion pattern exactly.
    
    Args:
        table_name: Full table name (e.g., 'submissions_lingua')
        schema: Database schema name
        column_list: List of column names in CSV order
        check_duplicates: Whether to handle duplicate IDs
        nullable_cols: Columns that may have empty strings to treat as NULL
        
    Returns:
        SQL query for data ingestion
    """
    full_table = f"{schema}.{table_name}"
    temp_table = f"temp_{table_name}"
    
    columns = ", ".join(column_list)
    
    # Build COPY options - FORCE_NULL for columns with empty values
    copy_options = "FORMAT csv, HEADER true, DELIMITER ',', NULL ''"
    if nullable_cols:
        force_null_cols = ", ".join(nullable_cols)
        copy_options += f", FORCE_NULL ({force_null_cols})"
    
    # Fields to update on conflict (all except 'id' which is the primary key)
    update_fields = [col for col in column_list if col != 'id']
    update_set = ",\n                ".join(
        f"{col} = EXCLUDED.{col}" for col in update_fields
    )
    
    # Determine ORDER BY clause for deduplication
    # Use retrieved_utc if available to prefer most recent version
    if 'retrieved_utc' in column_list:
        order_by = "id, retrieved_utc DESC"
    else:
        order_by = "id"
    
    if not check_duplicates:
        return f"""
            COPY {full_table}({columns})
            FROM '%s'
            WITH ({copy_options});
            """
    else:
        # Only add WHERE clause if retrieved_utc exists (for preferring newer versions)
        if 'retrieved_utc' in column_list:
            where_clause = f"""
            WHERE
                {full_table}.retrieved_utc < EXCLUDED.retrieved_utc"""
        else:
            where_clause = ""
        
        return f"""
            CREATE TEMPORARY TABLE {temp_table}
            AS SELECT * FROM {full_table} LIMIT 0;

            COPY {temp_table}({columns})
            FROM '%s'
            WITH ({copy_options});

            WITH latest_rows AS (
                SELECT DISTINCT ON (id)
                    {columns}
                FROM {temp_table}
                ORDER BY {order_by}
            )
            INSERT INTO {full_table}
                ({columns})
            SELECT
                {columns}
            FROM
                latest_rows
            ON CONFLICT (id) DO UPDATE SET
                {update_set}{where_clause};

            DROP TABLE {temp_table};
            """


def get_table_column_list(
    table_name: str,
    schema: str,
    dbname: str,
    host: str = '127.0.0.1',
    port: int = 5432,
    user: str = 'postgres'
) -> List[str]:
    """
    Get column names for an existing table in ordinal position order.
    
    Args:
        table_name: Table name (e.g., 'submissions_lingua')
        schema: Database schema name
        dbname: Database name
        host: Database host
        port: Database port
        user: Database user
        
    Returns:
        List of column names in table order
    """
    
    query = """
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
    """
    
    columns = []
    try:
        with psycopg.connect(dbname=dbname, user=user, host=host, port=port) as conn:
            with conn.cursor() as curr:
                curr.execute(query, (schema, table_name))
                columns = [row[0] for row in curr.fetchall()]
    except Exception as e:
        print(f"[sdb] Warning: Could not query columns for {table_name}: {e}")
    
    return columns


def ingest_classifier_csv(
    csv_file: str,
    data_type: str,
    classifier_name: str,
    dbname: str,
    schema: str,
    host: str,
    port: int,
    user: str,
    check_duplicates: bool = True,
    type_inference_rows: int = 1000,
    column_overrides: Optional[Dict[str, str]] = None,
    use_foreign_key: bool = True,
    suffix: Optional[str] = None
):
    """
    Ingest a classifier CSV file into PostgreSQL.
    
    Auto-infers column types from CSV data. Creates table if needed.
    Ingests the full CSV file directly via COPY (identical to base table ingestion).
    
    Args:
        csv_file: Path to the CSV file
        data_type: 'submissions' or 'comments'
        classifier_name: Name of the classifier (e.g., 'lingua') - used for logging
        dbname: Database name
        schema: Schema name
        host: Database host
        port: Database port
        user: Database user
        check_duplicates: Whether to handle duplicates
        type_inference_rows: Number of rows to sample for type inference
        column_overrides: Optional column type overrides
        use_foreign_key: If True, add FK constraint to main table (default: True)
        suffix: Suffix for table name (e.g., '_lingua'). If None, uses _{classifier_name}
    """
    # Determine table suffix
    if suffix is None:
        suffix = f"_{classifier_name}"
    
    # Table name: {data_type}{suffix} (e.g., submissions_lingua)
    table_name = f"{data_type}{suffix}"
    
    print(f"[sdb] Starting ingestion: {csv_file} -> {schema}.{table_name}")
    
    # Ensure schema exists
    ensure_schema_exists(schema, dbname, host, port, user)
    
    # Check if table exists FIRST to avoid unnecessary CSV inference
    if not table_exists(table_name, schema, dbname, host, port, user):
        # Table doesn't exist - infer schema from CSV and create it
        print(f"[sdb] Inferring schema from {type_inference_rows} rows...")
        column_list, column_types, nullable_cols = infer_classifier_schema(
            csv_file, type_inference_rows, column_overrides
        )
        
        if nullable_cols:
            print(f"[sdb] Columns with empty values (using FORCE_NULL): {nullable_cols}")
        
        print(f"[sdb] Inferred columns: {column_list}")
        
        # Check if main table exists when FK is requested
        main_table_exists = table_exists(data_type, schema, dbname, host, port, user)
        actual_use_fk = use_foreign_key and main_table_exists
        
        if use_foreign_key and not main_table_exists:
            print(f"[sdb] Warning: Main table {schema}.{data_type} not found, creating without FK")
        
        # Create table
        create_query = get_classifier_create_table_query(
            table_name, data_type, schema, column_list, column_types, 
            use_foreign_key=actual_use_fk
        )
        execute_query(create_query, dbname, host, port, user)
        fk_status = " (with FK)" if actual_use_fk else " (no FK)"
        print(f"[sdb] Created table: {schema}.{table_name}{fk_status}")
    else:
        # Table exists - get schema from database (no CSV inference needed)
        column_list = get_table_column_list(
            table_name, schema, dbname, host, port, user
        )
        # No need to determine nullable_cols - if there was an issue, first file would have failed
        nullable_cols = []
    
    # Ingest data (COPY entire CSV directly, just like base table)
    ingest_query = get_classifier_ingest_query(
        table_name, schema, column_list, check_duplicates, nullable_cols
    )
    execute_query(ingest_query, dbname, host, port, user, args=[csv_file])
    
    print(f"[sdb] Ingestion complete: {schema}.{table_name}")
