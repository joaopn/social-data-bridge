#!/usr/bin/env python3
"""Run .sql files against PostgreSQL and optionally copy results to data/."""

import argparse
import getpass
import shutil
import sys
import time
from pathlib import Path

import psycopg

AUTH_FILE = Path(__file__).parent / ".env"


def load_env(path: Path) -> dict[str, str]:
    """Read key=value pairs from a .env file."""
    env = {}
    if not path.exists():
        return env
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        key, _, value = line.partition("=")
        env[key.strip()] = value.strip()
    return env

CONTAINER_OUTPUT_DIR = "/export"
SERVER_OUTPUT_DIR = Path("/data/storage/sdp/export")
LOCAL_DATA_DIR = Path(__file__).parent / "data"
QUERIES_DIR = Path(__file__).parent / "queries"


def run_one(sql_file: Path, args):
    """Run a single .sql file."""
    print(f"\n--- Running: {sql_file} ---")

    sql = sql_file.read_text()
    sql = sql.replace("{output_dir}", args.output_dir)

    if args.overwrite:
        csv_name = sql_file.stem + ".csv"
        existing = Path(args.server_output_dir) / csv_name
        if existing.exists():
            existing.unlink()
            print(f"Deleted existing {existing}")

    try:
        conninfo = f"host={args.host} port=5432 dbname={args.db} user={args.user}"
        if args.password:
            conninfo += f" password={args.password}"
        with psycopg.connect(conninfo) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                t0 = time.perf_counter()
                cur.execute(sql)
                elapsed = time.perf_counter() - t0
                print(f"Query executed successfully: {sql_file} ({elapsed:.1f}s)")
    except psycopg.Error as e:
        print(f"PostgreSQL error: {e}", file=sys.stderr)
        return False

    if args.copy:
        csv_name = sql_file.stem + ".csv"
        src = Path(args.server_output_dir) / csv_name
        dst = LOCAL_DATA_DIR / csv_name
        if not src.exists():
            print(f"Warning: expected output {src} not found. Check the COPY TO path in your SQL.", file=sys.stderr)
            return False
        LOCAL_DATA_DIR.mkdir(exist_ok=True)
        shutil.copy2(src, dst)
        print(f"Copied {src} -> {dst}")

    return True


def main():
    parser = argparse.ArgumentParser(description="Run SQL query files against PostgreSQL.")
    parser.add_argument("sql_files", nargs="*", type=Path, help="Path(s) to .sql file(s) to execute")
    parser.add_argument("--all", action="store_true", help="Run all .sql files in the queries/ folder")

    env = load_env(AUTH_FILE)
    parser.add_argument("--host", default=env.get("DB_HOST", "localhost"), help="PostgreSQL host")
    parser.add_argument("--db", default=env.get("DB_NAME", "datasets"), help="Database name")
    parser.add_argument("--user", default=env.get("DB_USER", "readonly"), help="PostgreSQL user")
    parser.add_argument("--output-dir", default=CONTAINER_OUTPUT_DIR, help="In-container output directory for COPY TO (default: /export)")
    parser.add_argument("--server-output-dir", default=str(SERVER_OUTPUT_DIR), help="Server-side path where container volume is mounted (default: /data/storage/sdp/export)")
    parser.add_argument("--copy", action="store_true", help="Copy output CSV from server to data/")
    parser.add_argument("--overwrite", action="store_true", help="Delete existing output file before running query")
    args = parser.parse_args()
    args.password = getpass.getpass("PostgreSQL password: ")

    if args.all and args.sql_files:
        print("Error: cannot specify both --all and explicit sql files", file=sys.stderr)
        sys.exit(1)

    if args.all:
        args.sql_files = sorted(QUERIES_DIR.glob("*.sql"))
        if not args.sql_files:
            print(f"No .sql files found in {QUERIES_DIR}", file=sys.stderr)
            sys.exit(1)
        print(f"Found {len(args.sql_files)} queries in {QUERIES_DIR}")

    if not args.sql_files:
        print("Error: provide sql file(s) or use --all", file=sys.stderr)
        sys.exit(1)

    for sql_file in args.sql_files:
        if not sql_file.exists():
            print(f"Error: {sql_file} not found", file=sys.stderr)
            sys.exit(1)

    failed = []
    for sql_file in args.sql_files:
        if not run_one(sql_file, args):
            failed.append(sql_file)

    if len(args.sql_files) > 1:
        print(f"\n--- Done: {len(args.sql_files) - len(failed)}/{len(args.sql_files)} succeeded ---")
        if failed:
            print(f"Failed: {', '.join(str(f) for f in failed)}", file=sys.stderr)
            sys.exit(1)


if __name__ == "__main__":
    main()
