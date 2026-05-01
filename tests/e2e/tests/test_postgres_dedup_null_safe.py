"""E2E: dedup is null-safe AND deterministic against the full behavior table.

Bug class: silent data corruption from non-NULL-safe upsert WHERE / dedup ORDER BY,
plus non-deterministic ORDER BY ties.

Reddit pre-2010 dumps lack any retrieval timestamp, so `retrieved_utc` ingests
as NULL. The old SQL had two separate failure modes against that data:

  - Cross-batch: `WHERE table.retrieved_utc < EXCLUDED.retrieved_utc` evaluates
    to NULL → false on any NULL operand. A row that started life as NULL would
    silently never get its retrieved_utc updated even when newer ingests
    carried a real value, so the DB would forever show stale NULL.
  - Within-batch: `ORDER BY retrieved_utc DESC` defaults to NULLS FIRST in PG.
    Within-batch dedup picked the NULL row over rows with real values when the
    same id appeared twice (e.g. 2006-01 + a 2024 re-scrape colliding).

The fix wraps the order column in `COALESCE(..., -1)` everywhere, on both
sides of the WHERE compare, and appends `ctid` as the final ORDER BY
tiebreaker for run-to-run / cross-machine determinism.

These tests drive the full ingestion path (parse → fast-load + dedup, then
ON-CONFLICT on a re-ingest) and assert row contents per behavior-table row.
SQL-shape pinning lives in tests/db/test_ingest_dedup_sql.py.
"""

from tests.e2e.helpers.sdp import SDPSession, run_sdp, wait_for_healthy
from tests.e2e.helpers.fixtures import place_reddit_fixture_file
from tests.e2e.helpers.db import (
    pg_connect,
    pg_row_count,
    pg_select_one,
)


PG_DB_SETUP = {
    "db_data_path": "",
    "db_databases": "1",
    "db_pgdata_path": "",
    "db_export_path": "",
    "db_name": "",
    "db_pg_port": "",
    "db_tablespaces": "",
    "db_filesystem": "1",
    "db_pgtune_method": "3",
    "db_pg_mem_limit": "0",
    "db_auth": "",
    "db_write_files": "",
}

PG_SOURCE_ADD = {
    "src_data_types": "",
    "src_dumps_path": "",
    "src_extracted_path": "",
    "src_parsed_path": "",
    "src_output_path": "",
    "src_file_format": "1",            # parquet
    "src_parquet_rg_size": "",
    "src_profiles": "1,4",             # parse + postgres_ingest
    "src_parse_workers": "2",
    "src_pg_prefer_lingua": "n",
    "src_pg_index_workers": "2",
    "src_write_files": "",
}


def _setup_pg_with_reddit(*fixtures):
    """Standard preamble: db setup, source add, place fixtures, start PG.

    Each fixture is a (filename, data_type) tuple.
    """
    rc, output = SDPSession(PG_DB_SETUP).run_interactive("db setup")
    assert rc == 0, f"db setup failed:\n{output}"
    rc, output = SDPSession(PG_SOURCE_ADD).run_interactive("source add reddit")
    assert rc == 0, f"source add failed:\n{output}"
    for fname, dt in fixtures:
        place_reddit_fixture_file(fname, data_type=dt)
    result = run_sdp("db start postgres")
    assert result.returncode == 0, f"db start failed:\n{result.stderr}"
    wait_for_healthy("postgres")


def _ingest(build=False):
    """Run parse → postgres_ingest. Returns the ingest CompletedProcess."""
    suffix = " --build" if build else ""
    parse = run_sdp(f"run parse --source reddit{suffix}")
    assert parse.returncode == 0, f"parse failed:\n{parse.stderr}"
    return run_sdp(f"run postgres_ingest --source reddit{suffix}")


# -----------------------------------------------------------------------------
# Cross-batch coverage (4 tests) — exercises get_ingest_query WHERE clause
# -----------------------------------------------------------------------------

def test_upsert_existing_null_replaced_by_valid(workspace):
    """Existing NULL retrieved_utc must be overwritten when incoming has a value.

    Pre-fix: NULL < value evaluates NULL/false in SQL; the update silently
    skips and the stale NULL row stays.
    """
    _setup_pg_with_reddit(("RS_2006-01.ndjson", "submissions"))
    try:
        first = _ingest(build=True)
        assert first.returncode == 0, f"first ingest failed:\n{first.stderr}"

        # Second round: place an overlapping fixture with valid retrieved_utc.
        place_reddit_fixture_file("RS_2024-04.ndjson", data_type="submissions")
        second = _ingest()
        assert second.returncode == 0, f"second ingest failed:\n{second.stderr}\n{second.stdout}"

        conn = pg_connect()
        try:
            row = pg_select_one(conn, "reddit", "submissions", "sqh",
                                columns=("id", "retrieved_utc", "dataset"))
            assert row is not None, "row id='sqh' missing after re-ingest"
            assert row["retrieved_utc"] == 1710000000, (
                f"existing NULL was not replaced: row={row}"
            )
        finally:
            conn.close()
    finally:
        run_sdp("db stop postgres")


def test_upsert_existing_valid_not_downgraded_by_null(workspace):
    """Existing valid retrieved_utc must NOT be overwritten by an incoming NULL.

    The COALESCE-tuple compare yields valid > -1, so `WHERE` is false → no update.
    """
    _setup_pg_with_reddit(("RS_2024-01.ndjson", "submissions"))
    try:
        first = _ingest(build=True)
        assert first.returncode == 0, f"first ingest failed:\n{first.stderr}"

        place_reddit_fixture_file("RS_2024-05.ndjson", data_type="submissions")
        second = _ingest()
        assert second.returncode == 0, f"second ingest failed:\n{second.stderr}\n{second.stdout}"

        conn = pg_connect()
        try:
            row = pg_select_one(conn, "reddit", "submissions", "18x9a2b",
                                columns=("id", "retrieved_utc"))
            assert row is not None, "row id='18x9a2b' missing"
            assert row["retrieved_utc"] == 1704153600, (
                "existing valid retrieved_utc was downgraded by NULL incoming row: "
                f"row={row}"
            )
        finally:
            conn.close()
    finally:
        run_sdp("db stop postgres")


def test_upsert_both_null_idempotent(workspace):
    """Re-ingesting the same all-NULL fixture must not error or change row count."""
    _setup_pg_with_reddit(("RS_2006-01.ndjson", "submissions"))
    try:
        first = _ingest(build=True)
        assert first.returncode == 0, f"first ingest failed:\n{first.stderr}"

        conn = pg_connect()
        try:
            count_first = pg_row_count(conn, "reddit", "submissions")
            row_first = pg_select_one(conn, "reddit", "submissions", "sqh",
                                      columns=("id", "retrieved_utc"))
        finally:
            conn.close()
        assert count_first == 1
        assert row_first["retrieved_utc"] is None

        # Re-run. Files already in state → either skip or no-op upsert.
        second = _ingest()
        assert second.returncode == 0, f"second ingest failed:\n{second.stderr}\n{second.stdout}"

        conn = pg_connect()
        try:
            count_second = pg_row_count(conn, "reddit", "submissions")
            row_second = pg_select_one(conn, "reddit", "submissions", "sqh",
                                       columns=("id", "retrieved_utc"))
        finally:
            conn.close()
        assert count_second == count_first, (
            f"row count drift on re-run: was {count_first}, now {count_second}"
        )
        assert row_second["retrieved_utc"] is None
    finally:
        run_sdp("db stop postgres")


def test_upsert_later_valid_replaces_earlier_valid(workspace):
    """Standard 'later wins' must still hold post-fix (regression check)."""
    _setup_pg_with_reddit(("RS_2024-01.ndjson", "submissions"))
    try:
        first = _ingest(build=True)
        assert first.returncode == 0, f"first ingest failed:\n{first.stderr}"

        place_reddit_fixture_file("RS_2024-04.ndjson", data_type="submissions")
        second = _ingest()
        assert second.returncode == 0, f"second ingest failed:\n{second.stderr}\n{second.stdout}"

        conn = pg_connect()
        try:
            row = pg_select_one(conn, "reddit", "submissions", "18x9a2b",
                                columns=("id", "retrieved_utc"))
            assert row["retrieved_utc"] == 1710000000, (
                f"later retrieved_utc did not win the upsert: row={row}"
            )
        finally:
            conn.close()
    finally:
        run_sdp("db stop postgres")


# -----------------------------------------------------------------------------
# Within-batch coverage (5 tests) — exercises delete_duplicates ORDER BY
# -----------------------------------------------------------------------------

def test_dedup_mixed_null_and_valid_keeps_valid(workspace):
    """One file, same id twice — one NULL, one valid. Valid must win."""
    _setup_pg_with_reddit(("RS_2024-03.ndjson", "submissions"))
    try:
        result = _ingest(build=True)
        assert result.returncode == 0, f"ingest failed:\n{result.stderr}\n{result.stdout}"

        conn = pg_connect()
        try:
            count = pg_row_count(conn, "reddit", "submissions")
            assert count == 1, f"dedup should leave 1 row, got {count}"
            row = pg_select_one(conn, "reddit", "submissions", "18x9a2b",
                                columns=("id", "retrieved_utc", "title"))
            assert row["retrieved_utc"] == 1700000000, (
                f"NULL row won over valid row: {row}"
            )
            assert row["title"] == "DUP_SECOND_VALID_RETRIEVED_UTC"
        finally:
            conn.close()
    finally:
        run_sdp("db stop postgres")


def test_dedup_all_null_mixed_datasets_keeps_larger_dataset(workspace):
    """Same id across two datasets, both NULL retrieved_utc. Larger dataset wins."""
    _setup_pg_with_reddit(
        ("RS_2006-01.ndjson", "submissions"),
        ("RS_2006-02.ndjson", "submissions"),
    )
    try:
        result = _ingest(build=True)
        assert result.returncode == 0, f"ingest failed:\n{result.stderr}\n{result.stdout}"

        conn = pg_connect()
        try:
            count = pg_row_count(conn, "reddit", "submissions")
            assert count == 1, f"dedup should leave 1 row, got {count}"
            row = pg_select_one(conn, "reddit", "submissions", "sqh",
                                columns=("id", "retrieved_utc", "dataset"))
            assert row["retrieved_utc"] is None
            # dataset is char(7), so '2006-02' may come back padded; strip.
            assert row["dataset"].rstrip() == "2006-02", (
                f"larger dataset did not win cross-dataset all-NULL tie: {row}"
            )
        finally:
            conn.close()
    finally:
        run_sdp("db stop postgres")


def test_dedup_all_null_same_dataset_keeps_earliest_ctid(workspace):
    """Same id, same dataset, both NULL retrieved_utc. Earliest-inserted wins.

    The first row in the source file gets COPYed first → smallest ctid →
    `ROW_NUMBER` 1 → survives. Distinguish by `title`.
    """
    _setup_pg_with_reddit(("RS_2024-06.ndjson", "submissions"))
    try:
        result = _ingest(build=True)
        assert result.returncode == 0, f"ingest failed:\n{result.stderr}\n{result.stdout}"

        conn = pg_connect()
        try:
            row = pg_select_one(conn, "reddit", "submissions", "dupaaaa",
                                columns=("id", "retrieved_utc", "title"))
            assert row is not None, "row id='dupaaaa' missing"
            assert row["retrieved_utc"] is None
            assert row["title"] == "AAA_FIRST_INSERTED_NULL", (
                f"first-inserted row did not survive ctid tiebreaker: {row}"
            )
        finally:
            conn.close()
    finally:
        run_sdp("db stop postgres")


def test_dedup_all_valid_distinct_keeps_largest_retrieved_utc(workspace):
    """Same id, two valid distinct retrieved_utc values. Largest wins (regression check)."""
    _setup_pg_with_reddit(("RS_2024-06.ndjson", "submissions"))
    try:
        result = _ingest(build=True)
        assert result.returncode == 0, f"ingest failed:\n{result.stderr}\n{result.stdout}"

        conn = pg_connect()
        try:
            row = pg_select_one(conn, "reddit", "submissions", "dupbbbb",
                                columns=("id", "retrieved_utc", "title"))
            assert row is not None, "row id='dupbbbb' missing"
            assert row["retrieved_utc"] == 1700000001, (
                f"larger retrieved_utc did not win: {row}"
            )
            assert row["title"] == "BBB_HIGHER_RETRIEVED_UTC"
        finally:
            conn.close()
    finally:
        run_sdp("db stop postgres")


def test_dedup_all_valid_equal_keeps_earliest_ctid(workspace):
    """Same id, dataset, retrieved_utc — earliest ctid (first inserted) wins."""
    _setup_pg_with_reddit(("RS_2024-06.ndjson", "submissions"))
    try:
        result = _ingest(build=True)
        assert result.returncode == 0, f"ingest failed:\n{result.stderr}\n{result.stdout}"

        conn = pg_connect()
        try:
            row = pg_select_one(conn, "reddit", "submissions", "dupcccc",
                                columns=("id", "retrieved_utc", "title"))
            assert row is not None, "row id='dupcccc' missing"
            assert row["retrieved_utc"] == 1700000000
            assert row["title"] == "CCC_FIRST_INSERTED_EQUAL", (
                f"first-inserted row did not win equal-retrieved_utc tie: {row}"
            )
        finally:
            conn.close()
    finally:
        run_sdp("db stop postgres")


# -----------------------------------------------------------------------------
# Determinism check
# -----------------------------------------------------------------------------

def test_dedup_is_deterministic_on_repeat(workspace):
    """Drop and re-ingest the same fixture; surviving rows must match by content.

    Workspace fixture gives us a fresh workspace per test, so we exercise
    determinism within one workspace by dropping the schema between runs.
    Same input + same code → same surviving (id, title, retrieved_utc) tuples.
    """
    _setup_pg_with_reddit(("RS_2024-06.ndjson", "submissions"))
    try:
        first = _ingest(build=True)
        assert first.returncode == 0, f"first ingest failed:\n{first.stderr}"

        conn = pg_connect()
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT id, title, retrieved_utc FROM reddit.submissions "
                "ORDER BY id, title"
            )
            rows_first = cur.fetchall()
            cur.close()
            # Reset for second run.
            conn.execute("DROP SCHEMA reddit CASCADE")
            conn.commit()
        finally:
            conn.close()

        # Wipe ingestion state so the next ingest re-processes the same dump.
        # State lives at <workspace>/data/database/postgres/state_tracking/.
        state_dir = workspace / "data" / "database" / "postgres" / "state_tracking"
        for state_file in state_dir.glob("reddit_postgres_ingest_*.json"):
            state_file.unlink()

        second = _ingest()
        assert second.returncode == 0, f"second ingest failed:\n{second.stderr}\n{second.stdout}"

        conn = pg_connect()
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT id, title, retrieved_utc FROM reddit.submissions "
                "ORDER BY id, title"
            )
            rows_second = cur.fetchall()
            cur.close()
        finally:
            conn.close()

        assert rows_first == rows_second, (
            f"dedup non-deterministic: first={rows_first}, second={rows_second}"
        )
    finally:
        run_sdp("db stop postgres")
