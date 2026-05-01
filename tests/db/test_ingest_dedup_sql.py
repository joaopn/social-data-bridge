"""Pin the SQL shape of the dedup paths in postgres/ingest.py.

Bug class: silent data corruption from non-NULL-safe upsert WHERE / ORDER BY.

When `retrieved_utc` is nullable (Reddit pre-2010 dumps), the previous
`WHERE table.retrieved_utc < EXCLUDED.retrieved_utc` evaluated to NULL
on any NULL operand, silently skipping updates that should have replaced
a stale NULL row with a richer one. Within-batch dedup picked NULL over
real values because PG defaults `NULLS FIRST` on `DESC`, and ties on
equal values were resolved non-deterministically by the planner.

This file pins the new SQL: COALESCE on both sides of the WHERE compare,
COALESCE in the ORDER BY (NULL → -1, smaller), explicit `ctid` final
tiebreaker for run-to-run / cross-machine determinism. These are pure
string assertions — they catch typos and contract drift, but DO NOT
verify runtime behavior. Behavior coverage is in
`tests/e2e/tests/test_postgres_dedup_null_safe.py`.
"""

import sys
from unittest.mock import MagicMock

# Stub psycopg before importing the ingest module (matches the pattern
# in test_reddit_column_contract.py — psycopg isn't a test-time dep).
sys.modules.setdefault('psycopg', MagicMock())

from social_data_pipeline.db.postgres.ingest import (  # noqa: E402
    _build_dedup_order_by,
    get_classifier_ingest_query,
    get_ingest_query,
)


# Minimal platform_config the base-table get_ingest_query needs.
PLATFORM = {
    "fields": {
        "submissions": ["retrieved_utc", "created_utc", "author"],
    },
    "mandatory_fields": ["dataset", "id"],
    "primary_key": "id",
    "upsert_order_field": "retrieved_utc",
}


# =============================================================================
# get_ingest_query — base-table COPY/INSERT path
# =============================================================================

class TestGetIngestQueryNullSafe:
    def test_where_uses_coalesce_both_sides(self):
        """WHERE must wrap both table.col and EXCLUDED.col in COALESCE(-1)."""
        sql = get_ingest_query(
            data_type="submissions",
            schema="reddit",
            table="submissions",
            check_duplicates=True,
            platform_config=PLATFORM,
            csv_file="x.csv",
        )
        assert "COALESCE(reddit.submissions.retrieved_utc, -1)" in sql
        assert "COALESCE(EXCLUDED.retrieved_utc, -1)" in sql
        # Form must be a less-than compare; no bare `< EXCLUDED.retrieved_utc`.
        assert "COALESCE(reddit.submissions.retrieved_utc, -1) < COALESCE(EXCLUDED.retrieved_utc, -1)" in sql
        assert "reddit.submissions.retrieved_utc < EXCLUDED.retrieved_utc" not in sql

    def test_order_by_appends_ctid_with_coalesce(self):
        """DISTINCT ON ORDER BY must end in `ctid` and use COALESCE on the order field."""
        sql = get_ingest_query(
            data_type="submissions",
            schema="reddit",
            table="submissions",
            check_duplicates=True,
            platform_config=PLATFORM,
            csv_file="x.csv",
        )
        # ORDER BY appears once after DISTINCT ON; pin the exact form.
        assert "ORDER BY id, COALESCE(retrieved_utc, -1) DESC, ctid" in sql

    def test_no_order_field_falls_back_to_pk_and_ctid(self):
        """Platform without upsert_order_field → ORDER BY id, ctid; no WHERE."""
        platform_no_order = dict(PLATFORM)
        platform_no_order.pop("upsert_order_field")
        sql = get_ingest_query(
            data_type="submissions",
            schema="reddit",
            table="submissions",
            check_duplicates=True,
            platform_config=platform_no_order,
            csv_file="x.csv",
        )
        assert "ORDER BY id, ctid" in sql
        # No WHERE clause attached to the ON CONFLICT DO UPDATE.
        assert "WHERE" not in sql.split("DO UPDATE SET")[1]

    def test_unknown_order_field_does_not_break(self):
        """If upsert_order_field isn't in the column list, fall back cleanly."""
        platform_unknown = dict(PLATFORM)
        platform_unknown["upsert_order_field"] = "not_in_columns"
        sql = get_ingest_query(
            data_type="submissions",
            schema="reddit",
            table="submissions",
            check_duplicates=True,
            platform_config=platform_unknown,
            csv_file="x.csv",
        )
        assert "ORDER BY id, ctid" in sql
        assert "COALESCE(not_in_columns" not in sql
        # No WHERE on the upsert.
        assert "WHERE" not in sql.split("DO UPDATE SET")[1]

    def test_check_duplicates_false_unchanged(self):
        """With check_duplicates=False, query is a plain COPY — no ORDER BY, no WHERE."""
        sql = get_ingest_query(
            data_type="submissions",
            schema="reddit",
            table="submissions",
            check_duplicates=False,
            platform_config=PLATFORM,
            csv_file="x.csv",
        )
        assert "COPY reddit.submissions" in sql
        assert "ORDER BY" not in sql
        assert "ON CONFLICT" not in sql


# =============================================================================
# delete_duplicates ORDER BY builder
# =============================================================================

class TestDeleteDuplicatesOrderBy:
    def test_with_order_and_secondary(self):
        order_by = _build_dedup_order_by(
            pk_column="id",
            order_column="retrieved_utc",
            secondary_order_columns=["dataset"],
        )
        assert order_by == "id, COALESCE(retrieved_utc, -1) DESC, dataset DESC, ctid"

    def test_with_order_no_secondary(self):
        order_by = _build_dedup_order_by(
            pk_column="id",
            order_column="retrieved_utc",
            secondary_order_columns=None,
        )
        assert order_by == "id, COALESCE(retrieved_utc, -1) DESC, ctid"

    def test_no_order_with_secondary(self):
        order_by = _build_dedup_order_by(
            pk_column="id",
            order_column=None,
            secondary_order_columns=["dataset"],
        )
        assert order_by == "id, dataset DESC, ctid"

    def test_no_order_no_secondary(self):
        """Even with no order/secondary columns, ctid must always close the ORDER BY."""
        order_by = _build_dedup_order_by(
            pk_column="id",
            order_column=None,
            secondary_order_columns=None,
        )
        assert order_by == "id, ctid"

    def test_secondary_columns_preserve_order(self):
        order_by = _build_dedup_order_by(
            pk_column="id",
            order_column="retrieved_utc",
            secondary_order_columns=["dataset", "created_utc"],
        )
        # Secondary columns are appended in the order given.
        assert order_by == (
            "id, COALESCE(retrieved_utc, -1) DESC, "
            "dataset DESC, created_utc DESC, ctid"
        )


# =============================================================================
# get_classifier_ingest_query — classifier-table mirror
# =============================================================================

class TestGetClassifierIngestQueryNullSafe:
    def test_where_uses_coalesce_both_sides(self):
        sql = get_classifier_ingest_query(
            table_name="submissions_lingua",
            schema="reddit",
            column_list=["id", "retrieved_utc", "lang", "lang_prob"],
            check_duplicates=True,
            pk_column="id",
            order_field="retrieved_utc",
            csv_file="x.csv",
        )
        assert "COALESCE(reddit.submissions_lingua.retrieved_utc, -1)" in sql
        assert "COALESCE(EXCLUDED.retrieved_utc, -1)" in sql
        assert (
            "COALESCE(reddit.submissions_lingua.retrieved_utc, -1) < "
            "COALESCE(EXCLUDED.retrieved_utc, -1)"
        ) in sql

    def test_order_by_appends_ctid(self):
        sql = get_classifier_ingest_query(
            table_name="submissions_lingua",
            schema="reddit",
            column_list=["id", "retrieved_utc", "lang", "lang_prob"],
            check_duplicates=True,
            pk_column="id",
            order_field="retrieved_utc",
            csv_file="x.csv",
        )
        assert "ORDER BY id, COALESCE(retrieved_utc, -1) DESC, ctid" in sql

    def test_no_order_field_falls_back_to_pk_and_ctid(self):
        sql = get_classifier_ingest_query(
            table_name="submissions_lingua",
            schema="reddit",
            column_list=["id", "lang", "lang_prob"],
            check_duplicates=True,
            pk_column="id",
            order_field=None,
            csv_file="x.csv",
        )
        assert "ORDER BY id, ctid" in sql
        assert "WHERE" not in sql.split("DO UPDATE SET")[1]
