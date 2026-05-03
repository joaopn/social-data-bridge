"""Tests for the scheduler MCP submit-time validation and helpers in
``social_data_pipeline.jobs.mcp_tools``.

Covers behavior that runs *before* the runner gets the job: the
``validate_submission`` dispatcher and the Mongo CSV terminal-stage
pre-check. The runner-time validation paths are tested in
``test_backends_*.py``.
"""

from __future__ import annotations

import time

import pytest

from social_data_pipeline.jobs.backends import (
    BackendError,
    validate_submission,
)
from social_data_pipeline.jobs.mcp_tools import _csv_pipeline_warning
from social_data_pipeline.jobs.store import Job


def _pg_job(**overrides) -> Job:
    base = dict(
        job_id="pg_test",
        target="pg_main",
        backend="postgres",
        sql="SELECT 1",
        output_filename="out.parquet",
        overwrite=False,
        submitted_at=time.time(),
        status="pending",
    )
    base.update(overrides)
    return Job(**base)


def _sr_job(**overrides) -> Job:
    base = dict(
        job_id="sr_test",
        target="sr_main",
        backend="starrocks",
        sql="SELECT 1",
        output_filename="out.parquet",
        overwrite=False,
        submitted_at=time.time(),
        status="pending",
    )
    base.update(overrides)
    return Job(**base)


def _mongo_job(**overrides) -> Job:
    import json
    payload = json.dumps({
        "collection": "c",
        "database": "d",
        "pipeline": [{"$match": {}}],
    })
    base = dict(
        job_id="mg_test",
        target="mongo_main",
        backend="mongodb",
        sql=payload,
        output_filename="out.ndjson",
        overwrite=False,
        submitted_at=time.time(),
        status="pending",
        collection="c",
        database="d",
    )
    base.update(overrides)
    return Job(**base)


# ── validate_submission: postgres / starrocks ───────────────────────────────


class TestValidateSubmissionPostgres:
    def test_accepts_valid(self):
        validate_submission(_pg_job())

    def test_rejects_bad_extension(self):
        with pytest.raises(BackendError) as exc:
            validate_submission(_pg_job(output_filename="out.txt"))
        assert ".txt" in str(exc.value)

    def test_rejects_slash(self):
        with pytest.raises(BackendError) as exc:
            validate_submission(_pg_job(output_filename="sub/out.parquet"))
        assert "basename" in str(exc.value).lower()

    def test_rejects_empty_sql(self):
        with pytest.raises(BackendError):
            validate_submission(_pg_job(sql="   "))


class TestValidateSubmissionStarrocks:
    def test_accepts_valid(self):
        validate_submission(_sr_job())

    def test_rejects_bad_extension(self):
        with pytest.raises(BackendError):
            validate_submission(_sr_job(output_filename="out.json"))

    def test_rejects_empty_sql(self):
        with pytest.raises(BackendError):
            validate_submission(_sr_job(sql=";"))


# ── validate_submission: mongodb ────────────────────────────────────────────


class TestValidateSubmissionMongo:
    def test_accepts_ndjson(self):
        validate_submission(_mongo_job())

    def test_accepts_csv(self):
        validate_submission(_mongo_job(output_filename="out.csv"))

    def test_rejects_parquet(self):
        with pytest.raises(BackendError) as exc:
            validate_submission(_mongo_job(output_filename="out.parquet"))
        assert "extension" in str(exc.value).lower()

    def test_rejects_missing_database(self):
        j = _mongo_job(database="")
        with pytest.raises(BackendError) as exc:
            validate_submission(j)
        assert "database" in str(exc.value).lower()

    def test_rejects_missing_pipeline(self):
        # pipeline absent from the JSON payload
        j = _mongo_job(sql="{}")
        with pytest.raises(BackendError):
            validate_submission(j)


# ── validate_submission: unknown backend ────────────────────────────────────


class TestValidateSubmissionUnknown:
    def test_unknown_backend_raises(self):
        with pytest.raises(BackendError) as exc:
            validate_submission(_pg_job(backend="duckdb"))
        assert "duckdb" in str(exc.value)


# ── _csv_pipeline_warning ──────────────────────────────────────────────────


class TestCsvPipelineWarning:
    def test_ndjson_skipped(self):
        assert _csv_pipeline_warning("out.ndjson", [{"$limit": 5}]) is None

    def test_csv_with_project_at_end_ok(self):
        assert _csv_pipeline_warning(
            "out.csv",
            [{"$match": {}}, {"$project": {"a": 1}}],
        ) is None

    def test_csv_with_replaceRoot_ok(self):
        # $replaceRoot is permissive — runtime guard still catches non-flat output
        assert _csv_pipeline_warning(
            "out.csv",
            [{"$replaceRoot": {"newRoot": "$nested"}}],
        ) is None

    def test_csv_with_addFields_ok(self):
        # $addFields could flatten a nested field by overwriting it
        assert _csv_pipeline_warning(
            "out.csv",
            [{"$addFields": {"x": 1}}],
        ) is None

    def test_csv_with_bare_limit_rejected(self):
        msg = _csv_pipeline_warning("out.csv", [{"$limit": 5}])
        assert msg is not None
        assert "$limit" in msg
        assert "$project" in msg

    def test_csv_with_match_then_sort_rejected(self):
        msg = _csv_pipeline_warning(
            "out.csv",
            [{"$match": {}}, {"$sort": {"_id": 1}}],
        )
        assert msg is not None
        assert "$sort" in msg

    def test_csv_empty_pipeline_rejected(self):
        msg = _csv_pipeline_warning("out.csv", [])
        assert msg is not None
        assert "empty" in msg.lower()

    def test_csv_malformed_last_stage_rejected(self):
        msg = _csv_pipeline_warning("out.csv", [{}])
        assert msg is not None

    def test_csv_extension_case_insensitive(self):
        # Upper-case extension should still trigger the check
        assert _csv_pipeline_warning("OUT.CSV", [{"$limit": 1}]) is not None
