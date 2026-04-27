"""Tests for MongoBackend payload helpers, validation, and CSV/NDJSON writers.

execute() / cancel() / explain() / list_databases() require a live MongoDB
and are deferred to E2E.
"""

from __future__ import annotations

import datetime
import json

import pytest

from social_data_pipeline.jobs.backends.base import BackendError
from social_data_pipeline.jobs.backends.mongo import (
    MongoBackend,
    _collection_from_job,
    _json_default,
    _pipeline_from_job,
    _stringify_scalar,
    _validate_flat_scalars,
    _write_csv,
    _write_ndjson,
)
from social_data_pipeline.jobs.store import Job


def _job(sql_payload: dict | str | None = None, **overrides) -> Job:
    if isinstance(sql_payload, dict):
        sql = json.dumps(sql_payload)
    elif sql_payload is None:
        sql = json.dumps({"collection": "events", "pipeline": [{"$match": {}}]})
    else:
        sql = sql_payload
    base = dict(
        job_id="mg_test",
        target="docstore",
        backend="mongodb",
        sql=sql,
        output_filename="out.ndjson",
        overwrite=False,
        submitted_at=0.0,
        collection="events",
        database="reddit",
    )
    base.update(overrides)
    return Job(**base)


# ── _pipeline_from_job ──────────────────────────────────────────────────────


class TestPipelineFromJob:
    def test_extracts_pipeline(self):
        pipe = [{"$match": {"x": 1}}, {"$limit": 10}]
        j = _job(sql_payload={"collection": "events", "pipeline": pipe})
        assert _pipeline_from_job(j) == pipe

    def test_missing_pipeline_raises(self):
        j = _job(sql_payload={"collection": "events"})
        with pytest.raises(BackendError, match="pipeline"):
            _pipeline_from_job(j)

    def test_pipeline_not_list_raises(self):
        j = _job(sql_payload={"collection": "events", "pipeline": "not a list"})
        with pytest.raises(BackendError, match="pipeline"):
            _pipeline_from_job(j)

    def test_invalid_json_raises(self):
        j = _job(sql_payload="{not json")
        with pytest.raises(BackendError, match="not valid JSON"):
            _pipeline_from_job(j)


# ── _collection_from_job ────────────────────────────────────────────────────


class TestCollectionFromJob:
    def test_record_field_wins(self):
        # Record collection field overrides whatever's in the payload.
        j = _job(
            sql_payload={"collection": "ignored", "pipeline": []},
            collection="authoritative",
        )
        assert _collection_from_job(j) == "authoritative"

    def test_falls_back_to_payload(self):
        j = _job(
            sql_payload={"collection": "from_payload", "pipeline": []},
            collection=None,
        )
        assert _collection_from_job(j) == "from_payload"

    def test_missing_everywhere_raises(self):
        j = _job(sql_payload={"pipeline": []}, collection=None)
        with pytest.raises(BackendError, match="collection"):
            _collection_from_job(j)


# ── MongoBackend.validate ───────────────────────────────────────────────────


def _backend(tmp_path) -> MongoBackend:
    return MongoBackend(result_root=tmp_path)


class TestValidate:
    def test_valid_ndjson(self, tmp_path):
        _backend(tmp_path).validate(_job())

    def test_valid_csv(self, tmp_path):
        _backend(tmp_path).validate(_job(output_filename="out.csv"))

    def test_rejects_parquet(self, tmp_path):
        # Mongo can only write ndjson or csv (writers are pure-Python).
        with pytest.raises(BackendError):
            _backend(tmp_path).validate(_job(output_filename="out.parquet"))

    def test_rejects_missing_database(self, tmp_path):
        with pytest.raises(BackendError, match="database"):
            _backend(tmp_path).validate(_job(database=""))

    def test_rejects_invalid_payload(self, tmp_path):
        with pytest.raises(BackendError, match="not valid JSON"):
            _backend(tmp_path).validate(_job(sql_payload="{bad"))


# ── writers ─────────────────────────────────────────────────────────────────


class TestWriteNdjson:
    def test_writes_one_doc_per_line(self, tmp_path):
        out = tmp_path / "out.ndjson"
        cursor = iter([{"id": 1, "name": "a"}, {"id": 2, "name": "b"}])
        rows = _write_ndjson(cursor, out)
        assert rows == 2
        lines = out.read_text().splitlines()
        assert json.loads(lines[0]) == {"id": 1, "name": "a"}
        assert json.loads(lines[1]) == {"id": 2, "name": "b"}

    def test_handles_datetime(self, tmp_path):
        out = tmp_path / "out.ndjson"
        cursor = iter([{"when": datetime.datetime(2024, 1, 1, 12, 0, 0)}])
        _write_ndjson(cursor, out)
        # datetime serialized via _json_default
        line = out.read_text().strip()
        assert "2024-01-01T12:00:00" in line


class TestWriteCsv:
    def test_writes_header_then_rows(self, tmp_path):
        out = tmp_path / "out.csv"
        cursor = iter([{"id": 1, "name": "a"}, {"id": 2, "name": "b"}])
        rows = _write_csv(cursor, out)
        assert rows == 2
        text = out.read_text()
        # Header is sorted by key
        assert text.startswith("id,name\n")
        assert "1,a" in text
        assert "2,b" in text

    def test_empty_cursor_creates_empty_file(self, tmp_path):
        out = tmp_path / "out.csv"
        rows = _write_csv(iter([]), out)
        assert rows == 0
        assert out.exists()
        assert out.read_text() == ""

    def test_nested_value_rejected(self, tmp_path):
        out = tmp_path / "out.csv"
        cursor = iter([{"id": 1, "obj": {"nested": "v"}}])
        with pytest.raises(BackendError, match="flat scalars"):
            _write_csv(cursor, out)

    def test_heterogeneous_schema_rejected(self, tmp_path):
        out = tmp_path / "out.csv"
        # First doc fixes columns; second doc adds a key.
        cursor = iter([{"id": 1, "name": "a"}, {"id": 2, "name": "b", "extra": "x"}])
        with pytest.raises(BackendError, match="introduces new keys"):
            _write_csv(cursor, out)


class TestValidateFlatScalars:
    def test_passes_for_scalars(self):
        _validate_flat_scalars({"a": 1, "b": "s", "c": None, "d": 3.14, "e": True}, ["a", "b", "c", "d", "e"])

    @pytest.mark.parametrize("bad", [{"x": [1, 2]}, {"x": {"k": 1}}, {"x": (1, 2)}, {"x": {1, 2}}])
    def test_fails_for_containers(self, bad):
        with pytest.raises(BackendError, match="flat scalars"):
            _validate_flat_scalars(bad, list(bad.keys()))


class TestStringifyScalar:
    @pytest.mark.parametrize("v", [None, "s", 1, 3.14, True])
    def test_passthrough_primitives(self, v):
        assert _stringify_scalar(v) == v

    def test_datetime_to_iso(self):
        v = datetime.datetime(2024, 6, 1, 12, 0, 0)
        assert _stringify_scalar(v) == "2024-06-01T12:00:00"

    def test_other_types_to_str(self):
        class Obj:
            def __str__(self):
                return "obj-repr"
        assert _stringify_scalar(Obj()) == "obj-repr"


class TestJsonDefault:
    def test_datetime(self):
        v = datetime.datetime(2024, 6, 1, 12, 0, 0)
        assert _json_default(v) == "2024-06-01T12:00:00"

    def test_other_types_str(self):
        class Obj:
            def __str__(self):
                return "x"
        assert _json_default(Obj()) == "x"
