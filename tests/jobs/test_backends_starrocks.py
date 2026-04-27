"""Tests for StarrocksBackend.validate() and the SR-specific helper that
parses INTO OUTFILE summary tuples.
"""

from __future__ import annotations

import pytest

from social_data_pipeline.jobs.backends.base import BackendError
from social_data_pipeline.jobs.backends.starrocks import (
    StarrocksBackend,
    _rows_from_sr_outfile_summary,
)
from social_data_pipeline.jobs.store import Job


def _backend(tmp_path) -> StarrocksBackend:
    return StarrocksBackend(result_root=tmp_path, database="datasets")


def _job(**overrides) -> Job:
    base = dict(
        job_id="sr_test",
        target="olap",
        backend="starrocks",
        sql="SELECT 1",
        output_filename="out.parquet",
        overwrite=False,
        submitted_at=0.0,
    )
    base.update(overrides)
    return Job(**base)


class TestValidate:
    def test_valid_parquet(self, tmp_path):
        _backend(tmp_path).validate(_job())

    def test_valid_csv(self, tmp_path):
        _backend(tmp_path).validate(_job(output_filename="out.csv"))

    def test_rejects_unknown_extension(self, tmp_path):
        with pytest.raises(BackendError):
            _backend(tmp_path).validate(_job(output_filename="out.ndjson"))

    def test_rejects_empty_sql(self, tmp_path):
        with pytest.raises(BackendError):
            _backend(tmp_path).validate(_job(sql="   "))


class TestInit:
    def test_default_port_from_env(self, monkeypatch, tmp_path):
        monkeypatch.setenv("STARROCKS_PORT", "9931")
        b = StarrocksBackend(result_root=tmp_path, database="d")
        assert b.port == 9931

    def test_default_port_fallback(self, monkeypatch, tmp_path):
        monkeypatch.delenv("STARROCKS_PORT", raising=False)
        b = StarrocksBackend(result_root=tmp_path, database="d")
        assert b.port == 9030

    def test_explicit_port_wins(self, monkeypatch, tmp_path):
        monkeypatch.setenv("STARROCKS_PORT", "9931")
        b = StarrocksBackend(result_root=tmp_path, database="d", port=12345)
        assert b.port == 12345

    def test_max_file_size_default(self, tmp_path):
        b = StarrocksBackend(result_root=tmp_path, database="d")
        assert b.max_file_size == 1073741824  # 1 GiB

    def test_max_file_size_override(self, tmp_path):
        b = StarrocksBackend(result_root=tmp_path, database="d", max_file_size=42)
        assert b.max_file_size == 42


class TestRowsFromSrOutfileSummary:
    """SR's INTO OUTFILE returns (FileNumber, TotalRows, FileSize, URL).
    The helper extracts TotalRows; unexpected shapes return None rather than
    raising — defensive because SR has changed this format historically."""

    def test_typical_summary_returns_total_rows(self):
        assert _rows_from_sr_outfile_summary((3, 1000, 12345, "file:///x")) == 1000

    def test_short_tuple_returns_none(self):
        assert _rows_from_sr_outfile_summary((1,)) is None

    def test_non_int_total_returns_none(self):
        assert _rows_from_sr_outfile_summary((1, "not-a-number", 100, "url")) is None

    def test_empty_returns_none(self):
        assert _rows_from_sr_outfile_summary(()) is None
        assert _rows_from_sr_outfile_summary(None) is None
