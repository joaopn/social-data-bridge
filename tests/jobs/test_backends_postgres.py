"""Tests for PostgresBackend.validate() — submission shape checks.

execute() / cancel() / explain() require a real DB connection and are covered
by E2E.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from social_data_pipeline.jobs.backends.base import BackendError
from social_data_pipeline.jobs.backends.postgres import PostgresBackend
from social_data_pipeline.jobs.store import Job


def _backend(tmp_path) -> PostgresBackend:
    return PostgresBackend(result_root=tmp_path, database="datasets")


def _job(**overrides) -> Job:
    base = dict(
        job_id="pg_test",
        target="warehouse",
        backend="postgres",
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
            _backend(tmp_path).validate(_job(output_filename="out.json"))

    def test_rejects_path_traversal(self, tmp_path):
        with pytest.raises(BackendError):
            _backend(tmp_path).validate(_job(output_filename="../out.parquet"))

    def test_rejects_empty_sql(self, tmp_path):
        with pytest.raises(BackendError, match="empty"):
            _backend(tmp_path).validate(_job(sql=";;;"))


class TestInit:
    def test_default_port_from_env(self, monkeypatch, tmp_path):
        monkeypatch.setenv("POSTGRES_PORT", "5433")
        b = PostgresBackend(result_root=tmp_path, database="d")
        assert b.port == 5433

    def test_default_port_fallback(self, monkeypatch, tmp_path):
        monkeypatch.delenv("POSTGRES_PORT", raising=False)
        b = PostgresBackend(result_root=tmp_path, database="d")
        assert b.port == 5432

    def test_explicit_port_wins(self, monkeypatch, tmp_path):
        monkeypatch.setenv("POSTGRES_PORT", "5433")
        b = PostgresBackend(result_root=tmp_path, database="d", port=6000)
        assert b.port == 6000
