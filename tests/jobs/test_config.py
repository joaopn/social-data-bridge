"""Tests for jobs config loading and helpers."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from social_data_pipeline.jobs.config import (
    JobsConfig,
    Target,
    admin_password,
    auth_enabled,
    load_config,
)


# ── JobsConfig methods ──────────────────────────────────────────────────────


def _cfg(**overrides):
    base = dict(
        port=8050,
        jobs_dir=Path("/data/jobs"),
        result_root=Path("/data/jobs/results"),
        host_result_root="/host/jobs/results",
        max_concurrent=1,
        default_timeouts={"postgres": 0, "starrocks": 259200, "mongodb": 0},
        history_retention=500,
        auth_enabled=False,
        targets={
            "warehouse": Target(name="warehouse", backend="postgres", database="datasets"),
            "olap": Target(name="olap", backend="starrocks", database=""),
        },
    )
    base.update(overrides)
    return JobsConfig(**base)


class TestJobsConfigMethods:
    def test_targets_for_filters_by_backend(self):
        cfg = _cfg()
        pg = cfg.targets_for("postgres")
        assert [t.name for t in pg] == ["warehouse"]

    def test_targets_for_returns_empty_for_missing_backend(self):
        cfg = _cfg()
        assert cfg.targets_for("mongodb") == []

    def test_has_backend(self):
        cfg = _cfg()
        assert cfg.has_backend("postgres") is True
        assert cfg.has_backend("starrocks") is True
        assert cfg.has_backend("mongodb") is False

    def test_timeout_for_returns_configured_value(self):
        cfg = _cfg()
        assert cfg.timeout_for("postgres") == 0
        assert cfg.timeout_for("starrocks") == 259200

    def test_timeout_for_unknown_backend_zero(self):
        cfg = _cfg()
        # Out-of-band backend names return 0 ("no limit" sentinel).
        assert cfg.timeout_for("kafka") == 0


# ── load_config ─────────────────────────────────────────────────────────────


def _write_yaml(path: Path, data: dict) -> Path:
    path.write_text(yaml.dump(data))
    return path


class TestLoadConfig:
    def test_minimal_postgres_target(self, tmp_path, monkeypatch):
        monkeypatch.setenv("JOBS_DIR", str(tmp_path / "jobs"))
        monkeypatch.delenv("JOBS_RESULT_ROOT", raising=False)
        cfg_path = _write_yaml(
            tmp_path / "config.yaml",
            {
                "port": 9000,
                "max_concurrent": 4,
                "history_retention": 100,
                "targets": {
                    "wh": {"backend": "postgres", "database": "datasets"},
                },
            },
        )
        cfg = load_config(cfg_path)
        assert cfg.port == 9000
        assert cfg.max_concurrent == 4
        assert cfg.history_retention == 100
        assert "wh" in cfg.targets
        assert cfg.targets["wh"].backend == "postgres"
        assert cfg.targets["wh"].database == "datasets"

    def test_local_overlay_merged_on_top(self, tmp_path, monkeypatch):
        """`config.local.yaml` overrides `config.yaml` (mirrors the
        `*.conf` + `*.local.conf` pattern used elsewhere)."""
        monkeypatch.setenv("JOBS_DIR", str(tmp_path / "jobs"))
        cfg_path = _write_yaml(
            tmp_path / "config.yaml",
            {
                "port": 8050,
                "targets": {"wh": {"backend": "postgres", "database": "datasets"}},
            },
        )
        _write_yaml(
            tmp_path / "config.local.yaml",
            {"port": 9000, "max_concurrent": 8},
        )
        cfg = load_config(cfg_path)
        assert cfg.port == 9000
        assert cfg.max_concurrent == 8
        # Targets from base file survive when the local doesn't mention them.
        assert "wh" in cfg.targets

    def test_postgres_target_requires_database(self, tmp_path, monkeypatch):
        monkeypatch.setenv("JOBS_DIR", str(tmp_path / "jobs"))
        cfg_path = _write_yaml(
            tmp_path / "config.yaml",
            {"targets": {"wh": {"backend": "postgres", "database": ""}}},
        )
        with pytest.raises(ValueError, match="postgres targets require"):
            load_config(cfg_path)

    def test_starrocks_target_allows_empty_database(self, tmp_path, monkeypatch):
        # SR targets can omit database — the agent fully-qualifies refs.
        monkeypatch.setenv("JOBS_DIR", str(tmp_path / "jobs"))
        cfg_path = _write_yaml(
            tmp_path / "config.yaml",
            {"targets": {"olap": {"backend": "starrocks", "database": ""}}},
        )
        cfg = load_config(cfg_path)
        assert cfg.targets["olap"].database == ""

    def test_mongodb_target_allows_empty_database(self, tmp_path, monkeypatch):
        monkeypatch.setenv("JOBS_DIR", str(tmp_path / "jobs"))
        cfg_path = _write_yaml(
            tmp_path / "config.yaml",
            {"targets": {"docstore": {"backend": "mongodb", "database": ""}}},
        )
        cfg = load_config(cfg_path)
        assert cfg.targets["docstore"].backend == "mongodb"

    def test_unknown_backend_rejected(self, tmp_path, monkeypatch):
        monkeypatch.setenv("JOBS_DIR", str(tmp_path / "jobs"))
        cfg_path = _write_yaml(
            tmp_path / "config.yaml",
            {"targets": {"x": {"backend": "redis", "database": "x"}}},
        )
        with pytest.raises(ValueError, match="backend must be"):
            load_config(cfg_path)

    def test_no_targets_rejected(self, tmp_path, monkeypatch):
        monkeypatch.setenv("JOBS_DIR", str(tmp_path / "jobs"))
        cfg_path = _write_yaml(tmp_path / "config.yaml", {"port": 8050})
        with pytest.raises(ValueError, match="no targets"):
            load_config(cfg_path)

    def test_missing_files_raise(self, tmp_path, monkeypatch):
        monkeypatch.setenv("JOBS_DIR", str(tmp_path / "jobs"))
        with pytest.raises(FileNotFoundError):
            load_config(tmp_path / "missing.yaml")


class TestDefaultTimeouts:
    def test_explicit_per_backend_timeouts(self, tmp_path, monkeypatch):
        monkeypatch.setenv("JOBS_DIR", str(tmp_path / "jobs"))
        cfg_path = _write_yaml(
            tmp_path / "config.yaml",
            {
                "default_timeouts": {"postgres": 60, "starrocks": 600, "mongodb": 30},
                "targets": {"wh": {"backend": "postgres", "database": "d"}},
            },
        )
        cfg = load_config(cfg_path)
        assert cfg.timeout_for("postgres") == 60
        assert cfg.timeout_for("starrocks") == 600
        assert cfg.timeout_for("mongodb") == 30

    def test_legacy_default_timeout_seconds_applied(self, tmp_path, monkeypatch):
        """Older configs only had a single `default_timeout_seconds`. It must
        be applied across all backends, with SR capped at its 72h max."""
        monkeypatch.setenv("JOBS_DIR", str(tmp_path / "jobs"))
        cfg_path = _write_yaml(
            tmp_path / "config.yaml",
            {
                "default_timeout_seconds": 120,
                "targets": {"wh": {"backend": "postgres", "database": "d"}},
            },
        )
        cfg = load_config(cfg_path)
        assert cfg.timeout_for("postgres") == 120
        assert cfg.timeout_for("mongodb") == 120
        assert cfg.timeout_for("starrocks") == 120

    def test_legacy_zero_means_unlimited_caps_sr_at_72h(self, tmp_path, monkeypatch):
        """SR doesn't accept 0 (= unlimited). When the legacy single-value
        config requests unlimited, SR must be clamped to its 72h ceiling."""
        monkeypatch.setenv("JOBS_DIR", str(tmp_path / "jobs"))
        cfg_path = _write_yaml(
            tmp_path / "config.yaml",
            {
                "default_timeout_seconds": 0,
                "targets": {"wh": {"backend": "postgres", "database": "d"}},
            },
        )
        cfg = load_config(cfg_path)
        assert cfg.timeout_for("postgres") == 0
        assert cfg.timeout_for("mongodb") == 0
        assert cfg.timeout_for("starrocks") == 259200

    def test_safe_defaults_when_unset(self, tmp_path, monkeypatch):
        monkeypatch.setenv("JOBS_DIR", str(tmp_path / "jobs"))
        cfg_path = _write_yaml(
            tmp_path / "config.yaml",
            {"targets": {"wh": {"backend": "postgres", "database": "d"}}},
        )
        cfg = load_config(cfg_path)
        # PG/Mongo unlimited (0), SR clamped to 72h.
        assert cfg.timeout_for("postgres") == 0
        assert cfg.timeout_for("mongodb") == 0
        assert cfg.timeout_for("starrocks") == 259200


# ── env-driven helpers ──────────────────────────────────────────────────────


class TestAuthEnabled:
    @pytest.mark.parametrize("backend,env_var", [
        ("postgres", "POSTGRES_AUTH_ENABLED"),
        ("starrocks", "STARROCKS_AUTH_ENABLED"),
        ("mongodb", "MONGO_AUTH_ENABLED"),
    ])
    def test_truthy_values(self, monkeypatch, backend, env_var):
        for v in ("1", "true", "yes", "TRUE", "Yes"):
            monkeypatch.setenv(env_var, v)
            assert auth_enabled(backend) is True

    @pytest.mark.parametrize("backend,env_var", [
        ("postgres", "POSTGRES_AUTH_ENABLED"),
        ("starrocks", "STARROCKS_AUTH_ENABLED"),
        ("mongodb", "MONGO_AUTH_ENABLED"),
    ])
    def test_falsey_or_unset(self, monkeypatch, backend, env_var):
        # Unset
        monkeypatch.delenv(env_var, raising=False)
        assert auth_enabled(backend) is False
        # Explicit falsy values
        for v in ("", "0", "false", "no"):
            monkeypatch.setenv(env_var, v)
            assert auth_enabled(backend) is False


class TestAdminPassword:
    @pytest.mark.parametrize("backend,env_var", [
        ("postgres", "POSTGRES_PASSWORD"),
        ("starrocks", "STARROCKS_ROOT_PASSWORD"),
        ("mongodb", "MONGO_ADMIN_PASSWORD"),
    ])
    def test_returns_password(self, monkeypatch, backend, env_var):
        monkeypatch.setenv(env_var, "s3cr3t")
        assert admin_password(backend) == "s3cr3t"

    @pytest.mark.parametrize("backend,env_var", [
        ("postgres", "POSTGRES_PASSWORD"),
        ("starrocks", "STARROCKS_ROOT_PASSWORD"),
        ("mongodb", "MONGO_ADMIN_PASSWORD"),
    ])
    def test_unset_returns_none(self, monkeypatch, backend, env_var):
        monkeypatch.delenv(env_var, raising=False)
        assert admin_password(backend) is None

    @pytest.mark.parametrize("backend,env_var", [
        ("postgres", "POSTGRES_PASSWORD"),
        ("starrocks", "STARROCKS_ROOT_PASSWORD"),
        ("mongodb", "MONGO_ADMIN_PASSWORD"),
    ])
    def test_empty_string_returns_none(self, monkeypatch, backend, env_var):
        monkeypatch.setenv(env_var, "")
        assert admin_password(backend) is None
