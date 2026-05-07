"""Tests for the drift-detection helpers in social_data_pipeline.setup.verify
and the `db verify` CLI wiring.

The verify module is the source of truth for "is the DB install internally
consistent?". Two callers consume it: ``sdp db verify`` (exit non-zero on
drift, JSON or text) and the drift section of ``sdp db status`` (always
exit 0, advisory). These tests cover the pure ``compute_drift`` function
matrix-style — yaml ↔ env ↔ creds combinations — plus the CLI wrapper's
exit-code, ``--json`` shape, and ``--db`` filter contract.

Container-state coherence is tested with mocked ``container_states``
inputs; the live ``docker inspect`` probe (``_probe_container_state``) is
covered separately by the E2E flow because mocking docker subprocess
plumbing in unit tests trades clarity for almost no extra coverage.
"""

from __future__ import annotations

import json

import pytest

import sdp
from social_data_pipeline.setup.verify import (
    Finding,
    compute_drift,
    is_clean,
)


# ---------------------------------------------------------------------------
# Helpers — build the verify ctx with sensible defaults so tests focus on
# whatever they're actually exercising.
# ---------------------------------------------------------------------------


def _ok_cred_state(db, exists=True, mode=0o600, host_owned=True, readable=True):
    return {
        "path": f"/data/database/{db}/.ro_credentials",
        "exists": exists,
        "mode": mode if exists else None,
        "host_owned": host_owned if exists else None,
        "readable": readable if exists else False,
    }


def _ctx(**overrides):
    base = {
        "env": {},
        "configured_dbs": [],
        "db_yamls": {},
        "cred_file_states": {},
        "sources_info": [],
        "override_data": {},
        "mcp_config": {},
        "jobs_config": {},
        "container_states": {},
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# Auth + creds findings — the matrix of yaml ↔ env ↔ cred-file states.
# ---------------------------------------------------------------------------


class TestAuthCoherence:
    def test_clean_no_auth_anywhere(self):
        ctx = _ctx(
            configured_dbs=["postgres"],
            db_yamls={"postgres": {"port": 5432, "name": "datasets"}},  # no auth key
            env={},
            cred_file_states={"postgres": _ok_cred_state("postgres", exists=False)},
        )
        out = compute_drift(ctx)
        assert out == {"postgres": []}

    def test_clean_full_auth(self):
        ctx = _ctx(
            configured_dbs=["postgres"],
            db_yamls={"postgres": {"auth": True}},
            env={"POSTGRES_AUTH_ENABLED": "true"},
            cred_file_states={"postgres": _ok_cred_state("postgres")},
        )
        assert compute_drift(ctx) == {"postgres": []}

    def test_yaml_auth_but_env_missing(self):
        # config/db/postgres.yaml says auth:true but .env doesn't agree.
        # Classic drift surfaced by the unsetup/setup cycle.
        ctx = _ctx(
            configured_dbs=["postgres"],
            db_yamls={"postgres": {"auth": True}},
            env={},
            cred_file_states={"postgres": _ok_cred_state("postgres")},
        )
        findings = compute_drift(ctx)["postgres"]
        cats = [f.category for f in findings]
        assert "auth" in cats
        assert any("POSTGRES_AUTH_ENABLED" in f.message for f in findings)

    def test_env_auth_but_yaml_disagrees(self):
        # Mirror failure: .env enables auth, yaml doesn't.
        ctx = _ctx(
            configured_dbs=["mongo"],
            db_yamls={"mongo": {"auth": False}},
            env={"MONGO_AUTH_ENABLED": "true"},
            cred_file_states={"mongo": _ok_cred_state("mongo")},
        )
        findings = compute_drift(ctx)["mongo"]
        assert any(f.category == "auth" for f in findings)
        assert any("auth:false" in f.message or "unset" in f.message for f in findings)

    def test_auth_on_creds_missing(self):
        ctx = _ctx(
            configured_dbs=["postgres"],
            db_yamls={"postgres": {"auth": True}},
            env={"POSTGRES_AUTH_ENABLED": "true"},
            cred_file_states={"postgres": _ok_cred_state("postgres", exists=False)},
        )
        findings = compute_drift(ctx)["postgres"]
        assert any(f.category == "creds" and "missing" in f.message for f in findings)
        assert any("recover-password --regenerate-ro" in f.fix for f in findings)

    def test_auth_on_creds_not_host_owned(self):
        ctx = _ctx(
            configured_dbs=["starrocks"],
            db_yamls={"starrocks": {"auth": True}},
            env={"STARROCKS_AUTH_ENABLED": "true"},
            cred_file_states={"starrocks": _ok_cred_state("starrocks", host_owned=False)},
        )
        findings = compute_drift(ctx)["starrocks"]
        assert any(f.category == "creds" and "not host-owned" in f.message for f in findings)

    def test_auth_on_creds_wrong_mode(self):
        ctx = _ctx(
            configured_dbs=["postgres"],
            db_yamls={"postgres": {"auth": True}},
            env={"POSTGRES_AUTH_ENABLED": "true"},
            cred_file_states={"postgres": _ok_cred_state("postgres", mode=0o644)},
        )
        findings = compute_drift(ctx)["postgres"]
        assert any(
            f.category == "creds" and "0o644" in f.message and "0600" in f.message
            for f in findings
        )

    def test_auth_on_creds_unreadable(self):
        ctx = _ctx(
            configured_dbs=["mongo"],
            db_yamls={"mongo": {"auth": True}},
            env={"MONGO_AUTH_ENABLED": "true"},
            cred_file_states={"mongo": _ok_cred_state("mongo", readable=False)},
        )
        findings = compute_drift(ctx)["mongo"]
        assert any(f.category == "creds" and "cannot be read" in f.message for f in findings)


# ---------------------------------------------------------------------------
# Mount-coherence findings — wired through compute_mount_drift.
# ---------------------------------------------------------------------------


class TestMountCoherence:
    def test_mongo_never_reports_mounts(self):
        # Mongo has no server-side mounts (mongoimport reads in the ingest
        # container), so mount-drift checks must be a no-op for it.
        ctx = _ctx(
            configured_dbs=["mongo"],
            db_yamls={"mongo": {"auth": False}},
            env={},
            cred_file_states={"mongo": _ok_cred_state("mongo", exists=False)},
            sources_info=[{
                "name": "reddit",
                "profiles": ["mongo_ingest"],
                "paths": {"parsed": "/host/parsed/reddit"},
            }],
            override_data={},
        )
        findings = compute_drift(ctx)["mongo"]
        assert all(f.category != "mounts" for f in findings)

    def test_postgres_missing_source_mount(self):
        ctx = _ctx(
            configured_dbs=["postgres"],
            db_yamls={"postgres": {}},
            env={},
            cred_file_states={"postgres": _ok_cred_state("postgres", exists=False)},
            sources_info=[{
                "name": "reddit",
                "profiles": ["postgres_ingest"],
                "paths": {
                    "parsed": "/host/parsed/reddit",
                    "output": "/host/output/reddit",
                },
            }],
            override_data={},  # nothing in override → drift
        )
        mount_findings = [
            f for f in compute_drift(ctx)["postgres"] if f.category == "mounts"
        ]
        # Two missing mounts (parsed + output), each with the recovery hint.
        assert len(mount_findings) == 2
        assert all("db stop postgres" in f.fix for f in mount_findings)


# ---------------------------------------------------------------------------
# Container-state findings — covered with mocked `container_states`.
# ---------------------------------------------------------------------------


class TestContainerStateCoherence:
    def test_no_probe_means_no_findings(self):
        # container_states defaulting to {} (verify ctx from db status)
        # must not produce any container-category findings.
        ctx = _ctx(
            configured_dbs=["postgres"],
            db_yamls={"postgres": {"auth": True}},
            env={"POSTGRES_AUTH_ENABLED": "true"},
            cred_file_states={"postgres": _ok_cred_state("postgres")},
            container_states={},
        )
        findings = compute_drift(ctx)["postgres"]
        assert all(f.category != "container" for f in findings)

    def test_container_env_diverges(self):
        ctx = _ctx(
            configured_dbs=["postgres"],
            db_yamls={"postgres": {"auth": True}},
            env={"POSTGRES_AUTH_ENABLED": "true"},
            cred_file_states={"postgres": _ok_cred_state("postgres")},
            container_states={"postgres": {
                "running": True, "healthy": True, "env_auth": False,
            }},
        )
        findings = compute_drift(ctx)["postgres"]
        assert any(
            f.category == "container" and "started before .env changed" in f.message
            for f in findings
        )

    def test_unhealthy_under_auth_flagged(self):
        ctx = _ctx(
            configured_dbs=["mongo"],
            db_yamls={"mongo": {"auth": True}},
            env={"MONGO_AUTH_ENABLED": "true"},
            cred_file_states={"mongo": _ok_cred_state("mongo")},
            container_states={"mongo": {
                "running": True, "healthy": False, "env_auth": True,
            }},
        )
        findings = compute_drift(ctx)["mongo"]
        assert any(
            f.category == "container" and "unhealthy" in f.message for f in findings
        )

    def test_not_running_skips_container_checks(self):
        ctx = _ctx(
            configured_dbs=["postgres"],
            db_yamls={"postgres": {"auth": True}},
            env={"POSTGRES_AUTH_ENABLED": "true"},
            cred_file_states={"postgres": _ok_cred_state("postgres")},
            container_states={"postgres": {"running": False}},
        )
        findings = compute_drift(ctx)["postgres"]
        assert all(f.category != "container" for f in findings)


# ---------------------------------------------------------------------------
# Cross-cutting MCP / jobs findings.
# ---------------------------------------------------------------------------


class TestMcpCoherence:
    def test_mcp_enabled_for_unconfigured_db(self):
        # mcp.yaml has postgres enabled but config/db/postgres.yaml doesn't
        # exist. Common after `db unsetup --db postgres` skipped MCP cleanup.
        ctx = _ctx(
            configured_dbs=["mongo"],
            mcp_config={
                "postgres": {"enabled": True},
                "mongo": {"enabled": False},
            },
            db_yamls={"mongo": {}},
            cred_file_states={"mongo": _ok_cred_state("mongo", exists=False)},
        )
        out = compute_drift(ctx)
        assert "mcp" in out
        assert any(
            f.category == "mcp" and "postgres_mcp" in f.message and "not configured" in f.message
            for f in out["mcp"]
        )

    def test_mcp_enabled_but_creds_missing(self):
        ctx = _ctx(
            configured_dbs=["postgres"],
            mcp_config={"postgres": {"enabled": True}},
            db_yamls={"postgres": {"auth": True}},
            env={"POSTGRES_AUTH_ENABLED": "true"},
            cred_file_states={"postgres": _ok_cred_state("postgres", exists=False)},
        )
        out = compute_drift(ctx)
        # MCP finding piggy-backs on the missing creds — both paths must surface.
        assert "mcp" in out
        assert any(
            f.category == "mcp" and ".ro_credentials is missing" in f.message
            for f in out["mcp"]
        )

    def test_mcp_clean_when_disabled(self):
        ctx = _ctx(
            configured_dbs=["postgres"],
            mcp_config={"postgres": {"enabled": False}},
            db_yamls={"postgres": {}},
            cred_file_states={"postgres": _ok_cred_state("postgres", exists=False)},
        )
        # No mcp findings when no MCP is enabled.
        assert "mcp" not in compute_drift(ctx)


class TestJobsCoherence:
    def test_target_referencing_unconfigured_db(self):
        ctx = _ctx(
            configured_dbs=["postgres"],
            db_yamls={"postgres": {}},
            cred_file_states={"postgres": _ok_cred_state("postgres", exists=False)},
            jobs_config={
                "targets": {
                    "sr_main": {"backend": "starrocks", "database": ""},
                },
            },
        )
        out = compute_drift(ctx)
        assert "jobs" in out
        assert any(
            f.category == "jobs" and "starrocks" in f.message and "not configured" in f.message
            for f in out["jobs"]
        )

    def test_target_with_unknown_backend(self):
        ctx = _ctx(
            configured_dbs=["postgres"],
            db_yamls={"postgres": {}},
            cred_file_states={"postgres": _ok_cred_state("postgres", exists=False)},
            jobs_config={
                "targets": {
                    "weird": {"backend": "redis", "database": ""},
                },
            },
        )
        out = compute_drift(ctx)
        assert "jobs" in out
        assert any(f.category == "jobs" and "unknown backend" in f.message for f in out["jobs"])

    def test_jobs_auth_without_db_auth(self):
        # jobs config has auth: true but no DB has auth enabled — the UI
        # would prompt for an admin password that no backend uses.
        ctx = _ctx(
            configured_dbs=["postgres"],
            db_yamls={"postgres": {}},
            env={},
            cred_file_states={"postgres": _ok_cred_state("postgres", exists=False)},
            jobs_config={"auth": True, "targets": {}},
        )
        out = compute_drift(ctx)
        assert "jobs" in out
        assert any(
            f.category == "jobs" and "no configured DB has auth enabled" in f.message
            for f in out["jobs"]
        )

    def test_jobs_auth_paired_with_db_auth_is_clean(self):
        ctx = _ctx(
            configured_dbs=["postgres"],
            db_yamls={"postgres": {"auth": True}},
            env={"POSTGRES_AUTH_ENABLED": "true"},
            cred_file_states={"postgres": _ok_cred_state("postgres")},
            jobs_config={
                "auth": True,
                "targets": {"pg_main": {"backend": "postgres", "database": "datasets"}},
            },
        )
        # The DB block has zero findings; jobs section has none either.
        out = compute_drift(ctx)
        assert out["postgres"] == []
        assert "jobs" not in out


# ---------------------------------------------------------------------------
# is_clean helper.
# ---------------------------------------------------------------------------


class TestIsClean:
    def test_empty_findings_are_clean(self):
        assert is_clean({}) is True
        assert is_clean({"postgres": [], "mongo": []}) is True

    def test_any_finding_dirty(self):
        f = Finding(category="auth", message="m", fix="x")
        assert is_clean({"postgres": [f]}) is False
        assert is_clean({"postgres": [], "mongo": [f]}) is False


# ---------------------------------------------------------------------------
# CLI wiring — cmd_db_verify exit code, --json shape, --db filter.
# ---------------------------------------------------------------------------


class TestCmdDbVerify:
    def _patch(self, monkeypatch, *, configured, ctx_drift):
        """Patch build_verify_context + compute_drift to feed the CLI."""
        monkeypatch.setattr(sdp, "_get_configured_db_services", lambda: configured)
        monkeypatch.setattr(sdp, "_build_verify_context", lambda **_: {"_marker": True})
        monkeypatch.setattr(
            "social_data_pipeline.setup.verify.compute_drift",
            lambda ctx: ctx_drift,
        )

    def test_exits_zero_when_clean(self, monkeypatch, capsys):
        self._patch(
            monkeypatch,
            configured=["postgres"],
            ctx_drift={"postgres": []},
        )
        rc = sdp.cmd_db_verify(type("Args", (), {"db": None, "json": False})())
        assert rc == 0
        out = capsys.readouterr().out
        assert "OK" in out

    def test_exits_one_on_drift(self, monkeypatch, capsys):
        from social_data_pipeline.setup.verify import Finding
        self._patch(
            monkeypatch,
            configured=["postgres"],
            ctx_drift={"postgres": [
                Finding(category="creds", message="missing creds", fix="recover"),
            ]},
        )
        rc = sdp.cmd_db_verify(type("Args", (), {"db": None, "json": False})())
        assert rc == 1
        out = capsys.readouterr().out
        assert "DRIFT" in out
        assert "missing creds" in out
        assert "Fix: recover" in out
        assert "Exit: 1" in out

    def test_no_dbs_configured_returns_zero(self, monkeypatch, capsys):
        monkeypatch.setattr(sdp, "_get_configured_db_services", lambda: [])
        rc = sdp.cmd_db_verify(type("Args", (), {"db": None, "json": False})())
        assert rc == 0
        assert "No databases configured" in capsys.readouterr().out

    def test_json_mode_emits_machine_readable(self, monkeypatch, capsys):
        from social_data_pipeline.setup.verify import Finding
        self._patch(
            monkeypatch,
            configured=["postgres", "mongo"],
            ctx_drift={
                "postgres": [Finding(category="auth", message="m", fix="f")],
                "mongo": [],
            },
        )
        rc = sdp.cmd_db_verify(type("Args", (), {"db": None, "json": True})())
        assert rc == 1
        payload = json.loads(capsys.readouterr().out)
        assert payload["ok"] is False
        assert payload["exit_code"] == 1
        assert payload["results"]["postgres"]["ok"] is False
        assert payload["results"]["postgres"]["findings"] == [
            {"category": "auth", "message": "m", "fix": "f"},
        ]
        assert payload["results"]["mongo"]["ok"] is True
        assert payload["results"]["mongo"]["findings"] == []

    def test_json_mode_clean_is_zero(self, monkeypatch, capsys):
        self._patch(monkeypatch, configured=["postgres"], ctx_drift={"postgres": []})
        rc = sdp.cmd_db_verify(type("Args", (), {"db": None, "json": True})())
        assert rc == 0
        payload = json.loads(capsys.readouterr().out)
        assert payload["ok"] is True
        assert payload["exit_code"] == 0

    def test_db_filter_keeps_only_target_and_cross_cutting(self, monkeypatch, capsys):
        from social_data_pipeline.setup.verify import Finding
        self._patch(
            monkeypatch,
            configured=["postgres", "mongo"],
            ctx_drift={
                "postgres": [Finding(category="auth", message="pg", fix="f")],
                "mongo": [Finding(category="auth", message="mg", fix="f")],
                "mcp": [Finding(category="mcp", message="x-cut", fix="f")],
                "jobs": [Finding(category="jobs", message="j-cut", fix="f")],
            },
        )
        rc = sdp.cmd_db_verify(type("Args", (), {"db": "postgres", "json": True})())
        assert rc == 1
        payload = json.loads(capsys.readouterr().out)
        # postgres kept, mongo dropped, cross-cutting (mcp / jobs) kept
        assert "postgres" in payload["results"]
        assert "mongo" not in payload["results"]
        assert "mcp" in payload["results"]
        assert "jobs" in payload["results"]


# ---------------------------------------------------------------------------
# _resolve_cred_state — the wrapper that probes the actual filesystem.
# Verified end-to-end against tmp_path so the dataclass-ish dict shape it
# returns matches what compute_drift consumes.
# ---------------------------------------------------------------------------


class TestResolveCredState:
    def test_path_unset_in_env(self, monkeypatch):
        # No PGDATA_PATH in env → state is "no path resolvable", caller
        # decides whether to flag this as drift (auth_on yes, off no).
        state = sdp._resolve_cred_state("postgres", {})
        assert state["path"] == ""
        assert state["exists"] is False

    def test_file_present_and_readable(self, tmp_path):
        cred = tmp_path / ".ro_credentials"
        cred.write_text("password\n")
        import os
        os.chmod(cred, 0o600)
        state = sdp._resolve_cred_state("postgres", {"PGDATA_PATH": str(tmp_path)})
        assert state["exists"] is True
        assert state["mode"] == 0o600
        assert state["host_owned"] is True
        assert state["readable"] is True

    def test_file_with_loose_mode_still_exists(self, tmp_path):
        cred = tmp_path / ".ro_credentials"
        cred.write_text("password\n")
        import os
        os.chmod(cred, 0o644)
        state = sdp._resolve_cred_state("postgres", {"PGDATA_PATH": str(tmp_path)})
        assert state["exists"] is True
        assert (state["mode"] & 0o777) == 0o644
