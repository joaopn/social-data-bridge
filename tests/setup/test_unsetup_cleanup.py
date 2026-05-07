"""Tests for unsetup-symmetry cleanups.

Four contracts pinned here:

- ``db unsetup --db starrocks`` and full ``db unsetup`` enumerate
  ``storage_paths`` from ``config/db/starrocks.yaml`` and rmtree them
  alongside the primary data dir, mirroring how PG handles tablespaces.
- ``db unsetup-mcp`` notes that locally-built / pulled MCP images
  remain on disk so the operator knows the manual prune line exists.
- ``_unsetup_single_db`` checks the jobs-scheduler config for targets
  pointing at the DB being removed and warns inline; the user runs
  ``db unsetup-jobs`` (or edits the config) to clean up.

These tests cover the readable-input / printed-output edges. The actual
``shutil.rmtree`` + chown-via-docker dance inside ``_unsetup_single_db``
and ``cmd_db_unsetup`` lives behind interactive ``input()`` prompts and
``docker run`` calls — exercising it for real lives in E2E. Here we pin
the helpers and the surface text.
"""

from __future__ import annotations

import io
from contextlib import redirect_stdout

import sdp


# ---------------------------------------------------------------------------
# `_read_sr_storage_paths` is the source of truth for SR cleanup.
# ---------------------------------------------------------------------------


class TestReadSrStoragePaths:
    def _patch_config(self, tmp_path, monkeypatch, content):
        config_dir = tmp_path / "config"
        (config_dir / "db").mkdir(parents=True)
        if content is not None:
            (config_dir / "db" / "starrocks.yaml").write_text(content)
        monkeypatch.setattr(sdp, "CONFIG_DIR", config_dir)

    def test_returns_indexed_paths_from_yaml(self, tmp_path, monkeypatch):
        self._patch_config(tmp_path, monkeypatch, (
            "port: 9030\n"
            "storage_paths:\n"
            "  - /mnt/disk1\n"
            "  - /mnt/disk2\n"
        ))
        assert sdp._read_sr_storage_paths() == {
            "storage_0": "/mnt/disk1",
            "storage_1": "/mnt/disk2",
        }

    def test_no_yaml_returns_empty(self, tmp_path, monkeypatch):
        self._patch_config(tmp_path, monkeypatch, None)
        assert sdp._read_sr_storage_paths() == {}

    def test_no_storage_paths_key_returns_empty(self, tmp_path, monkeypatch):
        # Single-disk install (the common case) — yaml has no extra disks.
        self._patch_config(tmp_path, monkeypatch, "port: 9030\n")
        assert sdp._read_sr_storage_paths() == {}

    def test_storage_paths_not_a_list_is_ignored(self, tmp_path, monkeypatch):
        # Defensive: malformed yaml shouldn't crash; treat as no extras.
        self._patch_config(tmp_path, monkeypatch, (
            "storage_paths: '/wrong/shape'\n"
        ))
        assert sdp._read_sr_storage_paths() == {}

    def test_empty_strings_skipped(self, tmp_path, monkeypatch):
        # Empty entries don't get a synthetic name — they'd produce mounts
        # to the workspace root if we kept them.
        self._patch_config(tmp_path, monkeypatch, (
            "storage_paths:\n"
            "  - /mnt/disk1\n"
            "  - ''\n"
            "  - /mnt/disk2\n"
        ))
        result = sdp._read_sr_storage_paths()
        assert "/mnt/disk1" in result.values()
        assert "/mnt/disk2" in result.values()
        assert "" not in result.values()


# ---------------------------------------------------------------------------
# Orphaned-jobs-target detection.
# ---------------------------------------------------------------------------


class TestOrphanedJobsTargets:
    def _patch_jobs(self, monkeypatch, *, configured, targets):
        monkeypatch.setattr(sdp, "_is_jobs_configured", lambda: configured)
        monkeypatch.setattr(
            sdp, "_load_jobs_config",
            lambda: {"targets": targets} if configured else {},
        )

    def test_no_jobs_configured_returns_empty(self, monkeypatch):
        self._patch_jobs(monkeypatch, configured=False, targets={})
        assert sdp._orphaned_jobs_targets_for("postgres") == []

    def test_postgres_target_matched(self, monkeypatch):
        self._patch_jobs(monkeypatch, configured=True, targets={
            "pg_main": {"backend": "postgres", "database": "datasets"},
            "sr_main": {"backend": "starrocks", "database": ""},
        })
        assert sdp._orphaned_jobs_targets_for("postgres") == ["pg_main"]
        assert sdp._orphaned_jobs_targets_for("starrocks") == ["sr_main"]

    def test_mongo_db_name_routes_through_mongodb_backend(self, monkeypatch):
        # Jobs config uses 'mongodb' for the mongo backend; the helper has to
        # map db_name='mongo' → backend='mongodb' before matching.
        self._patch_jobs(monkeypatch, configured=True, targets={
            "mongo_main": {"backend": "mongodb", "database": ""},
        })
        assert sdp._orphaned_jobs_targets_for("mongo") == ["mongo_main"]

    def test_no_match_returns_empty(self, monkeypatch):
        self._patch_jobs(monkeypatch, configured=True, targets={
            "pg_main": {"backend": "postgres", "database": ""},
        })
        # Removing starrocks: no target referenced it.
        assert sdp._orphaned_jobs_targets_for("starrocks") == []

    def test_unknown_db_name_returns_empty(self, monkeypatch):
        # Defensive — caller passes a non-DB name. No matches, no crash.
        self._patch_jobs(monkeypatch, configured=True, targets={
            "pg_main": {"backend": "postgres", "database": ""},
        })
        assert sdp._orphaned_jobs_targets_for("redis") == []


# ---------------------------------------------------------------------------
# MCP-image-note shows up in cmd_db_unsetup_mcp output.
# ---------------------------------------------------------------------------


class TestUnsetupMcpImageNote:
    def test_image_note_printed_when_mcp_config_exists(self, tmp_path, monkeypatch):
        config_dir = tmp_path / "config"
        (config_dir / "db").mkdir(parents=True)
        mcp_path = config_dir / "db" / "mcp.yaml"
        mcp_path.write_text(
            "postgres:\n"
            "  enabled: true\n"
            "  port: 8000\n"
            "  access_mode: restricted\n"
            "  mcp_user: readonly\n"
        )

        # Empty .env so the env-strip pass has nothing to do.
        env_path = tmp_path / ".env"
        env_path.write_text("")

        monkeypatch.setattr(sdp, "ROOT", tmp_path)
        monkeypatch.setattr(sdp, "CONFIG_DIR", config_dir)
        monkeypatch.setattr(sdp, "_get_configured_mcp_services", lambda: [])
        monkeypatch.setattr(sdp, "_get_configured_db_services", lambda: [])

        buf = io.StringIO()
        with redirect_stdout(buf):
            rc = sdp.cmd_db_unsetup_mcp(type("Args", (), {})())

        out = buf.getvalue()
        assert rc == 0
        assert "MCP configuration removed" in out
        assert "MCP images remain on disk" in out
        assert "docker image prune" in out

    def test_no_note_when_no_mcp_config(self, tmp_path, monkeypatch, capsys):
        # cmd_db_unsetup_mcp early-returns when there's nothing to remove —
        # no image note (nothing was undone, nothing to clean up).
        config_dir = tmp_path / "config"
        (config_dir / "db").mkdir(parents=True)
        monkeypatch.setattr(sdp, "ROOT", tmp_path)
        monkeypatch.setattr(sdp, "CONFIG_DIR", config_dir)

        rc = sdp.cmd_db_unsetup_mcp(type("Args", (), {})())
        out = capsys.readouterr().out
        assert rc == 0
        assert "No MCP configuration found" in out
        assert "MCP images remain" not in out
