"""Tests for `sdp db reset --db <name>`.

Reset is the lighter-touch sibling of `db unsetup --db`: it wipes the
database's data but preserves every configuration surface (config files,
.env, docker-compose.override.yml, MCP entries, per-source overrides,
.ro_credentials) so the operator can re-ingest without going through a
full setup cycle.

Contracts pinned here:

- Config preservation: every config / env / override / MCP / per-source
  file present before reset is byte-identical after.
- `.ro_credentials` is NEVER deleted (the entrypoint re-creates the RO
  user from it on the next start).
- Compose down is restricted to the DB profile and its MCP profile —
  never tears down sibling DBs or the jobs profile.
- Mongo reset wipes `<MONGO_DATA_PATH>/db/`, NOT the parent. This is the
  bug class the `recover-password --directoryperdb` incident surfaced.
- StarRocks reset wipes BOTH `fe/` and `be/` (the two SR-owned subdirs)
  plus the contents of every `storage_paths[*]` extra disk.
- Postgres reset wipes `pgdata/` plus tablespace contents (preserves
  tablespace dirs themselves so the docker mount still resolves).
- Reset refuses when an ingest container is in flight against the
  target DB — otherwise the orchestrator container keeps writing into
  state_tracking/ as it's being deleted.
- A `--db` value with no configuration short-circuits to a no-op.

These tests mock out `docker_compose` and `subprocess.run` (the chown
container call) — the rmtree + mkdir loop runs for real against tmp
paths so we can assert file-level outcomes.
"""

from __future__ import annotations

import io
import subprocess
from contextlib import redirect_stdout

import sdp


# ---------------------------------------------------------------------------
# Helpers shared by the suite.
# ---------------------------------------------------------------------------


def _make_args(db):
    return type("Args", (), {"db": db})()


def _seed_pg_layout(tmp_path):
    """Build a realistic tmp layout for a postgres install.

    Returns the resolved Path to the pgdata parent (what PGDATA_PATH
    points at) so callers can assert what was wiped / kept.
    """
    data_path = tmp_path / "data" / "database" / "postgres"
    (data_path / "pgdata").mkdir(parents=True)
    (data_path / "pgdata" / "PG_VERSION").write_text("18\n")
    (data_path / "pgdata" / "pg_wal").mkdir()
    (data_path / "pgdata" / "pg_wal" / "0001.wal").write_text("wal")
    (data_path / "state_tracking").mkdir()
    (data_path / "state_tracking" / "reddit_postgres_RC.json").write_text("{}")
    (data_path / ".ro_credentials").write_text("preserved-ro-pw\n")
    return data_path


def _seed_mongo_layout(tmp_path):
    data_path = tmp_path / "data" / "database" / "mongo"
    (data_path / "db").mkdir(parents=True)
    # directoryPerDB layout — the bug class the recovery flow surfaced.
    (data_path / "db" / "WiredTiger").write_text("wt")
    (data_path / "db" / "reddit_RC").mkdir()
    (data_path / "db" / "reddit_RC" / "collection-0.wt").write_text("data")
    (data_path / "db" / ".sdb_auth_initialized").write_text("")
    (data_path / "state_tracking").mkdir()
    (data_path / "state_tracking" / "reddit_mongo_RC.json").write_text("{}")
    (data_path / "logs").mkdir()
    (data_path / "logs" / "mongo.log").write_text("hello")
    (data_path / ".ro_credentials").write_text("preserved-ro-pw\n")
    return data_path


def _seed_sr_layout(tmp_path):
    data_path = tmp_path / "data" / "database" / "starrocks"
    (data_path / "fe" / "meta").mkdir(parents=True)
    (data_path / "fe" / "meta" / "image.0").write_text("meta")
    (data_path / "be" / "storage").mkdir(parents=True)
    (data_path / "be" / "storage" / "tablet").write_text("data")
    (data_path / "state_tracking").mkdir()
    (data_path / "state_tracking" / "reddit_sr_ingest_RC.json").write_text("{}")
    (data_path / ".ro_credentials").write_text("preserved-ro-pw\n")
    return data_path


def _seed_config_surfaces(tmp_path, db_name):
    """Drop every config file reset is meant to preserve into tmp_path."""
    config_dir = tmp_path / "config"
    (config_dir / "db").mkdir(parents=True)
    (config_dir / "db" / f"{db_name}.yaml").write_text(f"# {db_name} config\n")
    (config_dir / "db" / "mcp.yaml").write_text(
        f"{db_name}:\n  enabled: true\n  port: 8000\n"
    )
    (config_dir / "sources" / "reddit").mkdir(parents=True)
    (config_dir / "sources" / "reddit" / f"{db_name}.yaml").write_text(
        "# per-source override\n"
    )
    (tmp_path / ".env").write_text(
        f"{sdp._DB_DATA_PATH_ENV[db_name]}=./data/database/{db_name}\n"
        f"{db_name.upper()}_PORT=1234\n"
    )
    (tmp_path / "docker-compose.override.yml").write_text(
        f"services:\n  {db_name}:\n    volumes: []\n"
    )
    return config_dir


class _StubCallRecorder:
    """Record `docker_compose` and `subprocess.run` calls without side-effects."""

    def __init__(self):
        self.compose_calls = []
        self.subprocess_calls = []

    def compose(self, *args, **kwargs):
        self.compose_calls.append(args)
        return subprocess.CompletedProcess(args=args, returncode=0)

    def run(self, *args, **kwargs):
        self.subprocess_calls.append((args, kwargs))
        return subprocess.CompletedProcess(args[0] if args else (), 0,
                                           stdout="", stderr="")


def _patch_for_reset(monkeypatch, tmp_path, db_name, data_path,
                     *, in_flight=None, configured=True, input_answer="y",
                     orphaned_jobs=None):
    """Wire up monkeypatches shared by most tests.

    Returns the `_StubCallRecorder` so tests can inspect captured argv.
    """
    monkeypatch.setattr(sdp, "ROOT", tmp_path)
    monkeypatch.setattr(sdp, "CONFIG_DIR", tmp_path / "config")
    monkeypatch.setattr(
        sdp, "load_env",
        lambda: {sdp._DB_DATA_PATH_ENV[db_name]: str(data_path)},
    )
    monkeypatch.setattr(
        sdp, "_get_configured_db_services",
        lambda: [db_name] if configured else [],
    )
    monkeypatch.setattr(
        sdp, "_running_services",
        lambda: set(in_flight or []),
    )
    monkeypatch.setattr(
        sdp, "_orphaned_jobs_targets_for",
        lambda _db: list(orphaned_jobs or []),
    )
    # Tablespaces / storage_paths default to empty unless a test overrides.
    monkeypatch.setattr(sdp, "_read_pg_tablespace_paths", lambda: {})
    monkeypatch.setattr(sdp, "_read_sr_storage_paths", lambda: {})

    recorder = _StubCallRecorder()
    monkeypatch.setattr(sdp, "docker_compose", recorder.compose)
    monkeypatch.setattr(sdp.subprocess, "run", recorder.run)
    monkeypatch.setattr("builtins.input", lambda *_a, **_kw: input_answer)
    return recorder


# ---------------------------------------------------------------------------
# Early-exit paths.
# ---------------------------------------------------------------------------


class TestResetEarlyExits:
    def test_no_configuration_returns_zero(self, monkeypatch, tmp_path, capsys):
        # When the DB isn't configured, reset is a no-op.
        _patch_for_reset(monkeypatch, tmp_path, "postgres",
                         data_path=tmp_path / "pg", configured=False)
        rc = sdp.cmd_db_reset(_make_args("postgres"))
        out = capsys.readouterr().out
        assert rc == 0
        assert "No postgres configuration found" in out

    def test_missing_env_path_returns_zero(self, monkeypatch, tmp_path, capsys):
        # PGDATA_PATH not in .env → nothing to do; we should not crash.
        monkeypatch.setattr(sdp, "ROOT", tmp_path)
        monkeypatch.setattr(sdp, "CONFIG_DIR", tmp_path / "config")
        monkeypatch.setattr(sdp, "load_env", lambda: {})
        monkeypatch.setattr(
            sdp, "_get_configured_db_services", lambda: ["postgres"],
        )
        rc = sdp.cmd_db_reset(_make_args("postgres"))
        out = capsys.readouterr().out
        assert rc == 0
        assert "nothing to reset" in out.lower()

    def test_data_path_does_not_exist_returns_zero(self, monkeypatch, tmp_path, capsys):
        # .env points at a path that was already manually deleted.
        ghost = tmp_path / "ghost"  # not created
        _patch_for_reset(monkeypatch, tmp_path, "postgres",
                         data_path=ghost)
        rc = sdp.cmd_db_reset(_make_args("postgres"))
        out = capsys.readouterr().out
        assert rc == 0
        assert "does not exist" in out.lower()

    def test_aborts_on_no(self, monkeypatch, tmp_path):
        # Confirmation "n" → nothing is touched, nothing is stopped.
        data_path = _seed_pg_layout(tmp_path)
        rec = _patch_for_reset(monkeypatch, tmp_path, "postgres",
                               data_path=data_path, input_answer="n")
        with redirect_stdout(io.StringIO()):
            rc = sdp.cmd_db_reset(_make_args("postgres"))
        assert rc == 0
        assert rec.compose_calls == []
        assert rec.subprocess_calls == []
        assert (data_path / "pgdata" / "PG_VERSION").exists()
        assert (data_path / ".ro_credentials").exists()


# ---------------------------------------------------------------------------
# In-flight ingest guard.
# ---------------------------------------------------------------------------


class TestResetInFlightGuard:
    def test_refuses_when_postgres_ingest_running(self, monkeypatch, tmp_path, capsys):
        data_path = _seed_pg_layout(tmp_path)
        rec = _patch_for_reset(
            monkeypatch, tmp_path, "postgres",
            data_path=data_path,
            in_flight={"postgres-ingest"},
        )
        rc = sdp.cmd_db_reset(_make_args("postgres"))
        out = capsys.readouterr().out
        assert rc == 1
        assert "postgres-ingest" in out
        # Nothing stopped, nothing chowned, nothing deleted.
        assert rec.compose_calls == []
        assert rec.subprocess_calls == []
        assert (data_path / "pgdata" / "PG_VERSION").exists()

    def test_refuses_when_postgres_ml_running(self, monkeypatch, tmp_path):
        # Both postgres-ingest AND postgres-ml count.
        data_path = _seed_pg_layout(tmp_path)
        rec = _patch_for_reset(
            monkeypatch, tmp_path, "postgres",
            data_path=data_path,
            in_flight={"postgres-ml"},
        )
        with redirect_stdout(io.StringIO()):
            rc = sdp.cmd_db_reset(_make_args("postgres"))
        assert rc == 1
        assert rec.compose_calls == []

    def test_refuses_when_sr_ml_running(self, monkeypatch, tmp_path):
        data_path = _seed_sr_layout(tmp_path)
        rec = _patch_for_reset(
            monkeypatch, tmp_path, "starrocks",
            data_path=data_path,
            in_flight={"sr-ml"},
        )
        with redirect_stdout(io.StringIO()):
            rc = sdp.cmd_db_reset(_make_args("starrocks"))
        assert rc == 1
        assert rec.compose_calls == []

    def test_unrelated_service_does_not_block(self, monkeypatch, tmp_path):
        # parse / lingua / mongo-ingest don't read the postgres state files,
        # so they must not block a postgres reset.
        data_path = _seed_pg_layout(tmp_path)
        rec = _patch_for_reset(
            monkeypatch, tmp_path, "postgres",
            data_path=data_path,
            in_flight={"parse", "lingua", "mongo-ingest"},
        )
        with redirect_stdout(io.StringIO()):
            rc = sdp.cmd_db_reset(_make_args("postgres"))
        assert rc == 0
        # Compose down was called → guard didn't trip.
        assert rec.compose_calls != []


# ---------------------------------------------------------------------------
# Config preservation — the central reset contract.
# ---------------------------------------------------------------------------


class TestResetPreservesConfig:
    def test_postgres_preserves_every_config_surface(self, monkeypatch, tmp_path):
        data_path = _seed_pg_layout(tmp_path)
        config_dir = _seed_config_surfaces(tmp_path, "postgres")
        env_before = (tmp_path / ".env").read_bytes()
        override_before = (tmp_path / "docker-compose.override.yml").read_bytes()
        mcp_before = (config_dir / "db" / "mcp.yaml").read_bytes()
        db_yaml_before = (config_dir / "db" / "postgres.yaml").read_bytes()
        source_override_before = (
            config_dir / "sources" / "reddit" / "postgres.yaml"
        ).read_bytes()
        ro_before = (data_path / ".ro_credentials").read_bytes()

        _patch_for_reset(monkeypatch, tmp_path, "postgres", data_path)

        with redirect_stdout(io.StringIO()):
            rc = sdp.cmd_db_reset(_make_args("postgres"))

        assert rc == 0
        # Every config surface is byte-identical post-reset.
        assert (tmp_path / ".env").read_bytes() == env_before
        assert (tmp_path / "docker-compose.override.yml").read_bytes() == override_before
        assert (config_dir / "db" / "mcp.yaml").read_bytes() == mcp_before
        assert (config_dir / "db" / "postgres.yaml").read_bytes() == db_yaml_before
        assert (
            config_dir / "sources" / "reddit" / "postgres.yaml"
        ).read_bytes() == source_override_before
        assert (data_path / ".ro_credentials").read_bytes() == ro_before
        # Server-owned subdirs got wiped clean.
        assert (data_path / "pgdata").exists()
        assert list((data_path / "pgdata").iterdir()) == []
        assert (data_path / "state_tracking").exists()
        assert list((data_path / "state_tracking").iterdir()) == []

    def test_mongo_preserves_every_config_surface(self, monkeypatch, tmp_path):
        data_path = _seed_mongo_layout(tmp_path)
        _seed_config_surfaces(tmp_path, "mongo")
        ro_before = (data_path / ".ro_credentials").read_bytes()

        _patch_for_reset(monkeypatch, tmp_path, "mongo", data_path)

        with redirect_stdout(io.StringIO()):
            rc = sdp.cmd_db_reset(_make_args("mongo"))

        assert rc == 0
        assert (data_path / ".ro_credentials").read_bytes() == ro_before
        # All three server subdirs wiped.
        for sub in ("db", "state_tracking", "logs"):
            assert (data_path / sub).exists()
            assert list((data_path / sub).iterdir()) == []

    def test_starrocks_preserves_every_config_surface(self, monkeypatch, tmp_path):
        data_path = _seed_sr_layout(tmp_path)
        _seed_config_surfaces(tmp_path, "starrocks")
        ro_before = (data_path / ".ro_credentials").read_bytes()

        _patch_for_reset(monkeypatch, tmp_path, "starrocks", data_path)

        with redirect_stdout(io.StringIO()):
            rc = sdp.cmd_db_reset(_make_args("starrocks"))

        assert rc == 0
        assert (data_path / ".ro_credentials").read_bytes() == ro_before
        for sub in ("fe", "be", "state_tracking"):
            assert (data_path / sub).exists()
            assert list((data_path / sub).iterdir()) == []


# ---------------------------------------------------------------------------
# Bug-class regressions: pinned per-DB wipe shapes.
# ---------------------------------------------------------------------------


class TestResetWipeShapes:
    def test_mongo_wipes_db_subdir_not_parent(self, monkeypatch, tmp_path):
        # The recover-password bug class: wiping the wrong layer corrupted
        # the directoryPerDB tree. Reset must target db/ specifically and
        # leave parent-level files alone.
        data_path = _seed_mongo_layout(tmp_path)
        _patch_for_reset(monkeypatch, tmp_path, "mongo", data_path)

        with redirect_stdout(io.StringIO()):
            sdp.cmd_db_reset(_make_args("mongo"))

        # Parent dir survives, .ro_credentials survives.
        assert data_path.exists()
        assert (data_path / ".ro_credentials").exists()
        # The directoryPerDB subdirs and the auth marker are gone.
        assert not (data_path / "db" / "WiredTiger").exists()
        assert not (data_path / "db" / "reddit_RC").exists()
        assert not (data_path / "db" / ".sdb_auth_initialized").exists()

    def test_starrocks_wipes_both_fe_and_be(self, monkeypatch, tmp_path):
        # SR stores FE meta + BE storage in sibling subdirs. Reset must
        # wipe both — wiping only one leaves a half-initialized cluster
        # SR refuses to start cleanly from.
        data_path = _seed_sr_layout(tmp_path)
        _patch_for_reset(monkeypatch, tmp_path, "starrocks", data_path)

        with redirect_stdout(io.StringIO()):
            sdp.cmd_db_reset(_make_args("starrocks"))

        assert not (data_path / "fe" / "meta").exists()
        assert not (data_path / "be" / "storage").exists()
        # The subdirs themselves are recreated empty so future mounts resolve.
        assert (data_path / "fe").is_dir()
        assert (data_path / "be").is_dir()


# ---------------------------------------------------------------------------
# Container lifecycle: only the target DB profile + its MCP profile.
# ---------------------------------------------------------------------------


class TestResetStopsCorrectProfiles:
    def _capture_profiles(self, monkeypatch, tmp_path, db_name, data_path):
        rec = _patch_for_reset(monkeypatch, tmp_path, db_name, data_path)
        with redirect_stdout(io.StringIO()):
            sdp.cmd_db_reset(_make_args(db_name))
        return rec.compose_calls

    def test_postgres_down_includes_postgres_and_postgres_mcp(self, monkeypatch, tmp_path):
        data_path = _seed_pg_layout(tmp_path)
        calls = self._capture_profiles(monkeypatch, tmp_path, "postgres", data_path)
        assert len(calls) == 1
        argv = calls[0]
        assert "--profile" in argv and "postgres" in argv
        assert "postgres_mcp" in argv
        assert "down" in argv
        # Crucially: no sibling DBs torn down.
        assert "mongo" not in argv
        assert "starrocks" not in argv
        assert "jobs" not in argv

    def test_mongo_down_includes_mongo_and_mongo_mcp(self, monkeypatch, tmp_path):
        data_path = _seed_mongo_layout(tmp_path)
        calls = self._capture_profiles(monkeypatch, tmp_path, "mongo", data_path)
        assert len(calls) == 1
        argv = calls[0]
        assert "mongo" in argv and "mongo_mcp" in argv
        assert "postgres" not in argv
        assert "starrocks" not in argv

    def test_starrocks_down_includes_starrocks_and_starrocks_mcp(self, monkeypatch, tmp_path):
        data_path = _seed_sr_layout(tmp_path)
        calls = self._capture_profiles(monkeypatch, tmp_path, "starrocks", data_path)
        assert len(calls) == 1
        argv = calls[0]
        assert "starrocks" in argv and "starrocks_mcp" in argv
        assert "postgres" not in argv
        assert "mongo" not in argv


# ---------------------------------------------------------------------------
# Chown image matrix.
# ---------------------------------------------------------------------------


class TestResetChownImage:
    def _chown_argv(self, monkeypatch, tmp_path, db_name, data_path):
        rec = _patch_for_reset(monkeypatch, tmp_path, db_name, data_path)
        with redirect_stdout(io.StringIO()):
            sdp.cmd_db_reset(_make_args(db_name))
        # The chown subprocess is the only `docker run --rm ...` call.
        chowns = [
            argv for (argv, _kw) in rec.subprocess_calls
            if argv and isinstance(argv[0], list)
            and argv[0][:3] == ["docker", "run", "--rm"]
        ]
        assert len(chowns) == 1, chowns
        return chowns[0][0]

    def test_postgres_uses_postgres_image(self, monkeypatch, tmp_path):
        argv = self._chown_argv(monkeypatch, tmp_path, "postgres",
                                _seed_pg_layout(tmp_path))
        assert "postgres:18" in argv

    def test_mongo_uses_mongo_image(self, monkeypatch, tmp_path):
        argv = self._chown_argv(monkeypatch, tmp_path, "mongo",
                                _seed_mongo_layout(tmp_path))
        assert "mongo:8" in argv

    def test_starrocks_uses_starrocks_image(self, monkeypatch, tmp_path):
        argv = self._chown_argv(monkeypatch, tmp_path, "starrocks",
                                _seed_sr_layout(tmp_path))
        assert "starrocks/allin1-ubuntu" in argv


# ---------------------------------------------------------------------------
# Tablespaces (PG) and storage_paths (SR): contents wiped, dir kept.
# ---------------------------------------------------------------------------


class TestResetTablespaces:
    def test_pg_tablespace_contents_wiped_dir_preserved(self, monkeypatch, tmp_path):
        # The tablespace dir must survive the wipe because docker-compose
        # mounts it on the next `db start`; only its contents go.
        data_path = _seed_pg_layout(tmp_path)
        ts_dir = tmp_path / "extra" / "pg_cold"
        ts_dir.mkdir(parents=True)
        (ts_dir / "tablespace_data").write_text("cold")
        sibling = tmp_path / "extra" / "do_not_touch"
        sibling.mkdir()
        (sibling / "important.txt").write_text("keep")

        rec = _patch_for_reset(monkeypatch, tmp_path, "postgres", data_path)
        monkeypatch.setattr(
            sdp, "_read_pg_tablespace_paths",
            lambda: {"cold": str(ts_dir)},
        )

        with redirect_stdout(io.StringIO()):
            sdp.cmd_db_reset(_make_args("postgres"))

        # The chown argv must include both /tablespace/cold (recursive) and
        # /tsparent_cold (non-recursive parent).
        chown_argvs = [
            argv for (argv, _kw) in rec.subprocess_calls
            if argv and isinstance(argv[0], list)
            and argv[0][:3] == ["docker", "run", "--rm"]
        ]
        assert chown_argvs
        flat = " ".join(chown_argvs[0][0])
        assert "/tablespace/cold" in flat
        assert "/tsparent_cold" in flat

        # Contents gone, dir preserved.
        assert ts_dir.exists()
        assert list(ts_dir.iterdir()) == []
        # Sibling on the same parent disk untouched (non-recursive chown
        # of parent doesn't recurse into siblings; we don't wipe siblings).
        assert (sibling / "important.txt").read_text() == "keep"

    def test_sr_storage_paths_contents_wiped_dir_preserved(self, monkeypatch, tmp_path):
        data_path = _seed_sr_layout(tmp_path)
        sp = tmp_path / "extra" / "sr_disk1"
        sp.mkdir(parents=True)
        (sp / "tablet_001").write_text("data")

        _patch_for_reset(monkeypatch, tmp_path, "starrocks", data_path)
        monkeypatch.setattr(
            sdp, "_read_sr_storage_paths",
            lambda: {"storage_0": str(sp)},
        )

        with redirect_stdout(io.StringIO()):
            sdp.cmd_db_reset(_make_args("starrocks"))

        assert sp.exists()
        assert list(sp.iterdir()) == []

    def test_missing_tablespace_dir_skipped_silently(self, monkeypatch, tmp_path):
        # If a tablespace dir referenced in config doesn't exist on disk
        # (operator moved it, never created it, etc.), reset must not crash.
        data_path = _seed_pg_layout(tmp_path)
        ghost_ts = tmp_path / "extra" / "ghost_ts"  # NOT created

        _patch_for_reset(monkeypatch, tmp_path, "postgres", data_path)
        monkeypatch.setattr(
            sdp, "_read_pg_tablespace_paths",
            lambda: {"ghost": str(ghost_ts)},
        )

        with redirect_stdout(io.StringIO()):
            rc = sdp.cmd_db_reset(_make_args("postgres"))
        assert rc == 0


# ---------------------------------------------------------------------------
# Orphaned-jobs warning (same shape as unsetup).
# ---------------------------------------------------------------------------


class TestResetOrphanedJobsWarning:
    def test_warning_shown_when_jobs_target_exists(self, monkeypatch, tmp_path):
        data_path = _seed_pg_layout(tmp_path)
        _patch_for_reset(
            monkeypatch, tmp_path, "postgres", data_path,
            orphaned_jobs=["pg_main"],
        )
        buf = io.StringIO()
        with redirect_stdout(buf):
            sdp.cmd_db_reset(_make_args("postgres"))
        out = buf.getvalue()
        assert "pg_main" in out
        assert "Jobs scheduler" in out

    def test_no_warning_when_no_targets(self, monkeypatch, tmp_path):
        data_path = _seed_pg_layout(tmp_path)
        _patch_for_reset(
            monkeypatch, tmp_path, "postgres", data_path,
            orphaned_jobs=[],
        )
        buf = io.StringIO()
        with redirect_stdout(buf):
            sdp.cmd_db_reset(_make_args("postgres"))
        assert "Jobs scheduler" not in buf.getvalue()


# ---------------------------------------------------------------------------
# Module-level constants pinned (cheap regression guard against typos).
# ---------------------------------------------------------------------------


class TestResetConstants:
    def test_reset_subdirs_match_dbs(self):
        assert set(sdp._DB_RESET_SUBDIRS) == {"postgres", "mongo", "starrocks"}
        # Pin the canonical layout: mongo's data is in `db/` (not `data/`,
        # `mongo/`, etc.) — wiping the wrong subdir is the regression target.
        assert "db" in sdp._DB_RESET_SUBDIRS["mongo"]
        assert "pgdata" in sdp._DB_RESET_SUBDIRS["postgres"]
        assert set(sdp._DB_RESET_SUBDIRS["starrocks"]) >= {"fe", "be"}

    def test_ingest_service_map_covers_each_db(self):
        assert set(sdp._DB_INGEST_SERVICES) == {"postgres", "mongo", "starrocks"}
        assert sdp._DB_INGEST_SERVICES["postgres"] == {"postgres-ingest", "postgres-ml"}
        assert sdp._DB_INGEST_SERVICES["mongo"] == {"mongo-ingest"}
        assert sdp._DB_INGEST_SERVICES["starrocks"] == {"sr-ingest", "sr-ml"}

    def test_chown_image_map_covers_each_db(self):
        assert set(sdp._DB_CHOWN_IMAGE) == {"postgres", "mongo", "starrocks"}
