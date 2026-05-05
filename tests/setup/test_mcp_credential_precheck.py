"""Tests for the MCP credential-file precheck (Commit 3).

`db setup-mcp` must refuse to write `mcp.yaml` if any enabled DB has
`auth: true` + an `ro_username` configured but no `.ro_credentials` file
on disk. Without this gate, MCP servers boot, fail to authenticate, and
the operator hits a 401 with no clear signal at setup time.

Pins:
- the helper that locates each cred file via `_RO_CRED_LOOKUP`,
- the refusal message names every missing file and points at
  `recover-password --regenerate-ro`.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from social_data_pipeline.setup import mcp as mcp_setup


def _make_db_setup(*, postgres_auth=False, mongo_auth=False, starrocks_auth=False,
                   ro_user="readonly"):
    """Build a `db_setup` dict shaped like `load_db_setup()` returns."""
    setup = {"databases": []}
    if postgres_auth:
        setup["databases"].append("postgres")
        setup["postgres_auth"] = True
        setup["postgres_ro_username"] = ro_user
    if mongo_auth:
        setup["databases"].append("mongo")
        setup["mongo_auth"] = True
        setup["mongo_ro_username"] = ro_user
    if starrocks_auth:
        setup["databases"].append("starrocks")
        setup["starrocks_auth"] = True
        setup["starrocks_ro_username"] = ro_user
    return setup


def test_no_missing_files_when_all_creds_present(tmp_path, monkeypatch):
    """Helper returns empty list when every enabled DB has its cred file."""
    monkeypatch.setattr(mcp_setup, "ROOT", tmp_path)

    pg_dir = tmp_path / "data" / "database" / "postgres"
    mongo_dir = tmp_path / "data" / "database" / "mongo"
    pg_dir.mkdir(parents=True)
    mongo_dir.mkdir(parents=True)
    (pg_dir / ".ro_credentials").write_text("pw\n")
    (mongo_dir / ".ro_credentials").write_text("pw\n")

    db_setup = _make_db_setup(postgres_auth=True, mongo_auth=True)
    missing = mcp_setup._missing_ro_cred_files(db_setup, env_vars={})

    assert missing == []


def test_missing_file_for_one_db(tmp_path, monkeypatch):
    """Helper flags exactly the DB whose cred file is absent."""
    monkeypatch.setattr(mcp_setup, "ROOT", tmp_path)

    pg_dir = tmp_path / "data" / "database" / "postgres"
    pg_dir.mkdir(parents=True)
    (pg_dir / ".ro_credentials").write_text("pw\n")
    # Mongo dir exists but no cred file in it.
    (tmp_path / "data" / "database" / "mongo").mkdir(parents=True)

    db_setup = _make_db_setup(postgres_auth=True, mongo_auth=True)
    missing = mcp_setup._missing_ro_cred_files(db_setup, env_vars={})

    assert len(missing) == 1
    db, path = missing[0]
    assert db == "mongo"
    assert path.endswith(".ro_credentials")
    assert "/mongo/" in path


def test_missing_files_for_all_dbs(tmp_path, monkeypatch):
    """Helper enumerates every missing file when none exist on disk."""
    monkeypatch.setattr(mcp_setup, "ROOT", tmp_path)

    db_setup = _make_db_setup(postgres_auth=True, mongo_auth=True, starrocks_auth=True)
    missing = mcp_setup._missing_ro_cred_files(db_setup, env_vars={})

    dbs = {db for db, _ in missing}
    assert dbs == {"postgres", "mongo", "starrocks"}


def test_skips_dbs_without_auth(tmp_path, monkeypatch):
    """Helper does not check DBs that don't have auth enabled."""
    monkeypatch.setattr(mcp_setup, "ROOT", tmp_path)

    db_setup = _make_db_setup(postgres_auth=False, mongo_auth=True)
    # No .ro_credentials anywhere on disk.
    (tmp_path / "data" / "database" / "mongo").mkdir(parents=True)

    missing = mcp_setup._missing_ro_cred_files(db_setup, env_vars={})

    # Only mongo has auth → only mongo is checked.
    assert {db for db, _ in missing} == {"mongo"}


def test_skips_auth_dbs_without_ro_user(tmp_path, monkeypatch):
    """Auth on but no RO user configured → not the precheck's concern.

    The "no RO user under auth" case is a separate failure mode (handled
    by the existing `missing_users` block in `main()`). The cred-file
    precheck only fires for DBs that *do* have an RO user, so it doesn't
    double-report or false-flag the no-RO path.
    """
    monkeypatch.setattr(mcp_setup, "ROOT", tmp_path)

    db_setup = {"databases": ["postgres"], "postgres_auth": True}
    # Note: no `postgres_ro_username`.

    missing = mcp_setup._missing_ro_cred_files(db_setup, env_vars={})

    assert missing == []


def test_respects_data_path_env_override(tmp_path, monkeypatch):
    """Helper consults `*_DATA_PATH` env vars before falling back to defaults.

    Operators with non-default volume layouts must not be told their cred
    file is missing when it lives at the env-overridden path.
    """
    monkeypatch.setattr(mcp_setup, "ROOT", tmp_path)

    custom_pg = tmp_path / "elsewhere" / "pg"
    custom_pg.mkdir(parents=True)
    (custom_pg / ".ro_credentials").write_text("pw\n")

    db_setup = _make_db_setup(postgres_auth=True)
    env_vars = {"PGDATA_PATH": str(custom_pg)}

    missing = mcp_setup._missing_ro_cred_files(db_setup, env_vars)

    assert missing == []


def test_main_refuses_when_cred_file_missing(tmp_path, monkeypatch, capsys):
    """`main()` prints an actionable error and exits non-zero on missing creds.

    This is the user-visible refusal: not just the helper's data, but the
    formatted "Error: ... missing for: ... regenerate with ..." block.
    """
    monkeypatch.setattr(mcp_setup, "ROOT", tmp_path)

    # No cred files on disk; postgres has auth + RO user → must fail.
    monkeypatch.setattr(
        mcp_setup, "load_db_setup",
        lambda: _make_db_setup(postgres_auth=True),
    )
    monkeypatch.setattr(mcp_setup, "_load_env_vars", lambda: {})

    with pytest.raises(SystemExit) as excinfo:
        mcp_setup.main()

    assert excinfo.value.code == 1
    err = capsys.readouterr().out
    assert ".ro_credentials" in err
    assert "postgres" in err
    assert "recover-password --regenerate-ro" in err
