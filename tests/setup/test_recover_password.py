"""Tests for `db recover-password --regenerate-ro` and the keep-existing
RO password default on setup reconfigure.

These close the "lost RO credentials" gap: without an RO regeneration
path, the only recovery for a missing or known-leaked `.ro_credentials`
was a full `db unsetup` + `db setup` cycle.

Pinned behavior:
- `_regenerate_ro_credentials_for` writes a fresh password atomically to
  every target DB's data path (one shared password — same model as setup).
- `cmd_db_recover_password --regenerate-ro` writes new cred files inside
  each DB branch before the final restart so the entrypoint's RO sync
  block applies the new password without an extra round-trip.
- `cmd_db_recover_password` without the flag leaves cred files alone
  (the flag is the only opt-in for RO regeneration).
- `_needs_admin_password` (used by recover-password's gate) covers the
  "jobs UI auth on, no DB auth" case so jobs-only installs have a
  recovery path.
- Setup reconfigure flow defaults the RO password to "keep existing"
  rather than silently rotating it on every `db setup` re-run.

Bug class: silent rotation, missing recovery paths.
"""

from __future__ import annotations

import subprocess

import sdp


# ── _regenerate_ro_credentials_for ─────────────────────────────────────────


def test_regenerate_ro_writes_given_password(monkeypatch, tmp_path):
    """Passing an explicit password writes that password to each target's data path."""
    pg_path = tmp_path / "pg"
    pg_path.mkdir()
    monkeypatch.setattr(
        sdp, "load_env",
        lambda: {"PGDATA_PATH": str(pg_path)},
    )

    out = sdp._regenerate_ro_credentials_for(["postgres"], password="explicit-pw")

    assert out == "explicit-pw"
    assert (pg_path / ".ro_credentials").read_text() == "explicit-pw\n"


def test_regenerate_ro_generates_password_when_omitted(monkeypatch, tmp_path):
    """Omitting `password` triggers `secrets.token_urlsafe(24)` and returns it."""
    pg_path = tmp_path / "pg"
    pg_path.mkdir()
    monkeypatch.setattr(
        sdp, "load_env",
        lambda: {"PGDATA_PATH": str(pg_path)},
    )

    out = sdp._regenerate_ro_credentials_for(["postgres"])

    # token_urlsafe(24) decodes to 32 base64url characters.
    assert isinstance(out, str)
    assert len(out) >= 30
    assert (pg_path / ".ro_credentials").read_text() == out + "\n"


def test_regenerate_ro_writes_to_multiple_targets(monkeypatch, tmp_path):
    """One shared password is written to every target's data path."""
    pg = tmp_path / "pg"
    mg = tmp_path / "mg"
    sr = tmp_path / "sr"
    for p in (pg, mg, sr):
        p.mkdir()
    monkeypatch.setattr(
        sdp, "load_env",
        lambda: {
            "PGDATA_PATH": str(pg),
            "MONGO_DATA_PATH": str(mg),
            "STARROCKS_DATA_PATH": str(sr),
        },
    )

    sdp._regenerate_ro_credentials_for(
        ["postgres", "mongo", "starrocks"], password="shared",
    )

    for p in (pg, mg, sr):
        assert (p / ".ro_credentials").read_text() == "shared\n"


# ── cmd_db_recover_password integration ────────────────────────────────────


def test_recover_password_with_regenerate_ro_writes_cred_files(monkeypatch, tmp_path):
    """`recover-password --regenerate-ro` writes new .ro_credentials for
    each DB with auth + ro_user. The file must be in place *before* the
    final restart so the entrypoint's RO sync block reads the new pw."""
    pg_path = tmp_path / "pg"
    pg_path.mkdir()

    monkeypatch.setattr(sdp, "ROOT", tmp_path)
    monkeypatch.setattr(sdp, "CONFIG_DIR", tmp_path / "config")
    (tmp_path / "config" / "postgres").mkdir(parents=True)
    # pg_hba.local.conf must exist for the recovery flow not to bail out.
    (tmp_path / "config" / "postgres" / "pg_hba.local.conf").write_text(
        "host all all 0.0.0.0/0 scram-sha-256\n"
    )

    monkeypatch.setattr(
        sdp, "load_env",
        lambda: {"PGDATA_PATH": str(pg_path), "POSTGRES_PORT": "5432", "DB_NAME": "datasets"},
    )
    monkeypatch.setattr(sdp, "_needs_admin_password", lambda: True)
    monkeypatch.setattr(sdp, "_get_configured_db_services", lambda: ["postgres"])
    monkeypatch.setattr(
        sdp, "_load_db_yaml",
        lambda name: {"auth": True, "ro_username": "readonly"},
    )

    # Simulate the user typing a fresh admin password.
    monkeypatch.setattr("sdp.getpass", lambda prompt="": "new-admin-pw")
    # Stub out compose + subprocess so we don't actually start containers.
    monkeypatch.setattr(
        sdp, "docker_compose",
        lambda *a, **kw: subprocess.CompletedProcess(args=a, returncode=0),
    )
    monkeypatch.setattr(
        subprocess, "run",
        lambda *a, **kw: subprocess.CompletedProcess(a, 0, stdout="", stderr=""),
    )

    args = type("A", (), {"regenerate_ro": True})()
    rc = sdp.cmd_db_recover_password(args)

    assert rc == 0
    cred = pg_path / ".ro_credentials"
    assert cred.exists()
    # New password is non-empty and not the admin password (different secret).
    content = cred.read_text().strip()
    assert content
    assert content != "new-admin-pw"


def test_recover_password_without_regenerate_ro_does_not_touch_cred(monkeypatch, tmp_path):
    """Without `--regenerate-ro`, the cred file must NOT be rewritten —
    the flag is the only opt-in for RO regeneration."""
    pg_path = tmp_path / "pg"
    pg_path.mkdir()
    (pg_path / ".ro_credentials").write_text("preserved-ro-pw\n")

    monkeypatch.setattr(sdp, "ROOT", tmp_path)
    monkeypatch.setattr(sdp, "CONFIG_DIR", tmp_path / "config")
    (tmp_path / "config" / "postgres").mkdir(parents=True)
    (tmp_path / "config" / "postgres" / "pg_hba.local.conf").write_text("trust\n")

    monkeypatch.setattr(
        sdp, "load_env",
        lambda: {"PGDATA_PATH": str(pg_path), "POSTGRES_PORT": "5432", "DB_NAME": "datasets"},
    )
    monkeypatch.setattr(sdp, "_needs_admin_password", lambda: True)
    monkeypatch.setattr(sdp, "_get_configured_db_services", lambda: ["postgres"])
    monkeypatch.setattr(
        sdp, "_load_db_yaml",
        lambda name: {"auth": True, "ro_username": "readonly"},
    )
    monkeypatch.setattr("sdp.getpass", lambda prompt="": "new-admin-pw")
    monkeypatch.setattr(
        sdp, "docker_compose",
        lambda *a, **kw: subprocess.CompletedProcess(args=a, returncode=0),
    )
    monkeypatch.setattr(
        subprocess, "run",
        lambda *a, **kw: subprocess.CompletedProcess(a, 0, stdout="", stderr=""),
    )

    args = type("A", (), {"regenerate_ro": False})()
    rc = sdp.cmd_db_recover_password(args)

    assert rc == 0
    assert (pg_path / ".ro_credentials").read_text() == "preserved-ro-pw\n"


def test_recover_password_gate_covers_jobs_only_auth(monkeypatch):
    """The gate is `_needs_admin_password`, which is True when only the
    jobs UI has auth on (no DB auth). This gives jobs-only installs a
    recovery path for the admin password — they share it with the jobs
    UI even though no DB requires it."""
    monkeypatch.setattr(sdp, "_is_auth_enabled", lambda: False)
    monkeypatch.setattr(sdp, "_is_jobs_auth_enabled", lambda: True)

    # _needs_admin_password ORs both, so the gate must let us through.
    assert sdp._needs_admin_password() is True


# ── setup reconfigure: keep existing RO password by default ────────────────
#
# We test `_resolve_ro_password` directly rather than the full
# `run_questionnaire` flow. The questionnaire has dozens of unrelated
# prompts (PGTune paste, mongo cache, SR FE heap, …) that would have to
# be mocked one-by-one and that change every time setup grows a new
# question. The keep-existing-RO logic lives in the helper, so we pin
# its contract there and trust the questionnaire to call it.


def test_resolve_ro_returns_existing_when_kept(monkeypatch, tmp_path):
    """Same username + cred file present + user accepts default → keep.
    Closes the silent-rotation default that re-rolled the password on
    every `db setup` re-run.
    """
    from social_data_pipeline.setup import db as db_setup

    pg_path = tmp_path / "pg"
    pg_path.mkdir()
    (pg_path / ".ro_credentials").write_text("preserved-ro-pw\n")

    prompts = []

    def fake_ask_bool(label, default, tag=None):
        prompts.append((tag, default))
        return default  # accept whatever the default is

    monkeypatch.setattr(db_setup, "ask_bool", fake_ask_bool)

    pw = db_setup._resolve_ro_password(
        "readonly",
        {
            "auth_enabled": True,
            "ro_username": "readonly",
            "pgdata_path": str(pg_path),
        },
    )

    assert pw == "preserved-ro-pw"
    # Pinned: the new keep-existing prompt fires with default True.
    assert ("db_ro_keep_existing", True) in prompts
    # Auto-generate prompt must not be reached — keep-existing is the only path.
    assert ("db_ro_auto_password", True) not in prompts


def test_resolve_ro_username_changed_skips_keep_prompt(monkeypatch, tmp_path):
    """A username change is a different DB role; the existing password
    doesn't apply. The keep-existing prompt is suppressed entirely and
    the existing auto-generate path runs.
    """
    from social_data_pipeline.setup import db as db_setup

    pg_path = tmp_path / "pg"
    pg_path.mkdir()
    (pg_path / ".ro_credentials").write_text("preserved-ro-pw\n")

    prompts = []
    monkeypatch.setattr(
        db_setup, "ask_bool",
        lambda label, default, tag=None: prompts.append(tag) or default,
    )
    monkeypatch.setattr(
        db_setup, "ask_password",
        lambda *a, tag=None, **kw: "explicit-pw",
    )

    pw = db_setup._resolve_ro_password(
        "newname",  # ← changed
        {
            "auth_enabled": True,
            "ro_username": "readonly",
            "pgdata_path": str(pg_path),
        },
    )

    assert "db_ro_keep_existing" not in prompts
    # Auto-generate fires (default True), returns a token_urlsafe-shaped string.
    assert pw != "preserved-ro-pw"
    assert len(pw) >= 30


def test_resolve_ro_no_existing_file_uses_auto_generate(monkeypatch, tmp_path):
    """No `.ro_credentials` on disk → keep-existing prompt suppressed,
    auto-generate runs as before. Pins that the new branch is purely
    additive: fresh installs see the same UX they always did.
    """
    from social_data_pipeline.setup import db as db_setup

    prompts = []
    monkeypatch.setattr(
        db_setup, "ask_bool",
        lambda label, default, tag=None: prompts.append(tag) or default,
    )

    pw = db_setup._resolve_ro_password(
        "readonly",
        {"auth_enabled": True, "ro_username": "readonly", "pgdata_path": str(tmp_path)},
    )

    assert "db_ro_keep_existing" not in prompts
    assert "db_ro_auto_password" in prompts
    assert len(pw) >= 30


def test_resolve_ro_keep_declined_falls_through_to_auto_generate(monkeypatch, tmp_path):
    """Existing password present but user declines keep → auto-generate
    runs (covers the operator who explicitly wants a fresh password).
    """
    from social_data_pipeline.setup import db as db_setup

    pg_path = tmp_path / "pg"
    pg_path.mkdir()
    (pg_path / ".ro_credentials").write_text("preserved-ro-pw\n")

    answers = {"db_ro_keep_existing": False, "db_ro_auto_password": True}

    def fake_ask_bool(label, default, tag=None):
        return answers.get(tag, default)

    monkeypatch.setattr(db_setup, "ask_bool", fake_ask_bool)

    pw = db_setup._resolve_ro_password(
        "readonly",
        {
            "auth_enabled": True,
            "ro_username": "readonly",
            "pgdata_path": str(pg_path),
        },
    )

    assert pw != "preserved-ro-pw"
    assert len(pw) >= 30
