"""Tests for the .ro_credentials file format and host-side helpers.

The file is the password store only — the username lives authoritatively in
config/db/<db>.yaml. Earlier versions stored `username:password` on a single
line; this file pins:

- the new write format (password-only, mode 0600),
- atomicity (write goes through `.tmp` + rename; rename failure leaves the
  original file intact and the temp removed),
- the read helper that returns just the password,
- in-place migration from the legacy `username:password` format,
- loud failure: when yaml says `auth_enabled=true`, an unreadable / empty /
  malformed cred file raises `ConfigurationError` instead of silently
  returning `None`.

Bug class: silent drift between yaml and the file (returning the wrong
username), chown-induced unreadability of the host file, and partial-write
states left behind by a crashed setup run. Migration must not fail loudly
on legacy installs — the file is rewritten on first read.
"""

from __future__ import annotations

import os
import stat

import pytest

from social_data_pipeline.core.config import ConfigurationError
from social_data_pipeline.setup.db import (
    _read_existing_ro_password,
    _write_ro_credentials,
    _write_ro_password_for,
)


def test_write_password_only_format(tmp_path):
    """_write_ro_credentials writes single-line password (no `:` prefix)."""
    settings = {
        "ro_username": "readonly",  # ignored on disk now — yaml is authoritative
        "ro_password": "s3cret-pw",
        "pgdata_path": str(tmp_path),
    }

    written = _write_ro_credentials(settings)

    cred_file = tmp_path / ".ro_credentials"
    assert cred_file.exists()
    assert str(cred_file) in written

    content = cred_file.read_text()
    # Single line, password only, with trailing newline.
    assert content == "s3cret-pw\n"
    # The legacy `username:` prefix must not appear.
    assert "readonly:" not in content


def test_write_sets_mode_0600(tmp_path):
    """File mode is 0600 (chmod 600), readable by owner only."""
    settings = {
        "ro_username": "readonly",
        "ro_password": "pw",
        "pgdata_path": str(tmp_path),
    }

    _write_ro_credentials(settings)

    cred_file = tmp_path / ".ro_credentials"
    mode = stat.S_IMODE(os.stat(cred_file).st_mode)
    assert mode == 0o600, f"expected mode 0o600, got {oct(mode)}"


def test_read_returns_password_string_for_new_format(tmp_path):
    """_read_existing_ro_password returns just the password for new-format files."""
    cred_file = tmp_path / ".ro_credentials"
    cred_file.write_text("s3cret-pw\n")

    result = _read_existing_ro_password({"pgdata_path": str(tmp_path)})

    assert result == "s3cret-pw"


def test_read_returns_none_when_file_missing(tmp_path):
    """When no `.ro_credentials` is present anywhere, return None.

    Loud-failure behavior is reserved for callers that have already
    asserted auth is enabled; this helper preserves the silent-None
    contract for callers that can cope with a missing file (e.g.,
    reconfigure paths where the password is re-prompted).
    """
    result = _read_existing_ro_password({"pgdata_path": str(tmp_path)})
    assert result is None


def test_read_migrates_legacy_username_password_format(tmp_path):
    """Legacy `username:password\\n` files are converted in-place on read.

    The first read returns the password, and the file on disk is rewritten
    to the new password-only format so subsequent reads (and entrypoints)
    see consistent contents.
    """
    cred_file = tmp_path / ".ro_credentials"
    cred_file.write_text("readonly:s3cret-pw\n")
    os.chmod(cred_file, 0o600)

    first = _read_existing_ro_password({"pgdata_path": str(tmp_path)})
    assert first == "s3cret-pw"

    # File on disk is now password-only.
    on_disk = cred_file.read_text()
    assert on_disk == "s3cret-pw\n"
    assert ":" not in on_disk

    # Mode preserved at 0600 after rewrite.
    mode = stat.S_IMODE(os.stat(cred_file).st_mode)
    assert mode == 0o600

    # Second read returns the same password from the now-converted file.
    second = _read_existing_ro_password({"pgdata_path": str(tmp_path)})
    assert second == "s3cret-pw"


def test_read_skips_empty_files_when_auth_off(tmp_path):
    """Empty `.ro_credentials` returns None when auth is not enabled.

    The auth-off case stays silent — the operator may simply have an old
    truncated file lying around from a prior unsetup. Loud-failure under
    auth is exercised in `test_read_raises_on_empty_file_when_auth_on`.
    """
    cred_file = tmp_path / ".ro_credentials"
    cred_file.write_text("")

    result = _read_existing_ro_password({"pgdata_path": str(tmp_path)})
    assert result is None


def test_round_trip_write_then_read(tmp_path):
    """write → read returns the same password byte-for-byte."""
    settings = {
        "ro_username": "readonly",
        "ro_password": "complex-p@ssw0rd_with-special.chars",
        "pgdata_path": str(tmp_path),
    }

    _write_ro_credentials(settings)
    result = _read_existing_ro_password({"pgdata_path": str(tmp_path)})

    assert result == "complex-p@ssw0rd_with-special.chars"


def test_write_creates_files_for_each_data_path(tmp_path):
    """A multi-DB setup writes one .ro_credentials per configured data path."""
    pg_path = tmp_path / "pg"
    mongo_path = tmp_path / "mongo"
    sr_path = tmp_path / "sr"
    pg_path.mkdir()
    mongo_path.mkdir()
    sr_path.mkdir()

    settings = {
        "ro_username": "readonly",
        "ro_password": "shared-pw",
        "pgdata_path": str(pg_path),
        "mongo_data_path": str(mongo_path),
        "starrocks_data_path": str(sr_path),
    }

    written = _write_ro_credentials(settings)

    assert len(written) == 3
    for p in (pg_path, mongo_path, sr_path):
        assert (p / ".ro_credentials").read_text() == "shared-pw\n"


# ── Per-DB atomic write ────────────────────────────────────────────────────


def test_write_per_db_atomic_no_tmp_left_behind(tmp_path):
    """`_write_ro_password_for` leaves no `.tmp` after a successful write."""
    _write_ro_password_for("postgres", tmp_path, "pw")

    assert (tmp_path / ".ro_credentials").read_text() == "pw\n"
    # Atomic step's temp file must not survive a successful write.
    assert not (tmp_path / ".ro_credentials.tmp").exists()


def test_write_per_db_atomic_rollback_on_rename_failure(tmp_path, monkeypatch):
    """Rename failure mid-write: original (if any) intact, .tmp cleaned up.

    Pins atomicity: the function must not leave a half-written `.tmp`
    behind that would either confuse a future `db setup` (stale temp seen
    as "in flight") or get picked up as the real cred file by mistake.
    """
    cred_file = tmp_path / ".ro_credentials"
    tmp_file = tmp_path / ".ro_credentials.tmp"

    # Pre-existing valid file that must survive the failed rename.
    cred_file.write_text("original-pw\n")
    os.chmod(cred_file, 0o600)

    def _failing_replace(src, dst):
        raise OSError("simulated rename failure")

    monkeypatch.setattr("os.replace", _failing_replace)

    with pytest.raises(OSError, match="simulated rename failure"):
        _write_ro_password_for("postgres", tmp_path, "new-pw")

    # Original file untouched.
    assert cred_file.read_text() == "original-pw\n"
    # Temp cleaned up — no half-written state visible to future runs.
    assert not tmp_file.exists()


def test_write_per_db_repairs_mode_after_rename(tmp_path, monkeypatch):
    """Post-rename mode assertion re-applies 0600 if the FS strips it.

    Some filesystems and overlays silently strip mode bits during rename.
    The function must verify the destination's final mode and chmod again
    if it doesn't already match — defense-in-depth, cheap to test.
    """
    real_replace = os.replace

    def _replace_then_clobber_mode(src, dst):
        real_replace(src, dst)
        # Simulate a filesystem that drops the chmod.
        os.chmod(dst, 0o644)

    monkeypatch.setattr("os.replace", _replace_then_clobber_mode)

    _write_ro_password_for("postgres", tmp_path, "pw")

    cred_file = tmp_path / ".ro_credentials"
    mode = stat.S_IMODE(os.stat(cred_file).st_mode)
    assert mode == 0o600


def test_write_per_db_cleans_stale_tmp(tmp_path):
    """A stale `.ro_credentials.tmp` from a crashed prior run is removed.

    Pins that the function does not refuse / fail when an orphan `.tmp` is
    sitting in the data dir from a prior crash — it overwrites it as part
    of the new atomic write.
    """
    stale = tmp_path / ".ro_credentials.tmp"
    stale.write_text("garbage from a crashed run\n")

    _write_ro_password_for("postgres", tmp_path, "pw")

    assert (tmp_path / ".ro_credentials").read_text() == "pw\n"
    assert not stale.exists()


# ── Read raises loud under auth ────────────────────────────────────────────


def test_read_raises_on_empty_file_when_auth_on(tmp_path):
    """Empty `.ro_credentials` raises `ConfigurationError` under auth.

    Bug class: silent-None on read let setup proceed with auth_enabled=True
    but never re-write the file, leaving the entrypoints to fail at boot.
    Now it fails at setup time with a fix hint.
    """
    cred_file = tmp_path / ".ro_credentials"
    cred_file.write_text("")

    with pytest.raises(ConfigurationError, match="empty"):
        _read_existing_ro_password({
            "auth_enabled": True,
            "pgdata_path": str(tmp_path),
        })


def test_read_raises_on_oserror_when_auth_on(tmp_path, monkeypatch):
    """Unreadable `.ro_credentials` raises `ConfigurationError` under auth.

    Simulates the chown-trap class of failures: file exists but the host
    cannot read it (permission, IO error). Under auth the read MUST fail
    loudly so the operator can run `recover-password --regenerate-ro`
    instead of silently proceeding with no creds.
    """
    cred_file = tmp_path / ".ro_credentials"
    cred_file.write_text("real-pw\n")

    real_read = type(cred_file).read_text

    def _failing_read(self, *args, **kwargs):
        if self == cred_file:
            raise PermissionError("simulated chown trap")
        return real_read(self, *args, **kwargs)

    monkeypatch.setattr("pathlib.Path.read_text", _failing_read)

    with pytest.raises(ConfigurationError, match="Cannot read"):
        _read_existing_ro_password({
            "auth_enabled": True,
            "pgdata_path": str(tmp_path),
        })


def test_read_silent_none_on_oserror_when_auth_off(tmp_path, monkeypatch):
    """Same OSError, but auth disabled → silent None (legacy / pre-auth)."""
    cred_file = tmp_path / ".ro_credentials"
    cred_file.write_text("real-pw\n")

    real_read = type(cred_file).read_text

    def _failing_read(self, *args, **kwargs):
        if self == cred_file:
            raise PermissionError("simulated chown trap")
        return real_read(self, *args, **kwargs)

    monkeypatch.setattr("pathlib.Path.read_text", _failing_read)

    # No auth_enabled flag → silent skip path. The function neither raises
    # nor returns garbage; the operator just doesn't get a password.
    result = _read_existing_ro_password({"pgdata_path": str(tmp_path)})
    assert result is None


def test_read_raises_on_legacy_empty_password_when_auth_on(tmp_path):
    """Legacy `username:` (no password segment) raises under auth."""
    cred_file = tmp_path / ".ro_credentials"
    cred_file.write_text("readonly:\n")

    with pytest.raises(ConfigurationError, match="legacy"):
        _read_existing_ro_password({
            "auth_enabled": True,
            "pgdata_path": str(tmp_path),
        })


def test_read_no_file_returns_none_even_with_auth_on(tmp_path):
    """When no `.ro_credentials` exists at all, the read silently returns None.

    The "no creds yet" case is legitimate during initial `db setup --add`
    where the file will be written immediately after the read attempts to
    seed `settings["ro_password"]` from existing creds. Loud-failure here
    is the wrong shape; the loud-failure case is "file exists but unusable."
    """
    result = _read_existing_ro_password({
        "auth_enabled": True,
        "pgdata_path": str(tmp_path),
    })
    assert result is None
