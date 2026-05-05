"""Tests for the .ro_credentials file format and host-side helpers.

The file is the password store only — the username lives authoritatively in
config/db/<db>.yaml. Earlier versions stored `username:password` on a single
line; this file pins:

- the new write format (password-only, mode 0600),
- the read helper that returns just the password,
- in-place migration from the legacy `username:password` format.

Bug class: silent drift between yaml and the file (returning the wrong
username) and chown-induced unreadability of the host file. Migration must
not fail loudly on legacy installs — the file is rewritten on first read.
"""

from __future__ import annotations

import os
import stat

from social_data_pipeline.setup.db import (
    _read_existing_ro_password,
    _write_ro_credentials,
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

    Loud-failure behavior is added in Commit 3 (host-side); this commit
    preserves the silent-None contract for callers that can cope with a
    missing file (e.g., reconfigure paths where the password is re-prompted).
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


def test_read_skips_empty_files(tmp_path):
    """An empty `.ro_credentials` is treated as no readable file.

    Loud-failure for empty files is added in Commit 3. Here we just pin
    that the function does not return a stray empty string from a truncated
    file (which would let the caller proceed thinking it had a password).
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
