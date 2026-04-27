"""Tests for the backend-shared helpers in jobs/backends/base.py."""

from __future__ import annotations

import pytest

from social_data_pipeline.jobs.backends.base import (
    BackendError,
    dir_size_bytes,
    strip_trailing_semicolon,
    validate_filename,
)


# ── validate_filename ───────────────────────────────────────────────────────


class TestValidateFilename:
    @pytest.mark.parametrize("name", [
        "out.parquet",
        "out.csv",
        "subset_2024-01.parquet",
        "A.csv",
        "0.parquet",
    ])
    def test_accepts_valid(self, name):
        ext = validate_filename(name)
        assert ext in (".parquet", ".csv")

    @pytest.mark.parametrize("name", [
        "../escape.parquet",
        "/abs/path.parquet",
        "sub/dir.parquet",
        "win\\style.csv",
        "",
        ".",
        "..",
    ])
    def test_rejects_path_traversal(self, name):
        with pytest.raises(BackendError):
            validate_filename(name)

    def test_rejects_bad_chars(self):
        with pytest.raises(BackendError, match="must match"):
            validate_filename("has space.parquet")
        with pytest.raises(BackendError, match="must match"):
            validate_filename("name with space.csv")

    def test_rejects_unknown_extension(self):
        with pytest.raises(BackendError, match="extension"):
            validate_filename("out.txt")

    def test_rejects_too_long(self):
        long_name = "a" * 200 + ".csv"
        with pytest.raises(BackendError, match="must match"):
            validate_filename(long_name)

    def test_custom_allowed_exts(self):
        ext = validate_filename("out.ndjson", allowed_exts=frozenset({".ndjson"}))
        assert ext == ".ndjson"
        with pytest.raises(BackendError):
            validate_filename("out.csv", allowed_exts=frozenset({".ndjson"}))


# ── strip_trailing_semicolon ────────────────────────────────────────────────


class TestStripTrailingSemicolon:
    def test_no_semicolon(self):
        assert strip_trailing_semicolon("SELECT 1") == "SELECT 1"

    def test_single_trailing(self):
        assert strip_trailing_semicolon("SELECT 1;") == "SELECT 1"

    def test_multiple_trailing(self):
        assert strip_trailing_semicolon("SELECT 1;;;  ") == "SELECT 1"

    def test_strips_outer_whitespace(self):
        assert strip_trailing_semicolon("\n SELECT 1 \n") == "SELECT 1"

    def test_empty_after_strip_raises(self):
        with pytest.raises(BackendError, match="empty"):
            strip_trailing_semicolon("   ")
        with pytest.raises(BackendError, match="empty"):
            strip_trailing_semicolon(";;;")


# ── dir_size_bytes ──────────────────────────────────────────────────────────


class TestDirSizeBytes:
    def test_empty_dir(self, tmp_path):
        assert dir_size_bytes(tmp_path) == 0

    def test_single_file(self, tmp_path):
        (tmp_path / "f.bin").write_bytes(b"hello")
        assert dir_size_bytes(tmp_path) == 5

    def test_recursive(self, tmp_path):
        (tmp_path / "a.bin").write_bytes(b"123")
        sub = tmp_path / "sub"
        sub.mkdir()
        (sub / "b.bin").write_bytes(b"45")
        (sub / "c.bin").write_bytes(b"678")
        assert dir_size_bytes(tmp_path) == 8

    def test_skips_missing_file_during_walk(self, tmp_path):
        # Race-tolerance: dir_size_bytes catches FileNotFoundError so a file
        # that disappears mid-walk doesn't break sizing.
        (tmp_path / "f.bin").write_bytes(b"abc")
        # No way to deterministically race here, but verify the path returns
        # a sane integer for a normal file.
        assert dir_size_bytes(tmp_path) == 3
