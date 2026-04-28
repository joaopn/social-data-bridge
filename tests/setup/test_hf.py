"""Unit tests for HuggingFace download organization (setup/hf.py).

Pure-logic tests for organize_hf_downloads — the function that copies
parquet files from data/dumps/<source>/<config>/<split>/<idx>.parquet to
data/extracted/<source>/<data_type>/<config>_<idx>.parquet (multi-config)
or .../<idx>.parquet (single-config).

Bug class:
  - filename mangling regressions (multi-config vs single-config branch)
  - skip-if-already-organized check (size-based) breaking
  - missing config dir produces a warning, not a crash
  - rglob finds files across split subdirectories
"""

from pathlib import Path

import pytest

from social_data_pipeline.setup.hf import organize_hf_downloads


def _write_parquet(path: Path, content: bytes = b"PAR1\x00\x00") -> None:
    """Drop a tiny placeholder file. organize_hf_downloads doesn't read parquet
    contents — only filenames and stat().st_size — so any bytes work."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)


def test_single_config_strips_config_name(tmp_path):
    """One config per data type → output names use just the index (no config prefix)."""
    dumps = tmp_path / "dumps"
    extracted = tmp_path / "extracted"
    _write_parquet(dumps / "default" / "train" / "0.parquet")
    _write_parquet(dumps / "default" / "train" / "1.parquet")

    organize_hf_downloads(dumps, extracted, {"comments": ["default"]})

    out = extracted / "comments"
    files = sorted(p.name for p in out.iterdir())
    assert files == ["0.parquet", "1.parquet"], f"got {files}"


def test_multi_config_prefixes_config_name(tmp_path):
    """Multiple configs per data type → output names prefix the config to disambiguate."""
    dumps = tmp_path / "dumps"
    extracted = tmp_path / "extracted"
    _write_parquet(dumps / "config_a" / "train" / "0.parquet")
    _write_parquet(dumps / "config_b" / "train" / "0.parquet")

    organize_hf_downloads(
        dumps, extracted,
        {"comments": ["config_a", "config_b"]},
    )

    out = extracted / "comments"
    files = sorted(p.name for p in out.iterdir())
    assert files == ["config_a_0.parquet", "config_b_0.parquet"], f"got {files}"


def test_walks_across_split_subdirs(tmp_path):
    """rglob picks up parquet files from any split directory, not just train/."""
    dumps = tmp_path / "dumps"
    extracted = tmp_path / "extracted"
    _write_parquet(dumps / "default" / "train" / "0.parquet")
    _write_parquet(dumps / "default" / "test" / "0.parquet")
    _write_parquet(dumps / "default" / "validation" / "0.parquet")

    organize_hf_downloads(dumps, extracted, {"comments": ["default"]})

    # 3 input files → 3 output files indexed 0..2 (sorted by source path).
    files = sorted(p.name for p in (extracted / "comments").iterdir())
    assert files == ["0.parquet", "1.parquet", "2.parquet"], f"got {files}"


def test_skips_when_already_present_with_matching_size(tmp_path):
    """Re-running on an already-organized tree is a no-op (size match → skip)."""
    dumps = tmp_path / "dumps"
    extracted = tmp_path / "extracted"
    src = dumps / "default" / "train" / "0.parquet"
    _write_parquet(src, b"some-bytes")

    organize_hf_downloads(dumps, extracted, {"comments": ["default"]})
    dest = extracted / "comments" / "0.parquet"
    assert dest.exists()
    first_mtime = dest.stat().st_mtime_ns

    # Re-run — file already there with same size; should skip.
    organize_hf_downloads(dumps, extracted, {"comments": ["default"]})
    assert dest.stat().st_mtime_ns == first_mtime, "file was rewritten on re-run"


def test_recopies_when_size_differs(tmp_path):
    """If dest size doesn't match src, the file is rewritten."""
    dumps = tmp_path / "dumps"
    extracted = tmp_path / "extracted"
    src = dumps / "default" / "train" / "0.parquet"
    _write_parquet(src, b"new-bytes-12345")

    # Pre-seed extracted with a different-size file at the destination.
    dest = extracted / "comments" / "0.parquet"
    _write_parquet(dest, b"old")
    assert dest.stat().st_size != src.stat().st_size

    organize_hf_downloads(dumps, extracted, {"comments": ["default"]})

    assert dest.stat().st_size == src.stat().st_size
    assert dest.read_bytes() == b"new-bytes-12345"


def test_missing_config_warns_and_continues(tmp_path, capsys):
    """A config listed in config_map but absent from dumps is warned, not raised."""
    dumps = tmp_path / "dumps"
    extracted = tmp_path / "extracted"
    _write_parquet(dumps / "present" / "train" / "0.parquet")

    organize_hf_downloads(
        dumps, extracted,
        {"comments": ["present", "missing"]},
    )

    captured = capsys.readouterr()
    assert "missing" in captured.out, f"expected warning about missing config: {captured.out!r}"

    # The present config still got organized (multi_config=True since two listed).
    files = sorted(p.name for p in (extracted / "comments").iterdir())
    assert files == ["present_0.parquet"], f"got {files}"


def test_creates_data_type_dir_when_absent(tmp_path):
    """extracted/<data_type>/ is created if it doesn't exist yet."""
    dumps = tmp_path / "dumps"
    extracted = tmp_path / "extracted"
    _write_parquet(dumps / "default" / "train" / "0.parquet")

    assert not (extracted / "comments").exists()

    organize_hf_downloads(dumps, extracted, {"comments": ["default"]})

    assert (extracted / "comments").is_dir()
    assert (extracted / "comments" / "0.parquet").is_file()


def test_multiple_data_types_routed_separately(tmp_path):
    """Different data_types route to different extracted/<dt>/ subdirs."""
    dumps = tmp_path / "dumps"
    extracted = tmp_path / "extracted"
    _write_parquet(dumps / "comments_cfg" / "train" / "0.parquet")
    _write_parquet(dumps / "subs_cfg" / "train" / "0.parquet")

    organize_hf_downloads(
        dumps, extracted,
        {
            "comments": ["comments_cfg"],
            "submissions": ["subs_cfg"],
        },
    )

    assert (extracted / "comments" / "0.parquet").exists()
    assert (extracted / "submissions" / "0.parquet").exists()
