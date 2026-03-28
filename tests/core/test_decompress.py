"""Tests for social_data_pipeline.core.decompress."""

import gzip
import lzma
import tarfile
import subprocess
import pytest
from pathlib import Path

from social_data_pipeline.core.decompress import (
    detect_compression,
    strip_compression_extension,
    is_compressed,
    decompress_file,
)


# ── detect_compression ──────────────────────────────────────────────────────

class TestDetectCompression:
    def test_zst(self):
        assert detect_compression("data.zst") == "zst"

    def test_gz(self):
        assert detect_compression("data.gz") == "gz"

    def test_json_gz(self):
        assert detect_compression("RC_2024-01.ndjson.gz") == "gz"

    def test_xz(self):
        assert detect_compression("data.xz") == "xz"

    def test_tar_gz(self):
        assert detect_compression("archive.tar.gz") == "tar.gz"

    def test_tgz(self):
        assert detect_compression("archive.tgz") == "tar.gz"

    def test_case_insensitive(self):
        assert detect_compression("FILE.ZST") == "zst"
        assert detect_compression("File.Tar.Gz") == "tar.gz"

    def test_unrecognized(self):
        assert detect_compression("data.csv") is None
        assert detect_compression("data.parquet") is None

    def test_no_extension(self):
        assert detect_compression("README") is None


# ── strip_compression_extension ─────────────────────────────────────────────

class TestStripCompressionExtension:
    def test_zst(self):
        assert strip_compression_extension("RC_2024-01.zst") == "RC_2024-01"

    def test_gz(self):
        assert strip_compression_extension("data.gz") == "data"

    def test_json_gz_keeps_json(self):
        assert strip_compression_extension("RC_2024-01.ndjson.gz") == "RC_2024-01.ndjson"

    def test_xz(self):
        assert strip_compression_extension("data.xz") == "data"

    def test_tar_gz(self):
        assert strip_compression_extension("archive.tar.gz") == "archive"

    def test_tgz(self):
        assert strip_compression_extension("archive.tgz") == "archive"

    def test_no_ext(self):
        assert strip_compression_extension("plain") == "plain"


# ── is_compressed ───────────────────────────────────────────────────────────

class TestIsCompressed:
    def test_compressed(self):
        assert is_compressed("data.zst") is True
        assert is_compressed("data.gz") is True

    def test_not_compressed(self):
        assert is_compressed("data.csv") is False
        assert is_compressed("data.parquet") is False


# ── decompress_file (real decompression) ────────────────────────────────────

@pytest.mark.slow
class TestDecompressGz:
    def test_decompress_gz(self, tmp_path):
        content = b"hello world\n"
        gz_path = tmp_path / "test.ndjson.gz"
        with gzip.open(gz_path, "wb") as f:
            f.write(content)
        out_dir = tmp_path / "out"
        result = decompress_file(str(gz_path), str(out_dir))
        result_path = Path(result)
        assert result_path.exists()
        # .ndjson.gz -> strips .gz, keeps .ndjson -> "test.ndjson"
        assert result_path.name == "test.ndjson"
        assert result_path.read_bytes() == content

    def test_decompress_plain_gz(self, tmp_path):
        content = b"plain gzip data\n"
        gz_path = tmp_path / "data.gz"
        with gzip.open(gz_path, "wb") as f:
            f.write(content)
        out_dir = tmp_path / "out"
        result = decompress_file(str(gz_path), str(out_dir))
        assert Path(result).read_bytes() == content


@pytest.mark.slow
class TestDecompressXz:
    def test_decompress_xz(self, tmp_path):
        content = b"xz compressed content\n"
        xz_path = tmp_path / "data.xz"
        with lzma.open(xz_path, "wb") as f:
            f.write(content)
        out_dir = tmp_path / "out"
        result = decompress_file(str(xz_path), str(out_dir))
        assert Path(result).read_bytes() == content
        assert Path(result).name == "data"


@pytest.mark.slow
class TestDecompressTarGz:
    def test_decompress_tar_gz(self, tmp_path):
        # Create a tar.gz with one file inside
        src_file = tmp_path / "inner.txt"
        src_file.write_text("tar content")
        tar_path = tmp_path / "archive.tar.gz"
        with tarfile.open(tar_path, "w:gz") as tar:
            tar.add(src_file, arcname="inner.txt")
        out_dir = tmp_path / "out"
        result = decompress_file(str(tar_path), str(out_dir))
        assert (Path(result) / "inner.txt").read_text() == "tar content"


@pytest.mark.slow
class TestDecompressZst:
    @pytest.fixture(autouse=True)
    def _check_zstd(self):
        try:
            subprocess.run(["zstd", "--version"], capture_output=True, check=True)
        except (FileNotFoundError, subprocess.CalledProcessError):
            pytest.skip("zstd not installed")

    def test_decompress_zst(self, tmp_path):
        content = b"zstd compressed content\n"
        src = tmp_path / "data"
        src.write_bytes(content)
        zst_path = tmp_path / "data.zst"
        subprocess.run(["zstd", str(src), "-o", str(zst_path)], check=True, capture_output=True)
        src.unlink()  # remove original

        out_dir = tmp_path / "out"
        result = decompress_file(str(zst_path), str(out_dir))
        assert Path(result).read_bytes() == content


# ── decompress_file with fixture ────────────────────────────────────────────

@pytest.mark.slow
class TestDecompressFixture:
    def test_decompress_reddit_fixture(self, tmp_path, reddit_fixtures_dir):
        """Decompress the RC_2024-01.ndjson fixture via gzip."""
        # Create a gzipped version of the fixture
        src = reddit_fixtures_dir / "RC_2024-01.ndjson"
        if not src.exists():
            pytest.skip("reddit fixture RC_2024-01.ndjson not found")
        gz_path = tmp_path / "RC_2024-01.ndjson.gz"
        with open(src, "rb") as f_in, gzip.open(gz_path, "wb") as f_out:
            f_out.write(f_in.read())
        out_dir = tmp_path / "out"
        result = decompress_file(str(gz_path), str(out_dir))
        result_path = Path(result)
        assert result_path.exists()
        assert result_path.stat().st_size == src.stat().st_size


# ── Error cases ─────────────────────────────────────────────────────────────

class TestDecompressErrors:
    def test_unrecognized_format(self, tmp_path):
        f = tmp_path / "data.csv"
        f.write_text("a,b,c")
        with pytest.raises(ValueError, match="Unrecognized compression format"):
            decompress_file(str(f), str(tmp_path / "out"))

    def test_file_not_found_gz(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            decompress_file(str(tmp_path / "missing.gz"), str(tmp_path / "out"))

    def test_file_not_found_unrecognized(self, tmp_path):
        with pytest.raises(ValueError, match="Unrecognized"):
            decompress_file(str(tmp_path / "missing.csv"), str(tmp_path / "out"))
