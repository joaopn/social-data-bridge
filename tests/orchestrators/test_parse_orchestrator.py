"""Tests for parse orchestrator file detection functions."""

from pathlib import Path


from social_data_pipeline.orchestrators.parse import (
    detect_dump_files,
    detect_json_files,
    detect_parsed_files,
    detect_parquet_input_files,
    get_file_identifier,
)


# Shared reddit-style file patterns
REDDIT_FILE_PATTERNS = {
    "comments": {
        "dump": r"^RC_(\d{4}-\d{2})\..+$",
        "json": r"^RC_(\d{4}-\d{2})$",
        "csv": r"^RC_(\d{4}-\d{2})\.csv$",
        "parquet": r"^RC_(\d{4}-\d{2})\.parquet$",
    },
    "submissions": {
        "dump": r"^RS_(\d{4}-\d{2})\..+$",
        "json": r"^RS_(\d{4}-\d{2})$",
        "csv": r"^RS_(\d{4}-\d{2})\.csv$",
        "parquet": r"^RS_(\d{4}-\d{2})\.parquet$",
    },
}

DATA_TYPES = ["comments", "submissions"]


# ============================================================================
# detect_dump_files
# ============================================================================


class TestDetectDumpFiles:
    def test_finds_zst_files(self, tmp_path):
        (tmp_path / "comments").mkdir()
        (tmp_path / "comments" / "RC_2024-01.zst").touch()
        (tmp_path / "comments" / "RC_2024-02.zst").touch()

        result = detect_dump_files(str(tmp_path), DATA_TYPES, REDDIT_FILE_PATTERNS)
        assert len(result) == 2
        filenames = [Path(p).name for p, _ in result]
        assert "RC_2024-01.zst" in filenames
        assert "RC_2024-02.zst" in filenames

    def test_finds_gz_files(self, tmp_path):
        (tmp_path / "submissions").mkdir()
        (tmp_path / "submissions" / "RS_2024-03.json.gz").touch()

        result = detect_dump_files(str(tmp_path), DATA_TYPES, REDDIT_FILE_PATTERNS)
        assert len(result) == 1
        assert result[0][1] == "submissions"

    def test_ignores_non_matching_files(self, tmp_path):
        (tmp_path / "comments").mkdir()
        (tmp_path / "comments" / "RC_2024-01.zst").touch()
        (tmp_path / "comments" / "readme.txt").touch()
        (tmp_path / "comments" / "random.zst").touch()

        result = detect_dump_files(str(tmp_path), DATA_TYPES, REDDIT_FILE_PATTERNS)
        assert len(result) == 1

    def test_empty_dir(self, tmp_path):
        (tmp_path / "comments").mkdir()
        result = detect_dump_files(str(tmp_path), DATA_TYPES, REDDIT_FILE_PATTERNS)
        assert result == []

    def test_no_data_type_dir(self, tmp_path):
        # No subdirectories at all
        result = detect_dump_files(str(tmp_path), DATA_TYPES, REDDIT_FILE_PATTERNS)
        assert result == []

    def test_sorted_by_data_type_then_filename(self, tmp_path):
        (tmp_path / "comments").mkdir()
        (tmp_path / "submissions").mkdir()
        (tmp_path / "comments" / "RC_2024-02.zst").touch()
        (tmp_path / "comments" / "RC_2024-01.zst").touch()
        (tmp_path / "submissions" / "RS_2024-01.zst").touch()

        result = detect_dump_files(str(tmp_path), DATA_TYPES, REDDIT_FILE_PATTERNS)
        assert len(result) == 3
        # comments first (index 0 in data_types), then submissions
        assert result[0][1] == "comments"
        assert result[1][1] == "comments"
        assert result[2][1] == "submissions"
        # Within comments, sorted by filename
        assert Path(result[0][0]).name == "RC_2024-01.zst"
        assert Path(result[1][0]).name == "RC_2024-02.zst"


# ============================================================================
# detect_json_files
# ============================================================================


class TestDetectJsonFiles:
    def test_finds_json_files(self, tmp_path):
        (tmp_path / "comments").mkdir()
        (tmp_path / "comments" / "RC_2024-01").touch()
        (tmp_path / "comments" / "RC_2024-02").touch()

        result = detect_json_files(str(tmp_path), DATA_TYPES, REDDIT_FILE_PATTERNS)
        assert len(result) == 2
        # Returns (filepath, file_id, data_type)
        file_ids = [fid for _, fid, _ in result]
        assert "RC_2024-01" in file_ids
        assert "RC_2024-02" in file_ids

    def test_ignores_csv_files(self, tmp_path):
        (tmp_path / "comments").mkdir()
        (tmp_path / "comments" / "RC_2024-01").touch()
        (tmp_path / "comments" / "RC_2024-01.csv").touch()

        result = detect_json_files(str(tmp_path), DATA_TYPES, REDDIT_FILE_PATTERNS)
        assert len(result) == 1

    def test_returns_data_type(self, tmp_path):
        (tmp_path / "submissions").mkdir()
        (tmp_path / "submissions" / "RS_2024-05").touch()

        result = detect_json_files(str(tmp_path), DATA_TYPES, REDDIT_FILE_PATTERNS)
        assert len(result) == 1
        assert result[0][2] == "submissions"


# ============================================================================
# detect_parsed_files
# ============================================================================


class TestDetectParsedFiles:
    def test_detect_csv_files(self, tmp_path):
        (tmp_path / "comments").mkdir()
        (tmp_path / "comments" / "RC_2024-01.csv").touch()

        result = detect_parsed_files(str(tmp_path), DATA_TYPES, REDDIT_FILE_PATTERNS, file_format="csv")
        assert len(result) == 1
        assert result[0][2] == "comments"

    def test_detect_parquet_files(self, tmp_path):
        (tmp_path / "submissions").mkdir()
        (tmp_path / "submissions" / "RS_2024-01.parquet").touch()

        result = detect_parsed_files(str(tmp_path), DATA_TYPES, REDDIT_FILE_PATTERNS, file_format="parquet")
        assert len(result) == 1
        assert result[0][2] == "submissions"

    def test_csv_format_ignores_parquet(self, tmp_path):
        (tmp_path / "comments").mkdir()
        (tmp_path / "comments" / "RC_2024-01.parquet").touch()

        result = detect_parsed_files(str(tmp_path), DATA_TYPES, REDDIT_FILE_PATTERNS, file_format="csv")
        assert len(result) == 0

    def test_file_id_is_stem(self, tmp_path):
        (tmp_path / "comments").mkdir()
        (tmp_path / "comments" / "RC_2024-06.csv").touch()

        result = detect_parsed_files(str(tmp_path), DATA_TYPES, REDDIT_FILE_PATTERNS, file_format="csv")
        assert result[0][1] == "RC_2024-06"


# ============================================================================
# detect_parquet_input_files
# ============================================================================


class TestDetectParquetInputFiles:
    def test_finds_parquet_in_extracted(self, tmp_path):
        (tmp_path / "comments").mkdir()
        (tmp_path / "comments" / "RC_2024-01.parquet").touch()

        result = detect_parquet_input_files(str(tmp_path), DATA_TYPES, REDDIT_FILE_PATTERNS)
        assert len(result) == 1
        assert result[0][1] == "RC_2024-01"  # file_id = stem
        assert result[0][2] == "comments"

    def test_ignores_non_parquet(self, tmp_path):
        (tmp_path / "comments").mkdir()
        (tmp_path / "comments" / "RC_2024-01.csv").touch()
        (tmp_path / "comments" / "RC_2024-01").touch()

        result = detect_parquet_input_files(str(tmp_path), DATA_TYPES, REDDIT_FILE_PATTERNS)
        assert len(result) == 0


# ============================================================================
# get_file_identifier
# ============================================================================


class TestGetFileIdentifier:
    def test_zst_extension(self):
        assert get_file_identifier("/path/to/RC_2024-01.zst") == "RC_2024-01"

    def test_json_gz_extension(self):
        # strip_compression_extension: "data.json.gz" -> "data.json"
        # then get_file_identifier strips .json -> "data"
        assert get_file_identifier("/path/to/data.json.gz") == "data"

    def test_gz_extension(self):
        assert get_file_identifier("/path/to/data.gz") == "data"

    def test_xz_extension(self):
        assert get_file_identifier("/path/to/RC_2024-03.xz") == "RC_2024-03"

    def test_tar_gz_extension(self):
        assert get_file_identifier("/path/to/archive.tar.gz") == "archive"

    def test_uncompressed_file(self):
        # Not compressed: returns Path(filepath).stem
        assert get_file_identifier("/path/to/RC_2024-01") == "RC_2024-01"

    def test_uncompressed_csv(self):
        # Not compressed: returns Path(filepath).stem
        assert get_file_identifier("/path/to/RC_2024-01.csv") == "RC_2024-01"

    def test_csv_gz(self):
        # strip_compression_extension: "data.csv.gz" -> "data.csv"
        # then .csv is stripped -> "data"
        assert get_file_identifier("/path/to/data.csv.gz") == "data"
