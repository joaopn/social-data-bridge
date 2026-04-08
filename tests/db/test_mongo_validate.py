"""Tests for MongoDB pre-import file validation."""

import json
import pytest

from social_data_pipeline.db.mongo.ingest import (
    _validate_ndjson_tail,
    _validate_ndjson_full,
    _validate_csv_tail,
    validate_file,
)


# ── NDJSON tail validation ─────────────────────────────────────────────────


class TestValidateNdjsonTail:
    def test_valid_file(self, tmp_path):
        f = tmp_path / "valid.ndjson"
        f.write_text('{"a": 1}\n{"b": 2}\n')
        _validate_ndjson_tail(str(f))

    def test_valid_single_line(self, tmp_path):
        f = tmp_path / "single.ndjson"
        f.write_text('{"a": 1}\n')
        _validate_ndjson_tail(str(f))

    def test_truncated_mid_object(self, tmp_path):
        f = tmp_path / "truncated.ndjson"
        f.write_text('{"a": 1}\n{"b": 2, "c":')
        with pytest.raises(ValueError, match="not valid JSON"):
            _validate_ndjson_tail(str(f))

    def test_empty_file(self, tmp_path):
        f = tmp_path / "empty.ndjson"
        f.write_text('')
        with pytest.raises(ValueError, match="Empty file"):
            _validate_ndjson_tail(str(f))

    def test_only_whitespace(self, tmp_path):
        f = tmp_path / "whitespace.ndjson"
        f.write_text('\n\n\n')
        with pytest.raises(ValueError, match="no non-empty lines"):
            _validate_ndjson_tail(str(f))

    def test_file_smaller_than_8kb(self, tmp_path):
        f = tmp_path / "small.ndjson"
        lines = [json.dumps({"i": i}) for i in range(10)]
        f.write_text('\n'.join(lines) + '\n')
        _validate_ndjson_tail(str(f))

    def test_file_larger_than_8kb(self, tmp_path):
        f = tmp_path / "large.ndjson"
        # Each line ~30 bytes, need >8192 bytes → ~300 lines
        lines = [json.dumps({"index": i, "data": "x" * 20}) for i in range(400)]
        f.write_text('\n'.join(lines) + '\n')
        _validate_ndjson_tail(str(f))

    def test_large_file_truncated(self, tmp_path):
        f = tmp_path / "large_trunc.ndjson"
        lines = [json.dumps({"index": i, "data": "x" * 20}) for i in range(400)]
        content = '\n'.join(lines) + '\n{"incomplete": true, "cut_off'
        f.write_text(content)
        with pytest.raises(ValueError, match="not valid JSON"):
            _validate_ndjson_tail(str(f))

    def test_malformed_middle_passes_tail(self, tmp_path):
        """Tail validation does not catch malformed lines in the middle."""
        f = tmp_path / "bad_middle.ndjson"
        f.write_text('{"a": 1}\nNOT_JSON\n{"b": 2}\n')
        _validate_ndjson_tail(str(f))  # Should pass


# ── NDJSON full validation ─────────────────────────────────────────────────


class TestValidateNdjsonFull:
    def test_valid_file(self, tmp_path):
        f = tmp_path / "valid.ndjson"
        f.write_text('{"a": 1}\n{"b": 2}\n')
        _validate_ndjson_full(str(f))

    def test_valid_single_line(self, tmp_path):
        f = tmp_path / "single.ndjson"
        f.write_text('{"a": 1}\n')
        _validate_ndjson_full(str(f))

    def test_truncated_mid_object(self, tmp_path):
        f = tmp_path / "truncated.ndjson"
        f.write_text('{"a": 1}\n{"b": 2, "c":')
        with pytest.raises(ValueError, match="Invalid JSON at line 2"):
            _validate_ndjson_full(str(f))

    def test_malformed_middle(self, tmp_path):
        f = tmp_path / "bad_middle.ndjson"
        f.write_text('{"a": 1}\nNOT_JSON\n{"b": 2}\n')
        with pytest.raises(ValueError, match="Invalid JSON at line 2"):
            _validate_ndjson_full(str(f))

    def test_empty_file(self, tmp_path):
        f = tmp_path / "empty.ndjson"
        f.write_text('')
        with pytest.raises(ValueError, match="Empty file"):
            _validate_ndjson_full(str(f))

    def test_only_whitespace(self, tmp_path):
        f = tmp_path / "whitespace.ndjson"
        f.write_text('\n\n\n')
        with pytest.raises(ValueError, match="no non-empty lines"):
            _validate_ndjson_full(str(f))

    def test_blank_lines_between_valid(self, tmp_path):
        f = tmp_path / "blanks.ndjson"
        f.write_text('{"a": 1}\n\n{"b": 2}\n\n')
        _validate_ndjson_full(str(f))

    def test_error_reports_line_number(self, tmp_path):
        f = tmp_path / "line5.ndjson"
        lines = [json.dumps({"i": i}) for i in range(4)]
        lines.append('BROKEN')
        lines.append(json.dumps({"i": 5}))
        f.write_text('\n'.join(lines) + '\n')
        with pytest.raises(ValueError, match="line 5"):
            _validate_ndjson_full(str(f))


# ── CSV tail validation ────────────────────────────────────────────────────


class TestValidateCsvTail:
    def test_valid_csv(self, tmp_path):
        f = tmp_path / "valid.csv"
        f.write_text('a,b,c\n1,2,3\n4,5,6\n')
        _validate_csv_tail(str(f))

    def test_truncated_no_newline(self, tmp_path):
        f = tmp_path / "trunc.csv"
        f.write_text('a,b,c\n1,2,3\n4,5')
        with pytest.raises(ValueError, match="does not end with newline"):
            _validate_csv_tail(str(f))

    def test_truncated_field_count(self, tmp_path):
        f = tmp_path / "fields.csv"
        f.write_text('a,b,c\n1,2,3\n4,5\n')
        with pytest.raises(ValueError, match="last line has 2"):
            _validate_csv_tail(str(f))

    def test_empty_file(self, tmp_path):
        f = tmp_path / "empty.csv"
        f.write_text('')
        with pytest.raises(ValueError, match="Empty file"):
            _validate_csv_tail(str(f))

    def test_empty_header(self, tmp_path):
        f = tmp_path / "no_header.csv"
        f.write_text('\n1,2,3\n')
        with pytest.raises(ValueError, match="empty header"):
            _validate_csv_tail(str(f))


# ── validate_file dispatcher ──────────────────────────────────────────────


class TestValidateFile:
    def test_none_mode_skips(self, tmp_path):
        f = tmp_path / "bad.ndjson"
        f.write_text('NOT_JSON')
        validate_file(str(f), mode="none")  # Should not raise

    def test_csv_dispatch(self, tmp_path):
        f = tmp_path / "valid.csv"
        f.write_text('a,b\n1,2\n')
        validate_file(str(f), mode="full")  # CSV uses tail check regardless

    def test_ndjson_full_dispatch(self, tmp_path):
        f = tmp_path / "bad.ndjson"
        f.write_text('{"a": 1}\nBAD\n{"b": 2}\n')
        with pytest.raises(ValueError, match="Invalid JSON"):
            validate_file(str(f), mode="full")

    def test_ndjson_tail_dispatch(self, tmp_path):
        f = tmp_path / "bad_middle.ndjson"
        f.write_text('{"a": 1}\nBAD\n{"b": 2}\n')
        validate_file(str(f), mode="tail")  # Tail doesn't catch middle errors

    def test_ndjson_tail_catches_truncation(self, tmp_path):
        f = tmp_path / "trunc.ndjson"
        f.write_text('{"a": 1}\n{"incomplete":')
        with pytest.raises(ValueError):
            validate_file(str(f), mode="tail")
