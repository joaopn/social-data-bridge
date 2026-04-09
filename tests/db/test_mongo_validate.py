"""Tests for MongoDB pre-import file validation."""

import json
import pytest

from social_data_pipeline.db.mongo.ingest import (
    _validate_ndjson_tail,
    _validate_ndjson_full,
    _validate_csv_tail,
    validate_file,
)


# -- NDJSON tail validation --------------------------------------------------


class TestValidateNdjsonTail:
    def test_valid_file(self, tmp_path):
        f = tmp_path / "valid.ndjson"
        f.write_text('{"a": 1}\n{"b": 2}\n')
        assert _validate_ndjson_tail(str(f)) == "ok"

    def test_valid_single_line(self, tmp_path):
        f = tmp_path / "single.ndjson"
        f.write_text('{"a": 1}\n')
        assert _validate_ndjson_tail(str(f)) == "ok"

    def test_truncated_mid_object(self, tmp_path):
        f = tmp_path / "truncated.ndjson"
        f.write_text('{"a": 1}\n{"b": 2, "c":')
        assert _validate_ndjson_tail(str(f)) == "truncated"

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
        assert _validate_ndjson_tail(str(f)) == "ok"

    def test_file_larger_than_8kb(self, tmp_path):
        f = tmp_path / "large.ndjson"
        lines = [json.dumps({"index": i, "data": "x" * 20}) for i in range(400)]
        f.write_text('\n'.join(lines) + '\n')
        assert _validate_ndjson_tail(str(f)) == "ok"

    def test_large_file_truncated(self, tmp_path):
        f = tmp_path / "large_trunc.ndjson"
        lines = [json.dumps({"index": i, "data": "x" * 20}) for i in range(400)]
        content = '\n'.join(lines) + '\n{"incomplete": true, "cut_off'
        f.write_text(content)
        assert _validate_ndjson_tail(str(f)) == "truncated"

    def test_malformed_middle_passes_tail(self, tmp_path):
        """Tail validation does not catch malformed lines in the middle."""
        f = tmp_path / "bad_middle.ndjson"
        f.write_text('{"a": 1}\nNOT_JSON\n{"b": 2}\n')
        assert _validate_ndjson_tail(str(f)) == "ok"


# -- NDJSON full validation --------------------------------------------------


class TestValidateNdjsonFull:
    def test_valid_file(self, tmp_path):
        f = tmp_path / "valid.ndjson"
        f.write_text('{"a": 1}\n{"b": 2}\n')
        assert _validate_ndjson_full(str(f)) == "ok"

    def test_valid_single_line(self, tmp_path):
        f = tmp_path / "single.ndjson"
        f.write_text('{"a": 1}\n')
        assert _validate_ndjson_full(str(f)) == "ok"

    def test_truncated_mid_object(self, tmp_path):
        """Truncation at end of file returns 'truncated', not ValueError."""
        f = tmp_path / "truncated.ndjson"
        f.write_text('{"a": 1}\n{"b": 2, "c":')
        assert _validate_ndjson_full(str(f)) == "truncated"

    def test_truncated_with_trailing_blank(self, tmp_path):
        """Truncated last content line followed by blank lines."""
        f = tmp_path / "trunc_blank.ndjson"
        f.write_text('{"a": 1}\n{"b": 2, "c":\n\n')
        assert _validate_ndjson_full(str(f)) == "truncated"

    def test_malformed_middle(self, tmp_path):
        """Bad JSON in the middle raises ValueError (not truncation)."""
        f = tmp_path / "bad_middle.ndjson"
        f.write_text('{"a": 1}\nNOT_JSON\n{"b": 2}\n')
        with pytest.raises(ValueError, match="Malformed JSON at line 2"):
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
        assert _validate_ndjson_full(str(f)) == "ok"

    def test_error_reports_line_number(self, tmp_path):
        f = tmp_path / "line5.ndjson"
        lines = [json.dumps({"i": i}) for i in range(4)]
        lines.append('BROKEN')
        lines.append(json.dumps({"i": 5}))
        f.write_text('\n'.join(lines) + '\n')
        with pytest.raises(ValueError, match="line 5"):
            _validate_ndjson_full(str(f))

    def test_single_malformed_line_is_truncated(self, tmp_path):
        """A file with only one line that's bad JSON → truncated (no prior valid lines)."""
        f = tmp_path / "single_bad.ndjson"
        f.write_text('{"incomplete":')
        assert _validate_ndjson_full(str(f)) == "truncated"


# -- CSV tail validation -----------------------------------------------------


class TestValidateCsvTail:
    def test_valid_csv(self, tmp_path):
        f = tmp_path / "valid.csv"
        f.write_text('a,b,c\n1,2,3\n4,5,6\n')
        assert _validate_csv_tail(str(f)) == "ok"

    def test_truncated_no_newline(self, tmp_path):
        f = tmp_path / "trunc.csv"
        f.write_text('a,b,c\n1,2,3\n4,5')
        assert _validate_csv_tail(str(f)) == "truncated"

    def test_truncated_field_count(self, tmp_path):
        f = tmp_path / "fields.csv"
        f.write_text('a,b,c\n1,2,3\n4,5\n')
        assert _validate_csv_tail(str(f)) == "truncated"

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


# -- validate_file dispatcher -------------------------------------------------


class TestValidateFile:
    def test_none_mode_skips(self, tmp_path):
        f = tmp_path / "bad.ndjson"
        f.write_text('NOT_JSON')
        assert validate_file(str(f), mode="none") == "none"

    def test_csv_dispatch(self, tmp_path):
        f = tmp_path / "valid.csv"
        f.write_text('a,b\n1,2\n')
        assert validate_file(str(f), mode="full") == "ok"

    def test_ndjson_full_malformed_raises(self, tmp_path):
        f = tmp_path / "bad.ndjson"
        f.write_text('{"a": 1}\nBAD\n{"b": 2}\n')
        with pytest.raises(ValueError, match="Malformed JSON"):
            validate_file(str(f), mode="full")

    def test_ndjson_full_truncated(self, tmp_path):
        f = tmp_path / "trunc.ndjson"
        f.write_text('{"a": 1}\n{"incomplete":')
        assert validate_file(str(f), mode="full") == "truncated"

    def test_ndjson_tail_dispatch(self, tmp_path):
        f = tmp_path / "bad_middle.ndjson"
        f.write_text('{"a": 1}\nBAD\n{"b": 2}\n')
        assert validate_file(str(f), mode="tail") == "ok"  # Tail doesn't catch middle

    def test_ndjson_tail_catches_truncation(self, tmp_path):
        f = tmp_path / "trunc.ndjson"
        f.write_text('{"a": 1}\n{"incomplete":')
        assert validate_file(str(f), mode="tail") == "truncated"

    def test_csv_truncated(self, tmp_path):
        f = tmp_path / "trunc.csv"
        f.write_text('a,b,c\n1,2,3\n4,5')
        assert validate_file(str(f), mode="full") == "truncated"
