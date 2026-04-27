"""Tests for social_data_pipeline.core.parser."""

import pytest
import polars as pl

from social_data_pipeline.core.parser import (
    escape_string,
    escape_string_parquet,
    quote_field,
    get_nested_data,
    enforce_data_type,
    flatten_record,
    yaml_type_to_polars,
    build_parquet_schema,
    write_parquet_file,
    BatchedParquetWriter,
)


# ── escape_string ───────────────────────────────────────────────────────────

class TestEscapeString:
    def test_backslash(self):
        assert escape_string("a\\b") == "a\\\\b"

    def test_newline(self):
        assert escape_string("a\nb") == "a\\nb"

    def test_carriage_return(self):
        assert escape_string("a\rb") == "a\\rb"

    def test_null_byte_removed(self):
        assert escape_string("a\x00b") == "ab"

    def test_combined(self):
        assert escape_string("a\\\n\r\x00b") == "a\\\\\\n\\rb"

    def test_non_string_passthrough(self):
        assert escape_string(42) == 42
        assert escape_string(None) is None

    def test_empty_string(self):
        assert escape_string("") == ""


# ── escape_string_parquet ───────────────────────────────────────────────────

class TestEscapeStringParquet:
    def test_only_strips_null(self):
        assert escape_string_parquet("a\x00b") == "ab"

    def test_preserves_newlines(self):
        assert escape_string_parquet("a\nb") == "a\nb"

    def test_preserves_backslash(self):
        assert escape_string_parquet("a\\b") == "a\\b"

    def test_non_string_passthrough(self):
        assert escape_string_parquet(3.14) == 3.14


# ── quote_field ─────────────────────────────────────────────────────────────

class TestQuoteField:
    def test_none(self):
        assert quote_field(None) == ""

    def test_empty_string(self):
        assert quote_field("") == ""

    def test_simple_string(self):
        assert quote_field("hello") == '"hello"'

    def test_string_with_quotes(self):
        assert quote_field('say "hi"') == '"say ""hi"""'

    def test_integer(self):
        assert quote_field(42) == "42"

    def test_boolean(self):
        assert quote_field(True) == "True"


# ── get_nested_data ─────────────────────────────────────────────────────────

class TestGetNestedData:
    def test_simple_key(self):
        assert get_nested_data({"a": 1}, "a") == 1

    def test_dot_notation(self):
        assert get_nested_data({"user": {"name": "alice"}}, "user.name") == "alice"

    def test_array_index(self):
        assert get_nested_data({"items": [10, 20, 30]}, "items.1") == 20

    def test_array_to_pipe_string(self):
        assert get_nested_data({"tags": ["a", "b", "c"]}, "tags") == "a|b|c"

    def test_array_skips_none(self):
        assert get_nested_data({"tags": ["a", None, "c"]}, "tags") == "a|c"

    def test_missing_key_returns_none(self):
        assert get_nested_data({"a": 1}, "b") is None

    def test_deep_missing_returns_none(self):
        assert get_nested_data({"a": {"b": 1}}, "a.c.d") is None

    def test_none_intermediate(self):
        assert get_nested_data({"a": None}, "a.b") is None

    def test_array_index_out_of_range(self):
        assert get_nested_data({"items": [1]}, "items.5") is None

    def test_nested_array_access(self):
        data = {"items": [{"id": "x"}, {"id": "y"}]}
        assert get_nested_data(data, "items.0.id") == "x"


# ── enforce_data_type ───────────────────────────────────────────────────────

class TestEnforceDataType:
    def test_integer(self):
        assert enforce_data_type("count", "42", {"count": "integer"}) == 42

    def test_integer_invalid(self):
        assert enforce_data_type("count", "abc", {"count": "integer"}) is None

    def test_bigint(self):
        assert enforce_data_type("big", "999999999999", {"big": "bigint"}) == 999999999999

    def test_float(self):
        assert enforce_data_type("score", "3.14", {"score": "float"}) == pytest.approx(3.14)

    def test_float_invalid(self):
        assert enforce_data_type("score", "nope", {"score": "float"}) is None

    def test_boolean_true_values(self):
        types = {"flag": "boolean"}
        assert enforce_data_type("flag", True, types) is True
        assert enforce_data_type("flag", "true", types) is True
        assert enforce_data_type("flag", "True", types) is True
        assert enforce_data_type("flag", 1, types) is True

    def test_boolean_false_values(self):
        assert enforce_data_type("flag", False, {"flag": "boolean"}) is False
        assert enforce_data_type("flag", "false", {"flag": "boolean"}) is False

    def test_text(self):
        assert enforce_data_type("body", 123, {"body": "text"}) == "123"

    def test_text_none(self):
        assert enforce_data_type("body", None, {"body": "text"}) is None

    def test_char_truncation(self):
        types = {"id": ["char", 7]}
        assert enforce_data_type("id", "abcdefghij", types) == "abcdefg"

    def test_varchar_truncation(self):
        types = {"name": ["varchar", 5]}
        assert enforce_data_type("name", "toolong", types) == "toolo"

    def test_unknown_key_passthrough(self):
        assert enforce_data_type("unknown", "val", {"other": "text"}) == "val"

    def test_no_types_passthrough(self):
        assert enforce_data_type("k", "v", {}) == "v"
        assert enforce_data_type("k", "v", None) == "v"


# ── flatten_record ──────────────────────────────────────────────────────────

class TestFlattenRecord:
    def test_basic(self):
        record = {"id": "abc", "score": "10"}
        fields = ["id", "score"]
        types = {"id": "text", "score": "integer"}
        result = flatten_record(record, fields, types)
        assert result == ["abc", 10]

    def test_nested_field(self):
        record = {"user": {"name": "alice"}}
        fields = ["user.name"]
        types = {"name": "text"}
        result = flatten_record(record, fields, types)
        assert result == ["alice"]

    def test_escaping_applied(self):
        record = {"body": "line1\nline2"}
        result = flatten_record(record, ["body"], {"body": "text"})
        assert result == ["line1\\nline2"]

    def test_missing_field_returns_none(self):
        record = {"a": 1}
        result = flatten_record(record, ["b"], {})
        assert result == [None]


# ── yaml_type_to_polars ────────────────────────────────────────────────────

class TestYamlTypeToPolars:
    def test_integer(self):
        assert yaml_type_to_polars("integer") == pl.Int64

    def test_bigint(self):
        assert yaml_type_to_polars("bigint") == pl.Int64

    def test_float(self):
        assert yaml_type_to_polars("float") == pl.Float64

    def test_boolean(self):
        assert yaml_type_to_polars("boolean") == pl.Boolean

    def test_text(self):
        assert yaml_type_to_polars("text") == pl.Utf8

    def test_list_type(self):
        assert yaml_type_to_polars(["char", 7]) == pl.Utf8

    def test_unknown_defaults_to_utf8(self):
        assert yaml_type_to_polars("somethingelse") == pl.Utf8


# ── build_parquet_schema ────────────────────────────────────────────────────

class TestBuildParquetSchema:
    def test_basic(self):
        cols = ["id", "score", "body"]
        types = {"id": ["char", 7], "score": "integer", "body": "text"}
        schema = build_parquet_schema(cols, types)
        assert schema == {"id": pl.Utf8, "score": pl.Int64, "body": pl.Utf8}

    def test_missing_type_defaults_utf8(self):
        schema = build_parquet_schema(["unknown"], {})
        assert schema == {"unknown": pl.Utf8}


# ── write_parquet_file ──────────────────────────────────────────────────────

class TestWriteParquetFile:
    def test_write_and_read(self, tmp_path):
        out = tmp_path / "test.parquet"
        rows = [{"id": "a", "val": 1}, {"id": "b", "val": 2}]
        cols = ["id", "val"]
        types = {"id": "text", "val": "integer"}
        count = write_parquet_file(rows, cols, types, str(out))
        assert count == 2
        assert out.exists()
        df = pl.read_parquet(out)
        assert df.shape == (2, 2)
        assert df["id"].to_list() == ["a", "b"]
        assert df["val"].to_list() == [1, 2]

    def test_temp_file_cleaned_on_success(self, tmp_path):
        out = tmp_path / "clean.parquet"
        write_parquet_file([{"x": "1"}], ["x"], {"x": "text"}, str(out))
        assert not (tmp_path / "clean.parquet.temp").exists()

    def test_atomic_rename(self, tmp_path):
        """Output should not exist until write completes (atomic)."""
        out = tmp_path / "atomic.parquet"
        write_parquet_file([{"a": "v"}], ["a"], {}, str(out))
        assert out.exists()

    def test_empty_rows(self, tmp_path):
        out = tmp_path / "empty.parquet"
        count = write_parquet_file([], ["x"], {"x": "text"}, str(out))
        assert count == 0
        df = pl.read_parquet(out)
        assert df.shape[0] == 0


# ── BatchedParquetWriter ───────────────────────────────────────────────────

class TestBatchedParquetWriter:
    def test_basic_write(self, tmp_path):
        out = tmp_path / "batched.parquet"
        writer = BatchedParquetWriter(["id", "val"], {"id": "text", "val": "integer"}, str(out), batch_size=2)
        writer.append({"id": "a", "val": 1})
        writer.append({"id": "b", "val": 2})
        writer.append({"id": "c", "val": 3})
        total = writer.close()
        assert total == 3
        df = pl.read_parquet(out)
        assert df.shape == (3, 2)

    def test_auto_flush(self, tmp_path):
        out = tmp_path / "flush.parquet"
        writer = BatchedParquetWriter(["x"], {"x": "text"}, str(out), batch_size=2)
        writer.append({"x": "a"})
        writer.append({"x": "b"})  # triggers flush
        writer.append({"x": "c"})
        total = writer.close()
        assert total == 3

    def test_cleanup_removes_temp(self, tmp_path):
        out = tmp_path / "fail.parquet"
        writer = BatchedParquetWriter(["x"], {"x": "text"}, str(out), batch_size=100)
        writer.append({"x": "a"})
        writer._flush()  # write to temp
        temp = out.with_suffix(out.suffix + ".temp")
        assert temp.exists()
        writer.cleanup()
        assert not temp.exists()
        assert not out.exists()

    def test_close_returns_zero_for_empty(self, tmp_path):
        out = tmp_path / "noop.parquet"
        writer = BatchedParquetWriter(["x"], {"x": "text"}, str(out))
        total = writer.close()
        assert total == 0
