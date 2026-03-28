"""Tests for Reddit platform parser."""

import csv
import json
import os
from pathlib import Path

import pytest
import yaml

from social_data_pipeline.platforms.reddit.parser import (
    base36_to_int,
    determine_removal_status,
    get_all_columns,
    transform_json,
    parse_to_csv,
    process_single_file,
    MANDATORY_FIELDS,
    MANDATORY_FIELD_TYPES,
)


# ============================================================================
# base36_to_int
# ============================================================================


class TestBase36ToInt:
    def test_simple_value(self):
        assert base36_to_int("1a") == 46

    def test_letters_only(self):
        assert base36_to_int("abc") == 13368

    def test_empty_string(self):
        assert base36_to_int("") is None

    def test_none(self):
        assert base36_to_int(None) is None

    def test_invalid_characters(self):
        assert base36_to_int("!!!") is None

    def test_zero(self):
        assert base36_to_int("0") == 0

    def test_large_value(self):
        assert base36_to_int("zzzzzz") == 2176782335

    def test_single_digit(self):
        assert base36_to_int("z") == 35

    def test_mixed_case(self):
        # int(value, 36) is case-insensitive
        assert base36_to_int("ABC") == base36_to_int("abc")


# ============================================================================
# determine_removal_status
# ============================================================================


class TestDetermineRemovalStatus:
    """Test the waterfall algorithm priority levels."""

    def test_priority1_meta_removal_type(self):
        data = {"_meta": {"removal_type": "moderator"}}
        is_deleted, removal_type = determine_removal_status(data)
        assert is_deleted is True
        assert removal_type == "moderator"

    def test_priority1_meta_removal_type_passthrough(self):
        data = {"_meta": {"removal_type": "content_takedown"}}
        is_deleted, removal_type = determine_removal_status(data)
        assert is_deleted is True
        assert removal_type == "content_takedown"

    def test_priority2_was_deleted_later_with_no_type(self):
        """was_deleted_later alone defaults to 'deleted'."""
        data = {"_meta": {"was_deleted_later": True}}
        is_deleted, removal_type = determine_removal_status(data)
        assert is_deleted is True
        assert removal_type == "deleted"

    def test_priority2_was_deleted_later_with_later_priority(self):
        """was_deleted_later marks deleted, but type comes from later priority."""
        data = {"_meta": {"was_deleted_later": True}, "spam": True}
        is_deleted, removal_type = determine_removal_status(data)
        assert is_deleted is True
        assert removal_type == "reddit"

    def test_priority3_removed_by_category_deleted(self):
        data = {"removed_by_category": "deleted"}
        is_deleted, removal_type = determine_removal_status(data)
        assert is_deleted is True
        assert removal_type == "deleted"

    def test_priority3_removed_by_category_author(self):
        data = {"removed_by_category": "author"}
        is_deleted, removal_type = determine_removal_status(data)
        assert is_deleted is True
        assert removal_type == "deleted"

    def test_priority3_removed_by_category_moderator(self):
        data = {"removed_by_category": "moderator"}
        is_deleted, removal_type = determine_removal_status(data)
        assert is_deleted is True
        assert removal_type == "moderator"

    def test_priority3_removed_by_category_anti_evil_ops(self):
        data = {"removed_by_category": "anti_evil_ops"}
        is_deleted, removal_type = determine_removal_status(data)
        assert is_deleted is True
        assert removal_type == "reddit"

    def test_priority3_removed_by_category_admin(self):
        data = {"removed_by_category": "admin"}
        is_deleted, removal_type = determine_removal_status(data)
        assert is_deleted is True
        assert removal_type == "reddit"

    def test_priority3_removed_by_category_automod_filtered(self):
        data = {"removed_by_category": "automod_filtered"}
        is_deleted, removal_type = determine_removal_status(data)
        assert is_deleted is True
        assert removal_type == "automod_filtered"

    def test_priority3_removed_by_category_unknown(self):
        data = {"removed_by_category": "some_new_category"}
        is_deleted, removal_type = determine_removal_status(data)
        assert is_deleted is True
        assert removal_type == "moderator"

    def test_priority4_spam_true(self):
        data = {"spam": True}
        is_deleted, removal_type = determine_removal_status(data)
        assert is_deleted is True
        assert removal_type == "reddit"

    def test_priority4_spam_false_skipped(self):
        """spam=False should not trigger."""
        data = {"spam": False}
        is_deleted, removal_type = determine_removal_status(data)
        assert is_deleted is False
        assert removal_type == ""

    def test_priority5_removed_true(self):
        data = {"removed": True}
        is_deleted, removal_type = determine_removal_status(data)
        assert is_deleted is True
        assert removal_type == "moderator"

    def test_priority6_banned_by_true(self):
        data = {"banned_by": True}
        is_deleted, removal_type = determine_removal_status(data)
        assert is_deleted is True
        assert removal_type == "reddit"

    def test_priority6_banned_by_string_true(self):
        data = {"banned_by": "true"}
        is_deleted, removal_type = determine_removal_status(data)
        assert is_deleted is True
        assert removal_type == "reddit"

    def test_priority6_banned_by_automoderator(self):
        data = {"banned_by": "AutoModerator"}
        is_deleted, removal_type = determine_removal_status(data)
        assert is_deleted is True
        assert removal_type == "automod_filtered"

    def test_priority6_banned_by_other_string(self):
        data = {"banned_by": "some_mod_user"}
        is_deleted, removal_type = determine_removal_status(data)
        assert is_deleted is True
        assert removal_type == "moderator"

    def test_priority6_banned_by_skipped_values(self):
        """None, empty string, and False should be skipped."""
        for value in [None, "", False]:
            data = {"banned_by": value}
            is_deleted, removal_type = determine_removal_status(data)
            assert is_deleted is False, f"banned_by={value!r} should not trigger"

    def test_priority7_body_removed(self):
        data = {"body": "[removed]"}
        is_deleted, removal_type = determine_removal_status(data)
        assert is_deleted is True
        assert removal_type == "moderator"

    def test_priority7_selftext_deleted(self):
        data = {"selftext": "[deleted]"}
        is_deleted, removal_type = determine_removal_status(data)
        assert is_deleted is True
        assert removal_type == "deleted"

    def test_priority8_author_deleted(self):
        data = {"author": "[deleted]"}
        is_deleted, removal_type = determine_removal_status(data)
        assert is_deleted is True
        assert removal_type == "deleted"

    def test_not_removed(self):
        data = {"author": "normal_user", "body": "a normal comment"}
        is_deleted, removal_type = determine_removal_status(data)
        assert is_deleted is False
        assert removal_type == ""

    def test_priority_ordering_p1_wins_over_p3(self):
        """Priority 1 (_meta.removal_type) wins over priority 3 (removed_by_category)."""
        data = {
            "_meta": {"removal_type": "content_takedown"},
            "removed_by_category": "moderator",
        }
        is_deleted, removal_type = determine_removal_status(data)
        assert removal_type == "content_takedown"

    def test_priority_ordering_p3_wins_over_p7(self):
        """Priority 3 (removed_by_category) wins over text markers."""
        data = {
            "removed_by_category": "reddit",
            "body": "[deleted]",
        }
        is_deleted, removal_type = determine_removal_status(data)
        assert removal_type == "reddit"


# ============================================================================
# get_all_columns
# ============================================================================


class TestGetAllColumns:
    def test_basic(self):
        result = get_all_columns("comments", ["author", "body", "score"])
        assert result == ["dataset", "id", "retrieved_utc", "author", "body", "score"]

    def test_empty_fields(self):
        result = get_all_columns("submissions", [])
        assert result == ["dataset", "id", "retrieved_utc"]


# ============================================================================
# transform_json
# ============================================================================


class TestTransformJson:
    def test_basic_comment(self):
        data = {
            "id": "abc123",
            "author": "TestUser",
            "body": "Hello world",
            "subreddit": "Python",
            "score": 42,
            "created_utc": 1704067200,
            "retrieved_utc": 1704153600,
        }
        field_types = {"score": "integer", "created_utc": "integer", "author": "text", "body": "text", "subreddit": "text"}
        fields = ["created_utc", "author", "body", "subreddit", "score"]
        result = transform_json(data, "2024-01", field_types, fields)
        # [dataset, id, retrieved_utc, created_utc, author, body, subreddit, score]
        assert result[0] == "2024-01"
        assert result[1] == "abc123"  # id (truncated by char(7) -> "abc123" stays <= 7)
        assert result[2] == 1704153600  # retrieved_utc
        assert result[3] == 1704067200  # created_utc
        assert result[4] == "testuser"  # author lowercased
        assert result[6] == "python"  # subreddit lowercased

    def test_retrieved_utc_fallback_from_retrieved_on(self):
        data = {
            "id": "abc",
            "author": "user",
            "subreddit": "test",
            "retrieved_on": 12345,
        }
        field_types = {"author": "text", "subreddit": "text"}
        result = transform_json(data, "2024-01", field_types, ["author"])
        # retrieved_utc should come from retrieved_on
        assert result[2] == 12345

    def test_meta_retrieved_2nd_on(self):
        data = {
            "id": "abc",
            "author": "user",
            "subreddit": "test",
            "retrieved_utc": 11111,
            "_meta": {"retrieved_2nd_on": 22222},
        }
        field_types = {"author": "text", "subreddit": "text"}
        result = transform_json(data, "2024-01", field_types, ["author"])
        # retrieved_utc overridden by _meta.retrieved_2nd_on
        assert result[2] == 22222

    def test_id10_computation(self):
        data = {
            "id": "1a",
            "author": "user",
            "subreddit": "test",
            "retrieved_utc": 1000,
        }
        field_types = {"id10": "bigint", "author": "text", "subreddit": "text"}
        result = transform_json(data, "2024-01", field_types, ["id10"])
        # id10 = base36_to_int("1a") = 46
        assert result[3] == 46

    def test_is_deleted_and_removal_type_text(self):
        data = {
            "id": "abc",
            "author": "user",
            "subreddit": "test",
            "retrieved_utc": 1000,
            "spam": True,
        }
        field_types = {"is_deleted": "text", "removal_type": "text", "author": "text", "subreddit": "text"}
        result = transform_json(data, "2024-01", field_types, ["is_deleted", "removal_type"])
        # spam=True -> (True, 'reddit')
        assert result[3] == "True"  # is_deleted as text
        assert result[4] == "reddit"  # removal_type as text


# ============================================================================
# parse_to_csv end-to-end: comments to CSV
# ============================================================================


class TestParseToCSV:
    def test_parse_comments_csv(self, config_fixtures_dir, reddit_fixtures_dir, tmp_path):
        platform_config = yaml.safe_load(
            (config_fixtures_dir / "valid_platform_reddit.yaml").read_text()
        )
        platform_config["file_format"] = "csv"
        input_file = str(reddit_fixtures_dir / "RC_2024-01.ndjson")

        # Count input lines
        with open(input_file) as f:
            expected_rows = sum(1 for line in f if line.strip())

        output_path = parse_to_csv(
            input_file=input_file,
            output_dir=str(tmp_path),
            data_type="comments",
            platform_config=platform_config,
        )

        assert os.path.exists(output_path)
        with open(output_path, newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader)
            rows = list(reader)

        assert "dataset" in header
        assert "id" in header
        assert "retrieved_utc" in header
        assert len(rows) == expected_rows
        # Check no empty ids
        id_idx = header.index("id")
        for row in rows:
            assert row[id_idx] != ""

    def test_parse_submissions_parquet(self, config_fixtures_dir, reddit_fixtures_dir, tmp_path):
        import polars as pl

        platform_config = yaml.safe_load(
            (config_fixtures_dir / "valid_platform_reddit.yaml").read_text()
        )
        platform_config["file_format"] = "parquet"
        input_file = str(reddit_fixtures_dir / "RS_2024-01.ndjson")

        with open(input_file) as f:
            expected_rows = sum(1 for line in f if line.strip())

        output_path = parse_to_csv(
            input_file=input_file,
            output_dir=str(tmp_path),
            data_type="submissions",
            platform_config=platform_config,
        )

        assert os.path.exists(output_path)
        assert output_path.endswith(".parquet")
        df = pl.read_parquet(output_path)
        assert len(df) == expected_rows
        assert "dataset" in df.columns
        assert "id" in df.columns
        assert "retrieved_utc" in df.columns
        # Check submission-specific fields present
        assert "title" in df.columns
        assert "selftext" in df.columns

    def test_parse_comments_parquet_row_count(self, config_fixtures_dir, reddit_fixtures_dir, tmp_path):
        import polars as pl

        platform_config = yaml.safe_load(
            (config_fixtures_dir / "valid_platform_reddit.yaml").read_text()
        )
        platform_config["file_format"] = "parquet"
        input_file = str(reddit_fixtures_dir / "RC_2024-01.ndjson")

        with open(input_file) as f:
            expected_rows = sum(1 for line in f if line.strip())

        output_path = parse_to_csv(
            input_file=input_file,
            output_dir=str(tmp_path),
            data_type="comments",
            platform_config=platform_config,
        )

        df = pl.read_parquet(output_path)
        assert len(df) == expected_rows

    def test_parse_no_field_types_raises(self, tmp_path):
        from social_data_pipeline.core.config import ConfigurationError

        platform_config = {
            "fields": {"comments": ["author"]},
            "field_types": {},
        }
        with pytest.raises(ConfigurationError, match="No field_types"):
            parse_to_csv(
                input_file="dummy",
                output_dir=str(tmp_path),
                data_type="comments",
                platform_config=platform_config,
            )

    def test_parse_no_fields_for_data_type_raises(self, tmp_path):
        from social_data_pipeline.core.config import ConfigurationError

        platform_config = {
            "field_types": {"author": "text"},
            "fields": {},
        }
        with pytest.raises(ConfigurationError, match="No fields configured"):
            parse_to_csv(
                input_file="dummy",
                output_dir=str(tmp_path),
                data_type="comments",
                platform_config=platform_config,
            )


# ============================================================================
# process_single_file
# ============================================================================


class TestProcessSingleFile:
    def test_csv_output_uses_temp_rename(self, reddit_fixtures_dir, config_fixtures_dir, tmp_path):
        """Verify that the final CSV file exists (temp was renamed)."""
        platform_config = yaml.safe_load(
            (config_fixtures_dir / "valid_platform_reddit.yaml").read_text()
        )
        input_file = str(reddit_fixtures_dir / "RC_2024-01.ndjson")
        output_file = str(tmp_path / "comments" / "RC_2024-01.csv")
        (tmp_path / "comments").mkdir()

        input_size, out_path = process_single_file(
            input_file=input_file,
            output_file=output_file,
            data_type="comments",
            data_type_config=platform_config["field_types"],
            fields_to_extract=platform_config["fields"]["comments"],
            file_format="csv",
        )

        assert os.path.exists(out_path)
        assert input_size > 0
        # Temp file should not exist
        assert not os.path.exists(output_file + ".temp")

    def test_parquet_output(self, reddit_fixtures_dir, config_fixtures_dir, tmp_path):
        import polars as pl

        platform_config = yaml.safe_load(
            (config_fixtures_dir / "valid_platform_reddit.yaml").read_text()
        )
        input_file = str(reddit_fixtures_dir / "RC_2024-01.ndjson")
        output_file = str(tmp_path / "RC_2024-01.parquet")

        process_single_file(
            input_file=input_file,
            output_file=output_file,
            data_type="comments",
            data_type_config=platform_config["field_types"],
            fields_to_extract=platform_config["fields"]["comments"],
            file_format="parquet",
        )

        df = pl.read_parquet(output_file)
        assert len(df) == 10
        assert "dataset" in df.columns
