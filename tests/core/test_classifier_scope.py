"""Tests for `normalize_classifier_entries` — per-classifier data_types scope.

Introduced as a breaking change in commit b1bbeb2 ("per-classifier data_types
scope and ingestion auto-derivation in ml profile"). The function is the
single source of truth for the two YAML forms a classifier entry can take:

    classifiers:
      - my_classifier                          # bare string → runs on all data_types
      - name: scoped_classifier                # dict with 'data_types' → scoped
        data_types: [submissions]

A bug here is silent: a classifier runs on the wrong subset and you don't
notice until results are mysteriously incomplete.
"""

from __future__ import annotations

import pytest

from social_data_pipeline.core.config import (
    ConfigurationError,
    normalize_classifier_entries,
)


KNOWN = ["submissions", "comments"]


# ── happy paths ─────────────────────────────────────────────────────────────


class TestStringEntry:
    def test_runs_on_all_data_types(self):
        out = normalize_classifier_entries(["lingua"], KNOWN, "lingua")
        assert out == [{"name": "lingua", "data_types": None}]

    def test_multiple_string_entries(self):
        out = normalize_classifier_entries(["a", "b"], KNOWN, "ml")
        assert out == [
            {"name": "a", "data_types": None},
            {"name": "b", "data_types": None},
        ]


class TestDictEntry:
    def test_no_data_types_key_means_all(self):
        # Dict without 'data_types' is equivalent to a bare string entry.
        out = normalize_classifier_entries([{"name": "lingua"}], KNOWN, "lingua")
        assert out == [{"name": "lingua", "data_types": None}]

    def test_explicit_scope_preserved(self):
        out = normalize_classifier_entries(
            [{"name": "tox", "data_types": ["comments"]}], KNOWN, "ml"
        )
        assert out == [{"name": "tox", "data_types": ["comments"]}]

    def test_explicit_scope_full_set_kept_as_list(self):
        # Even when scope == every known data_type, we keep the explicit list
        # rather than collapsing to None — preserves the user's intent (and
        # prevents drift if a new data_type is added to the source later).
        out = normalize_classifier_entries(
            [{"name": "tox", "data_types": ["submissions", "comments"]}],
            KNOWN,
            "ml",
        )
        assert out == [{"name": "tox", "data_types": ["submissions", "comments"]}]


class TestMixedEntries:
    def test_string_and_dict_in_same_list(self):
        out = normalize_classifier_entries(
            ["lingua", {"name": "tox", "data_types": ["comments"]}],
            KNOWN,
            "ml",
        )
        assert out == [
            {"name": "lingua", "data_types": None},
            {"name": "tox", "data_types": ["comments"]},
        ]

    def test_empty_list(self):
        assert normalize_classifier_entries([], KNOWN, "ml") == []


# ── error paths ─────────────────────────────────────────────────────────────


class TestErrorPaths:
    def test_dict_missing_name(self):
        with pytest.raises(ConfigurationError, match="missing 'name'"):
            normalize_classifier_entries(
                [{"data_types": ["comments"]}], KNOWN, "ml"
            )

    def test_scope_must_be_a_list(self):
        with pytest.raises(ConfigurationError, match="must be a non-empty list"):
            normalize_classifier_entries(
                [{"name": "tox", "data_types": "comments"}], KNOWN, "ml"
            )

    def test_scope_must_be_non_empty(self):
        with pytest.raises(ConfigurationError, match="must be a non-empty list"):
            normalize_classifier_entries(
                [{"name": "tox", "data_types": []}], KNOWN, "ml"
            )

    def test_unknown_data_type_rejected(self):
        with pytest.raises(ConfigurationError, match="unknown data_types"):
            normalize_classifier_entries(
                [{"name": "tox", "data_types": ["ghost"]}], KNOWN, "ml"
            )

    def test_partial_unknown_data_type_rejected(self):
        # Even one unknown entry in an otherwise-valid list raises — silent
        # passthrough of typos would mean the classifier just never runs
        # on the typo'd data_type with no error.
        with pytest.raises(ConfigurationError, match=r"\['typo'\]"):
            normalize_classifier_entries(
                [{"name": "tox", "data_types": ["comments", "typo"]}], KNOWN, "ml"
            )

    def test_invalid_entry_type(self):
        with pytest.raises(ConfigurationError, match="must be string or dict"):
            normalize_classifier_entries([42], KNOWN, "ml")

    def test_profile_name_in_error_message(self):
        # Errors include the profile name (ml or lingua) so the user knows
        # which YAML to fix.
        with pytest.raises(ConfigurationError, match=r"\[lingua\]"):
            normalize_classifier_entries([42], KNOWN, "lingua")
