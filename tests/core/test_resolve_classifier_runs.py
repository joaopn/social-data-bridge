"""Tests for `resolve_classifier_runs` — postgres_ml and sr_ml share this.

The function composes the ordered list of classifier ingestion runs from
the source's lingua/ml profile configs and the per-classifier ingestion
overrides (services.yaml + source override). Two silent-bug classes:

- `prefer_lingua=True` is supposed to *skip* lingua ingestion (because
  lingua data is already merged into the base tables via the parent
  ingest profile's prefer_lingua flag). Get this wrong and you double-
  ingest lingua, or miss it entirely.
- `ingestion_overrides[name].enabled = False` should silently skip a
  classifier at ingest time without affecting the ml profile. Wrong logic
  here either runs disabled classifiers or filters out enabled ones.

`load_classifier_scopes` (file I/O) is monkey-patched per test; the focus
here is the dispatch + override merging.
"""

from __future__ import annotations

import pytest

from social_data_pipeline.core import config as cfg_mod
from social_data_pipeline.core.config import (
    ConfigurationError,
    resolve_classifier_runs,
)


# ── helpers ─────────────────────────────────────────────────────────────────


def _scope(name: str, suffix: str = None, data_types=None) -> dict:
    return {
        "name": name,
        "suffix": suffix or f"_{name}",
        "data_types": data_types,
    }


@pytest.fixture
def patch_scopes(monkeypatch):
    """Return a setter that installs canned scope lists per profile."""

    def _set(lingua_scopes=None, ml_scopes=None,
             lingua_raises: Exception | None = None,
             ml_raises: Exception | None = None):
        def fake(config_dir, source, profile="ml"):
            if profile == "lingua":
                if lingua_raises:
                    raise lingua_raises
                return list(lingua_scopes or [])
            if profile == "ml":
                if ml_raises:
                    raise ml_raises
                return list(ml_scopes or [])
            raise AssertionError(f"unexpected profile {profile!r}")

        monkeypatch.setattr(cfg_mod, "load_classifier_scopes", fake)

    return _set


# ── prefer_lingua dispatch ──────────────────────────────────────────────────


class TestPreferLingua:
    def test_true_skips_lingua(self, patch_scopes):
        # lingua data is already in the base tables via the parent ingest
        # profile. resolve_classifier_runs must NOT include a lingua run.
        patch_scopes(lingua_scopes=[_scope("lingua")], ml_scopes=[])
        runs = resolve_classifier_runs(
            config_dir="/cfg", source="reddit",
            ingestion_overrides={}, prefer_lingua=True,
        )
        assert runs == []

    def test_false_includes_lingua(self, patch_scopes):
        patch_scopes(lingua_scopes=[_scope("lingua", suffix="_lingua")], ml_scopes=[])
        runs = resolve_classifier_runs(
            config_dir="/cfg", source="reddit",
            ingestion_overrides={}, prefer_lingua=False,
        )
        assert len(runs) == 1
        assert runs[0]["name"] == "lingua"
        assert runs[0]["suffix"] == "_lingua"

    def test_true_with_no_lingua_profile_does_not_explode(self, patch_scopes):
        # Source may not have configured lingua at all. ConfigurationError
        # from load_classifier_scopes(..., profile='lingua') is swallowed.
        patch_scopes(lingua_raises=ConfigurationError("no lingua"), ml_scopes=[])
        runs = resolve_classifier_runs(
            config_dir="/cfg", source="reddit",
            ingestion_overrides={}, prefer_lingua=False,
        )
        assert runs == []


# ── ingestion_overrides.enabled flag ────────────────────────────────────────


class TestEnabledFlag:
    def test_lingua_disabled_skipped(self, patch_scopes):
        patch_scopes(lingua_scopes=[_scope("lingua")], ml_scopes=[])
        runs = resolve_classifier_runs(
            config_dir="/cfg", source="reddit",
            ingestion_overrides={"lingua": {"enabled": False}},
            prefer_lingua=False,
        )
        assert runs == []

    def test_ml_classifier_disabled_skipped(self, patch_scopes):
        patch_scopes(ml_scopes=[_scope("toxic"), _scope("emotions")])
        runs = resolve_classifier_runs(
            config_dir="/cfg", source="reddit",
            ingestion_overrides={"toxic": {"enabled": False}},
            prefer_lingua=True,
        )
        assert [r["name"] for r in runs] == ["emotions"]

    def test_default_enabled_is_true(self, patch_scopes):
        # No override at all → classifier runs.
        patch_scopes(ml_scopes=[_scope("toxic")])
        runs = resolve_classifier_runs(
            config_dir="/cfg", source="reddit",
            ingestion_overrides={}, prefer_lingua=True,
        )
        assert [r["name"] for r in runs] == ["toxic"]

    def test_explicit_enabled_true(self, patch_scopes):
        patch_scopes(ml_scopes=[_scope("toxic")])
        runs = resolve_classifier_runs(
            config_dir="/cfg", source="reddit",
            ingestion_overrides={"toxic": {"enabled": True}},
            prefer_lingua=True,
        )
        assert [r["name"] for r in runs] == ["toxic"]


# ── source_dir + column_overrides passthrough ───────────────────────────────


class TestOverridesPassthrough:
    def test_ml_source_dir_default_is_classifier_name(self, patch_scopes):
        patch_scopes(ml_scopes=[_scope("toxic")])
        runs = resolve_classifier_runs(
            config_dir="/cfg", source="reddit",
            ingestion_overrides={}, prefer_lingua=True,
        )
        assert runs[0]["source_dir"] == "toxic"

    def test_ml_source_dir_override(self, patch_scopes):
        patch_scopes(ml_scopes=[_scope("toxic")])
        runs = resolve_classifier_runs(
            config_dir="/cfg", source="reddit",
            ingestion_overrides={"toxic": {"source_dir": "tox_v2"}},
            prefer_lingua=True,
        )
        assert runs[0]["source_dir"] == "tox_v2"

    def test_lingua_source_dir_uses_source_dir_ingest(self, patch_scopes):
        # Lingua uses a distinct override key (`source_dir_ingest`) from the
        # ml classifiers (`source_dir`), with a different default.
        patch_scopes(lingua_scopes=[_scope("lingua")], ml_scopes=[])
        runs = resolve_classifier_runs(
            config_dir="/cfg", source="reddit",
            ingestion_overrides={"lingua": {"source_dir_ingest": "lang_out"}},
            prefer_lingua=False,
        )
        assert runs[0]["source_dir"] == "lang_out"

    def test_lingua_source_dir_default(self, patch_scopes):
        patch_scopes(lingua_scopes=[_scope("lingua")], ml_scopes=[])
        runs = resolve_classifier_runs(
            config_dir="/cfg", source="reddit",
            ingestion_overrides={}, prefer_lingua=False,
        )
        assert runs[0]["source_dir"] == "lingua_ingest"

    def test_column_overrides_passthrough(self, patch_scopes):
        patch_scopes(ml_scopes=[_scope("toxic")])
        col_ovr = {"toxic_score": "FLOAT"}
        runs = resolve_classifier_runs(
            config_dir="/cfg", source="reddit",
            ingestion_overrides={"toxic": {"column_overrides": col_ovr}},
            prefer_lingua=True,
        )
        assert runs[0]["column_overrides"] == col_ovr

    def test_no_column_overrides_yields_empty_dict(self, patch_scopes):
        patch_scopes(ml_scopes=[_scope("toxic")])
        runs = resolve_classifier_runs(
            config_dir="/cfg", source="reddit",
            ingestion_overrides={}, prefer_lingua=True,
        )
        assert runs[0]["column_overrides"] == {}


# ── data_types scope flows through ──────────────────────────────────────────


class TestScopeFlow:
    def test_ml_scope_preserved(self, patch_scopes):
        patch_scopes(ml_scopes=[_scope("toxic", data_types=["comments"])])
        runs = resolve_classifier_runs(
            config_dir="/cfg", source="reddit",
            ingestion_overrides={}, prefer_lingua=True,
        )
        assert runs[0]["data_types"] == ["comments"]

    def test_lingua_scope_preserved(self, patch_scopes):
        patch_scopes(
            lingua_scopes=[_scope("lingua", data_types=["submissions"])],
            ml_scopes=[],
        )
        runs = resolve_classifier_runs(
            config_dir="/cfg", source="reddit",
            ingestion_overrides={}, prefer_lingua=False,
        )
        assert runs[0]["data_types"] == ["submissions"]

    def test_none_scope_preserved_as_none(self, patch_scopes):
        # None means 'all' — must stay None, not get coerced to [] or similar.
        patch_scopes(ml_scopes=[_scope("toxic", data_types=None)])
        runs = resolve_classifier_runs(
            config_dir="/cfg", source="reddit",
            ingestion_overrides={}, prefer_lingua=True,
        )
        assert runs[0]["data_types"] is None


# ── ordering ────────────────────────────────────────────────────────────────


class TestOrdering:
    def test_lingua_first_then_ml(self, patch_scopes):
        # When both lingua (with prefer_lingua=False) and ml are present,
        # lingua runs must be emitted before ml runs.
        patch_scopes(
            lingua_scopes=[_scope("lingua")],
            ml_scopes=[_scope("toxic"), _scope("emotions")],
        )
        runs = resolve_classifier_runs(
            config_dir="/cfg", source="reddit",
            ingestion_overrides={}, prefer_lingua=False,
        )
        assert [r["name"] for r in runs] == ["lingua", "toxic", "emotions"]

    def test_ml_order_preserved(self, patch_scopes):
        patch_scopes(ml_scopes=[_scope("a"), _scope("b"), _scope("c")])
        runs = resolve_classifier_runs(
            config_dir="/cfg", source="reddit",
            ingestion_overrides={}, prefer_lingua=True,
        )
        assert [r["name"] for r in runs] == ["a", "b", "c"]


# ── error robustness ────────────────────────────────────────────────────────


class TestRobustness:
    def test_ml_profile_missing_yields_empty(self, patch_scopes):
        patch_scopes(ml_raises=ConfigurationError("no ml profile"))
        runs = resolve_classifier_runs(
            config_dir="/cfg", source="reddit",
            ingestion_overrides={}, prefer_lingua=True,
        )
        assert runs == []

    def test_both_profiles_missing(self, patch_scopes):
        patch_scopes(
            lingua_raises=ConfigurationError("no lingua"),
            ml_raises=ConfigurationError("no ml"),
        )
        runs = resolve_classifier_runs(
            config_dir="/cfg", source="reddit",
            ingestion_overrides={}, prefer_lingua=False,
        )
        assert runs == []

    def test_none_ingestion_override_value_treated_as_empty(self, patch_scopes):
        # `services.yaml` may legitimately have `toxic: null` → ovr resolves
        # to {}, so all defaults apply rather than crashing on .get on None.
        patch_scopes(ml_scopes=[_scope("toxic")])
        runs = resolve_classifier_runs(
            config_dir="/cfg", source="reddit",
            ingestion_overrides={"toxic": None},
            prefer_lingua=True,
        )
        assert runs[0]["name"] == "toxic"
        assert runs[0]["source_dir"] == "toxic"
