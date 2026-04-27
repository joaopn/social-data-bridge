"""Tests for social_data_pipeline.core.state."""

import json
import shutil

from social_data_pipeline.core.state import PipelineState


# ── Initialization ──────────────────────────────────────────────────────────

class TestInit:
    def test_creates_empty_state(self, tmp_path):
        sf = tmp_path / "state.json"
        ps = PipelineState(state_file=str(sf))
        assert ps.state["processed"] == []
        assert ps.state["failed"] == []
        assert ps.state["in_progress"] is None

    def test_creates_parent_dirs(self, tmp_path):
        sf = tmp_path / "deep" / "nested" / "state.json"
        PipelineState(state_file=str(sf))
        assert sf.parent.exists()

    def test_loads_existing_state(self, tmp_path, state_fixtures_dir):
        sf = tmp_path / "state.json"
        shutil.copy(state_fixtures_dir / "existing_state.json", sf)
        ps = PipelineState(state_file=str(sf))
        assert ps.state["processed"] == ["RC_2024-01", "RS_2024-01"]
        assert len(ps.state["failed"]) == 1
        assert ps.state["failed"][0]["filename"] == "RC_2024-02"

    def test_corrupt_state_starts_fresh(self, tmp_path, state_fixtures_dir):
        sf = tmp_path / "state.json"
        shutil.copy(state_fixtures_dir / "corrupt_state.json", sf)
        ps = PipelineState(state_file=str(sf))
        assert ps.state["processed"] == []


# ── is_processed / is_failed ────────────────────────────────────────────────

class TestQueryMethods:
    def test_is_processed_true(self, tmp_path, state_fixtures_dir):
        sf = tmp_path / "state.json"
        shutil.copy(state_fixtures_dir / "existing_state.json", sf)
        ps = PipelineState(state_file=str(sf))
        assert ps.is_processed("RC_2024-01") is True

    def test_is_processed_false(self, tmp_path):
        ps = PipelineState(state_file=str(tmp_path / "s.json"))
        assert ps.is_processed("never_seen") is False

    def test_is_failed_true(self, tmp_path, state_fixtures_dir):
        sf = tmp_path / "state.json"
        shutil.copy(state_fixtures_dir / "existing_state.json", sf)
        ps = PipelineState(state_file=str(sf))
        assert ps.is_failed("RC_2024-02") is True

    def test_is_failed_false(self, tmp_path):
        ps = PipelineState(state_file=str(tmp_path / "s.json"))
        assert ps.is_failed("nope") is False


# ── mark_in_progress / mark_completed / mark_failed ────────────────────────

class TestMarkMethods:
    def test_mark_in_progress(self, tmp_path):
        ps = PipelineState(state_file=str(tmp_path / "s.json"))
        ps.mark_in_progress("file1")
        assert ps.get_in_progress() == "file1"

    def test_mark_completed(self, tmp_path):
        ps = PipelineState(state_file=str(tmp_path / "s.json"))
        ps.mark_in_progress("file1")
        ps.mark_completed("file1")
        assert ps.is_processed("file1") is True
        assert ps.get_in_progress() is None

    def test_mark_completed_idempotent(self, tmp_path):
        ps = PipelineState(state_file=str(tmp_path / "s.json"))
        ps.mark_completed("file1")
        ps.mark_completed("file1")
        assert ps.state["processed"].count("file1") == 1

    def test_mark_failed(self, tmp_path):
        ps = PipelineState(state_file=str(tmp_path / "s.json"))
        ps.mark_in_progress("file1")
        ps.mark_failed("file1", "disk full")
        assert ps.is_failed("file1") is True
        assert ps.get_in_progress() is None
        assert ps.state["failed"][0]["error"] == "disk full"

    def test_mark_failed_updates_existing(self, tmp_path):
        ps = PipelineState(state_file=str(tmp_path / "s.json"))
        ps.mark_failed("file1", "error1")
        ps.mark_failed("file1", "error2")
        assert len(ps.state["failed"]) == 1
        assert ps.state["failed"][0]["error"] == "error2"


# ── clear_in_progress ──────────────────────────────────────────────────────

class TestClearInProgress:
    def test_clears(self, tmp_path):
        ps = PipelineState(state_file=str(tmp_path / "s.json"))
        ps.mark_in_progress("file1")
        ps.clear_in_progress()
        assert ps.get_in_progress() is None

    def test_noop_when_nothing_in_progress(self, tmp_path):
        ps = PipelineState(state_file=str(tmp_path / "s.json"))
        ps.clear_in_progress()  # should not raise
        assert ps.get_in_progress() is None


# ── get_stats ───────────────────────────────────────────────────────────────

class TestGetStats:
    def test_stats_counts(self, tmp_path, state_fixtures_dir):
        sf = tmp_path / "state.json"
        shutil.copy(state_fixtures_dir / "existing_state.json", sf)
        ps = PipelineState(state_file=str(sf))
        stats = ps.get_stats()
        assert stats["processed_count"] == 2
        assert stats["failed_count"] == 1
        assert stats["in_progress"] is None


# ── Persistence ─────────────────────────────────────────────────────────────

class TestPersistence:
    def test_state_persists_across_instances(self, tmp_path):
        sf = str(tmp_path / "s.json")
        ps1 = PipelineState(state_file=sf)
        ps1.mark_completed("file_a")

        ps2 = PipelineState(state_file=sf)
        assert ps2.is_processed("file_a") is True

    def test_last_updated_set(self, tmp_path):
        sf = tmp_path / "s.json"
        ps = PipelineState(state_file=str(sf))
        ps.mark_completed("x")
        data = json.loads(sf.read_text())
        assert data["last_updated"] is not None
