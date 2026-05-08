"""Tests for AutoAcceptStore — eligibility math, clamping, persistence."""

from __future__ import annotations

import json
from pathlib import Path

from social_data_pipeline.jobs.auto_accept import AutoAcceptStore


def _store(tmp_path: Path, max_limit: int = 4) -> AutoAcceptStore:
    return AutoAcceptStore(state_path=tmp_path / "auto_accept.json", max_limit=max_limit)


# ── eligible_targets ────────────────────────────────────────────────────────


class TestEligibleTargets:
    def test_target_disabled_returns_empty(self, tmp_path):
        s = _store(tmp_path)
        s.set_target("warehouse", enabled=False, limit=2)
        assert s.eligible_targets({}, {}) == {}

    def test_target_unknown_in_state_is_skipped(self, tmp_path):
        # No target ever configured — eligible should not include it even
        # if the runner reports zero in-flight jobs for it.
        s = _store(tmp_path)
        assert s.eligible_targets({"warehouse": 0}, {}) == {}

    def test_limit_zero_is_filtered(self, tmp_path):
        s = _store(tmp_path)
        s.set_target("warehouse", enabled=True, limit=0)
        assert s.eligible_targets({}, {}) == {}

    def test_running_count_consumes_slots(self, tmp_path):
        s = _store(tmp_path)
        s.set_target("warehouse", enabled=True, limit=2)
        assert s.eligible_targets({"warehouse": 1}, {}) == {"warehouse": 1}

    def test_approved_count_consumes_slots(self, tmp_path):
        # Approved (not yet running) jobs must also count, otherwise
        # auto-accept would over-approve while the runner is busy on
        # other targets.
        s = _store(tmp_path)
        s.set_target("warehouse", enabled=True, limit=2)
        assert s.eligible_targets({}, {"warehouse": 1}) == {"warehouse": 1}

    def test_running_plus_approved_at_limit_returns_empty(self, tmp_path):
        s = _store(tmp_path)
        s.set_target("warehouse", enabled=True, limit=2)
        assert s.eligible_targets({"warehouse": 1}, {"warehouse": 1}) == {}

    def test_multiple_targets_independent(self, tmp_path):
        s = _store(tmp_path)
        s.set_target("warehouse", enabled=True, limit=2)
        s.set_target("olap", enabled=True, limit=3)
        out = s.eligible_targets({"warehouse": 1}, {"olap": 1})
        assert out == {"warehouse": 1, "olap": 2}

    def test_unknown_target_in_counts_does_not_leak(self, tmp_path):
        # Counts dict can include targets we don't have settings for
        # (e.g. a target that was just removed from config). They should
        # be ignored, not added to the eligible set.
        s = _store(tmp_path)
        s.set_target("warehouse", enabled=True, limit=2)
        out = s.eligible_targets({"ghost": 5, "warehouse": 0}, {})
        assert out == {"warehouse": 2}


# ── set_target clamping + first-touch defaults ──────────────────────────────


class TestSetTargetClamp:
    def test_clamps_above_max(self, tmp_path):
        s = _store(tmp_path, max_limit=4)
        s.set_target("t", enabled=True, limit=99)
        assert s.target_settings("t").limit == 4

    def test_clamps_negative_to_zero(self, tmp_path):
        s = _store(tmp_path, max_limit=4)
        s.set_target("t", enabled=True, limit=-3)
        assert s.target_settings("t").limit == 0

    def test_garbage_input_clamps_to_zero(self, tmp_path):
        s = _store(tmp_path, max_limit=4)
        s.set_target("t", enabled=True, limit="not-an-int")  # type: ignore[arg-type]
        assert s.target_settings("t").limit == 0

    def test_partial_update_preserves_other_field(self, tmp_path):
        s = _store(tmp_path, max_limit=4)
        s.set_target("t", enabled=True, limit=3)
        s.set_target("t", limit=2)  # only limit changes
        st = s.target_settings("t")
        assert st.enabled is True and st.limit == 2

    def test_max_limit_floor_is_one(self, tmp_path):
        # max_concurrent could be misconfigured at zero; clamp prevents
        # the slider from collapsing to a 0..0 range.
        s = _store(tmp_path, max_limit=0)
        s.set_target("t", limit=5)
        assert s.target_settings("t").limit == 1

    def test_first_toggle_uses_max_as_default_limit(self, tmp_path):
        # Enabling a never-touched target writes limit = max_limit, so the
        # operator gets full slot budget on first opt-in (matches the
        # slider's pre-render default).
        s = _store(tmp_path, max_limit=20)
        s.set_target("t", enabled=True)
        assert s.target_settings("t").limit == 20

    def test_target_settings_default_is_max_limit(self, tmp_path):
        # Reading a never-written target returns limit = max_limit so the
        # initial slider render matches the value used when the user
        # eventually flips the switch.
        s = _store(tmp_path, max_limit=20)
        assert s.target_settings("ghost").limit == 20
        assert s.target_settings("ghost").enabled is False


# ── persistence round-trip ──────────────────────────────────────────────────


class TestPersistence:
    def test_state_survives_reload(self, tmp_path):
        s = _store(tmp_path, max_limit=4)
        s.set_target("warehouse", enabled=True, limit=3)
        s.set_target("olap", enabled=False, limit=2)

        # Fresh store on the same path — loads from disk.
        s2 = _store(tmp_path, max_limit=4)
        st = s2.get_state()
        assert st.targets["warehouse"].enabled is True
        assert st.targets["warehouse"].limit == 3
        assert st.targets["olap"].enabled is False
        assert st.targets["olap"].limit == 2

    def test_missing_file_yields_defaults(self, tmp_path):
        s = _store(tmp_path)
        assert s.get_state().targets == {}

    def test_corrupt_file_yields_defaults(self, tmp_path):
        path = tmp_path / "auto_accept.json"
        path.write_text("not json {")
        s = AutoAcceptStore(state_path=path, max_limit=4)
        assert s.get_state().targets == {}

    def test_persist_is_atomic_replace(self, tmp_path):
        # After a write, no .tmp turds and the file content is valid JSON.
        s = _store(tmp_path)
        s.set_target("t", enabled=True)
        path = tmp_path / "auto_accept.json"
        assert path.exists()
        # No leftover temp files in the directory.
        leftover = [p for p in tmp_path.iterdir() if p.name.endswith(".tmp")]
        assert leftover == []
        # Valid JSON we can re-parse.
        json.loads(path.read_text())

    def test_clamps_on_load(self, tmp_path):
        # A state file written with a higher cap than the current process
        # uses (e.g. operator lowered max_concurrent in config) must be
        # clamped down on load — the slider's max is the new cap.
        path = tmp_path / "auto_accept.json"
        path.write_text(json.dumps({
            "targets": {"t": {"enabled": True, "limit": 99}},
        }))
        s = AutoAcceptStore(state_path=path, max_limit=4)
        assert s.target_settings("t").limit == 4

    def test_legacy_master_enabled_field_is_ignored(self, tmp_path):
        # Files written by the pre-removal version still have a
        # `master_enabled` key. We must load them without crashing and
        # silently drop the field.
        path = tmp_path / "auto_accept.json"
        path.write_text(json.dumps({
            "master_enabled": True,
            "targets": {"t": {"enabled": True, "limit": 2}},
        }))
        s = AutoAcceptStore(state_path=path, max_limit=4)
        assert s.target_settings("t").enabled is True
        assert s.target_settings("t").limit == 2
