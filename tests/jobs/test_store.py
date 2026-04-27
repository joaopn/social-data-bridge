"""Tests for the filesystem-backed job store.

The store is a thin state machine over directory renames:
    submit  → pending/<id>.json
    approve → approved/<id>.json
    claim_approved → running/<id>.json (oldest first)
    complete/fail/reject/cancel/mark_cancelled → history.jsonl + delete from phase

These tests cover transitions, lookup, listing, and history retrieval.
"""

from __future__ import annotations

import json
import time

import pytest

from social_data_pipeline.jobs.store import Job, Store, TERMINAL_STATUSES


def _job(job_id: str = "pg_aaaa1111", **overrides) -> Job:
    base = dict(
        job_id=job_id,
        target="local",
        backend="postgres",
        sql="SELECT 1",
        output_filename="out.parquet",
        overwrite=False,
        submitted_at=time.time(),
    )
    base.update(overrides)
    return Job(**base)


@pytest.fixture
def store(tmp_path):
    return Store(jobs_dir=tmp_path)


# ── Job dataclass ────────────────────────────────────────────────────────────


class TestJob:
    def test_roundtrip_to_from_dict(self):
        j = _job(description="hello", rows=42)
        d = j.to_dict()
        # Round-trip preserves known fields
        back = Job.from_dict(d)
        assert back == j

    def test_from_dict_promotes_unknown_keys_to_extras(self):
        # Future-compat: a stored JSON with a field this version doesn't know
        # about must round-trip without exploding.
        d = _job().to_dict()
        d["surprise"] = "value"
        j = Job.from_dict(d)
        assert j.extras == {"surprise": "value"}

    def test_from_dict_ignores_no_unknown(self):
        j = Job.from_dict(_job().to_dict())
        assert j.extras == {}


# ── new_job_id ───────────────────────────────────────────────────────────────


class TestNewJobId:
    def test_postgres_prefix(self):
        assert Store.new_job_id("postgres").startswith("pg_")

    def test_starrocks_prefix(self):
        assert Store.new_job_id("starrocks").startswith("sr_")

    def test_mongodb_prefix(self):
        assert Store.new_job_id("mongodb").startswith("mg_")

    def test_unknown_backend_falls_back(self):
        assert Store.new_job_id("hypergraph").startswith("job_")

    def test_ids_are_unique(self):
        ids = {Store.new_job_id("postgres") for _ in range(50)}
        assert len(ids) == 50


# ── submit + approve + claim_approved ────────────────────────────────────────


class TestHappyPath:
    def test_submit_writes_pending(self, store):
        j = _job()
        store.submit(j)
        path = store.pending / f"{j.job_id}.json"
        assert path.exists()
        assert json.loads(path.read_text())["status"] == "pending"

    def test_approve_moves_pending_to_approved(self, store):
        j = _job()
        store.submit(j)
        approved = store.approve(j.job_id)
        assert approved.status == "approved"
        assert approved.approved_at is not None
        assert not (store.pending / f"{j.job_id}.json").exists()
        assert (store.approved / f"{j.job_id}.json").exists()

    def test_claim_approved_picks_oldest(self, store, monkeypatch):
        # Submit + approve in deterministic order. claim_approved sorts by
        # mtime, so we control mtime explicitly.
        old = _job("pg_old00001")
        new = _job("pg_new00002")
        store.submit(old)
        store.approve(old.job_id)
        store.submit(new)
        store.approve(new.job_id)

        # Make `old` strictly older than `new`.
        old_path = store.approved / f"{old.job_id}.json"
        new_path = store.approved / f"{new.job_id}.json"
        import os
        now = time.time()
        os.utime(old_path, (now - 60, now - 60))
        os.utime(new_path, (now, now))

        claimed = store.claim_approved()
        assert claimed is not None
        assert claimed.job_id == old.job_id
        assert claimed.status == "running"
        assert claimed.started_at is not None
        assert (store.running / f"{old.job_id}.json").exists()
        assert not (store.approved / f"{old.job_id}.json").exists()

    def test_claim_approved_returns_none_when_empty(self, store):
        assert store.claim_approved() is None

    def test_complete_retires_running_to_history(self, store):
        j = _job()
        store.submit(j)
        store.approve(j.job_id)
        running = store.claim_approved()
        store.complete(running)

        assert running.status == "done"
        assert running.finished_at is not None
        assert not (store.running / f"{j.job_id}.json").exists()

        hist = store.iter_history()
        assert len(hist) == 1
        assert hist[0].job_id == j.job_id
        assert hist[0].status == "done"


# ── failure / cancel / reject paths ─────────────────────────────────────────


class TestTerminalTransitions:
    def test_fail_records_error(self, store):
        j = _job()
        store.submit(j)
        store.approve(j.job_id)
        running = store.claim_approved()
        store.fail(running, error="boom")

        assert running.status == "failed"
        assert running.error == "boom"
        hist = store.iter_history()
        assert hist[0].status == "failed"
        assert hist[0].error == "boom"

    def test_reject_pending(self, store):
        j = _job()
        store.submit(j)
        store.reject(j.job_id, reason="not allowed")

        assert not (store.pending / f"{j.job_id}.json").exists()
        hist = store.iter_history()
        assert hist[0].status == "rejected"
        assert hist[0].reject_reason == "not allowed"

    def test_cancel_pending(self, store):
        j = _job()
        store.submit(j)
        store.cancel_pending(j.job_id)

        assert not (store.pending / f"{j.job_id}.json").exists()
        hist = store.iter_history()
        assert hist[0].status == "cancelled"

    def test_mark_cancelled_running(self, store):
        j = _job()
        store.submit(j)
        store.approve(j.job_id)
        running = store.claim_approved()
        store.mark_cancelled(running, reason="user clicked kill")

        assert running.status == "cancelled"
        assert running.error == "user clicked kill"
        hist = store.iter_history()
        assert hist[0].status == "cancelled"

    def test_terminal_statuses_set_is_complete(self):
        # Documents what `_retire` is for. Any new terminal state must be
        # added here AND ensure the transition method calls `_retire`.
        assert TERMINAL_STATUSES == frozenset({"done", "failed", "rejected", "cancelled"})


# ── error paths ─────────────────────────────────────────────────────────────


class TestErrorPaths:
    def test_approve_missing_job(self, store):
        with pytest.raises(KeyError):
            store.approve("does_not_exist")

    def test_reject_missing_job(self, store):
        with pytest.raises(KeyError):
            store.reject("does_not_exist")

    def test_cancel_pending_missing_job(self, store):
        with pytest.raises(KeyError):
            store.cancel_pending("does_not_exist")


# ── lookup ──────────────────────────────────────────────────────────────────


class TestFind:
    def test_find_in_pending(self, store):
        j = _job()
        store.submit(j)
        phase, found = store.find(j.job_id)
        assert phase == "pending"
        assert found.job_id == j.job_id

    def test_find_in_approved(self, store):
        j = _job()
        store.submit(j)
        store.approve(j.job_id)
        phase, _ = store.find(j.job_id)
        assert phase == "approved"

    def test_find_in_running(self, store):
        j = _job()
        store.submit(j)
        store.approve(j.job_id)
        store.claim_approved()
        phase, _ = store.find(j.job_id)
        assert phase == "running"

    def test_find_in_history(self, store):
        j = _job()
        store.submit(j)
        store.approve(j.job_id)
        running = store.claim_approved()
        store.complete(running)
        phase, found = store.find(j.job_id)
        assert phase == "history"
        assert found.status == "done"

    def test_find_not_found(self, store):
        assert store.find("missing") is None


# ── listing ─────────────────────────────────────────────────────────────────


class TestListPhase:
    def test_list_pending_sorted_by_mtime(self, store, tmp_path):
        import os
        a = _job("pg_aaa00001")
        b = _job("pg_bbb00002")
        store.submit(a)
        store.submit(b)
        # Deterministic order: a is older.
        now = time.time()
        os.utime(store.pending / f"{a.job_id}.json", (now - 10, now - 10))
        os.utime(store.pending / f"{b.job_id}.json", (now, now))

        out = store.list_phase("pending")
        assert [j.job_id for j in out] == [a.job_id, b.job_id]

    def test_list_phase_skips_corrupt_files(self, store):
        j = _job()
        store.submit(j)
        bad = store.pending / "bad.json"
        bad.write_text("not json")
        out = store.list_phase("pending")
        # Corrupt file silently skipped, valid one returned.
        assert [x.job_id for x in out] == [j.job_id]

    def test_orphaned_running_returns_running_jobs(self, store):
        j = _job()
        store.submit(j)
        store.approve(j.job_id)
        store.claim_approved()
        orphans = store.orphaned_running()
        assert [x.job_id for x in orphans] == [j.job_id]


# ── history ─────────────────────────────────────────────────────────────────


class TestHistory:
    def test_iter_history_most_recent_first(self, store):
        for i in range(3):
            j = _job(f"pg_{i:08x}")
            store.submit(j)
            store.approve(j.job_id)
            running = store.claim_approved()
            store.complete(running)

        hist = store.iter_history()
        # Most-recent-first means the last completed (i=2) appears first.
        assert hist[0].job_id == "pg_00000002"
        assert hist[-1].job_id == "pg_00000000"

    def test_iter_history_respects_limit(self, store):
        for i in range(5):
            j = _job(f"pg_{i:08x}")
            store.submit(j)
            store.approve(j.job_id)
            running = store.claim_approved()
            store.complete(running)

        hist = store.iter_history(limit=2)
        assert len(hist) == 2

    def test_iter_history_skips_invalid_lines(self, store):
        j = _job()
        store.submit(j)
        store.approve(j.job_id)
        running = store.claim_approved()
        store.complete(running)

        # Inject a corrupt line at the end of history.jsonl. iter_history reads
        # in reverse, so this is what it sees first.
        with open(store.history, "a") as f:
            f.write("not json\n")
        hist = store.iter_history()
        assert len(hist) == 1
        assert hist[0].job_id == j.job_id


# ── update_running ──────────────────────────────────────────────────────────


class TestUpdateRunning:
    def test_persists_handle_metadata(self, store):
        j = _job()
        store.submit(j)
        store.approve(j.job_id)
        running = store.claim_approved()
        running.backend_pid = 12345
        store.update_running(running)

        # Re-read from disk
        path = store.running / f"{j.job_id}.json"
        on_disk = Job.from_dict(json.loads(path.read_text()))
        assert on_disk.backend_pid == 12345
