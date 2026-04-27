"""Tests for the Runner — backend wiring, dispatch, cancel, orphan recovery.

The runner's poll loop is a simple thread that calls `_drain_once()` at fixed
intervals; we test `_drain_once()` and `_run_job()` directly rather than start
real threads. Backends are replaced with in-memory fakes that record calls.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from social_data_pipeline.jobs.backends.base import (
    BackendError,
    ExecutionHandle,
    ExecutionResult,
)
from social_data_pipeline.jobs.config import JobsConfig, Target
from social_data_pipeline.jobs.runner import Runner
from social_data_pipeline.jobs.store import Job, Store


# ── fakes ───────────────────────────────────────────────────────────────────


@dataclass
class FakeResult:
    rows: int = 1
    size_bytes: int = 100
    result_path: str = ""


class FakeBackend:
    """In-memory backend that records calls and returns a configurable result.

    Behaviour can be flipped with `mode`:
      "ok"       → execute() returns FakeResult
      "raise"    → execute() raises BackendError
      "crash"    → execute() raises a non-BackendError
      "cancel"   → execute() honors cancel by raising BackendError after the
                   on_handle callback (simulates a server-side cancel)
    """

    def __init__(self, mode: str = "ok"):
        self.mode = mode
        self.executed: list[Job] = []
        self.cancelled: list[ExecutionHandle] = []
        self.explained: list[str] = []
        self.list_db_calls = 0

    def execute(self, job: Job, timeout_seconds: int, on_handle):
        on_handle(ExecutionHandle(backend_pid=1234))
        self.executed.append(job)
        if self.mode == "raise":
            raise BackendError("backend rejected")
        if self.mode == "crash":
            raise RuntimeError("kaboom")
        if self.mode == "cancel":
            raise BackendError("query cancelled")
        return ExecutionResult(rows=10, size_bytes=512, result_path="/some/path")

    def cancel(self, handle: ExecutionHandle, job: Job | None = None):
        self.cancelled.append(handle)

    def explain(self, sql: str, job: Job | None = None) -> str:
        self.explained.append(sql)
        return "PLAN OK"

    def list_databases(self):
        self.list_db_calls += 1
        return [{"name": "x", "sizeOnDisk": 0, "empty": False}]

    def validate(self, job: Job) -> None:
        pass


def _cfg(tmp_path: Path, **overrides) -> JobsConfig:
    base = dict(
        port=8050,
        jobs_dir=tmp_path,
        result_root=tmp_path / "results",
        host_result_root=str(tmp_path / "results"),
        max_concurrent=1,
        default_timeouts={"postgres": 0, "starrocks": 259200, "mongodb": 0},
        history_retention=500,
        auth_enabled=False,
        targets={
            "warehouse": Target(name="warehouse", backend="postgres", database="datasets"),
            "olap": Target(name="olap", backend="starrocks", database=""),
            "docstore": Target(name="docstore", backend="mongodb", database=""),
        },
    )
    base.update(overrides)
    return JobsConfig(**base)


def _runner_with_fake_backends(tmp_path):
    cfg = _cfg(tmp_path)
    store = Store(jobs_dir=tmp_path)
    runner = Runner(cfg, store)
    fakes = {key: FakeBackend() for key in runner._backends}
    runner._backends = fakes
    return runner, store, fakes


def _job(target="warehouse", backend="postgres", **overrides) -> Job:
    base = dict(
        job_id=f"{backend[:2]}_{target}_test",
        target=target,
        backend=backend,
        sql="SELECT 1",
        output_filename="out.parquet",
        overwrite=False,
        submitted_at=time.time(),
    )
    base.update(overrides)
    return Job(**base)


# ── _build_backends + _backend_for ──────────────────────────────────────────


class TestBackendWiring:
    def test_builds_one_per_target(self, tmp_path):
        cfg = _cfg(tmp_path)
        store = Store(jobs_dir=tmp_path)
        runner = Runner(cfg, store)
        # Three targets → three backend instances, keyed by "<backend>:<target>".
        assert set(runner._backends.keys()) == {
            "postgres:warehouse",
            "starrocks:olap",
            "mongodb:docstore",
        }

    def test_backend_for_resolves_by_target(self, tmp_path):
        runner, _, _ = _runner_with_fake_backends(tmp_path)
        b = runner._backend_for(_job(target="warehouse", backend="postgres"))
        assert b is runner._backends["postgres:warehouse"]

    def test_backend_for_unknown_raises(self, tmp_path):
        runner, _, _ = _runner_with_fake_backends(tmp_path)
        with pytest.raises(BackendError, match="no backend"):
            runner._backend_for(_job(target="ghost", backend="postgres"))


# ── _run_job (synchronous run, replaces the pool worker call) ──────────────


def _approve_and_claim(store: Store, job: Job) -> Job:
    store.submit(job)
    store.approve(job.job_id)
    claimed = store.claim_approved()
    assert claimed is not None
    return claimed


class TestRunJobOk:
    def test_records_completion_and_metadata(self, tmp_path):
        runner, store, fakes = _runner_with_fake_backends(tmp_path)
        job = _approve_and_claim(store, _job())
        cancel_flag = threading.Event()

        runner._run_job(job, cancel_flag)

        # Job moved to history with status=done and metadata from ExecutionResult.
        hist = store.iter_history()
        assert hist[0].status == "done"
        assert hist[0].rows == 10
        assert hist[0].size_bytes == 512
        # Backend was called.
        assert len(fakes["postgres:warehouse"].executed) == 1


class TestRunJobErrors:
    def test_backend_error_marks_failed(self, tmp_path):
        runner, store, fakes = _runner_with_fake_backends(tmp_path)
        fakes["postgres:warehouse"].mode = "raise"
        job = _approve_and_claim(store, _job())

        runner._run_job(job, threading.Event())

        hist = store.iter_history()
        assert hist[0].status == "failed"
        assert "rejected" in (hist[0].error or "")

    def test_unexpected_exception_marks_failed_with_type(self, tmp_path):
        runner, store, fakes = _runner_with_fake_backends(tmp_path)
        fakes["postgres:warehouse"].mode = "crash"
        job = _approve_and_claim(store, _job())

        runner._run_job(job, threading.Event())

        hist = store.iter_history()
        assert hist[0].status == "failed"
        # Uncaught exceptions are recorded as "TypeName: message".
        assert hist[0].error.startswith("RuntimeError: ")

    def test_cancel_flag_set_during_backend_error_marks_cancelled(self, tmp_path):
        runner, store, fakes = _runner_with_fake_backends(tmp_path)
        fakes["postgres:warehouse"].mode = "raise"
        job = _approve_and_claim(store, _job())
        flag = threading.Event()
        flag.set()

        runner._run_job(job, flag)

        hist = store.iter_history()
        assert hist[0].status == "cancelled"

    def test_cancel_after_completion_wins_race(self, tmp_path):
        # execute() returned normally but cancel_flag was set in flight —
        # runner must mark cancelled rather than done.
        runner, store, _ = _runner_with_fake_backends(tmp_path)
        job = _approve_and_claim(store, _job())
        flag = threading.Event()
        flag.set()

        runner._run_job(job, flag)

        hist = store.iter_history()
        assert hist[0].status == "cancelled"


class TestRunJobCleanup:
    def test_removes_result_folder_on_failure(self, tmp_path):
        runner, store, fakes = _runner_with_fake_backends(tmp_path)
        fakes["postgres:warehouse"].mode = "raise"
        job = _approve_and_claim(store, _job())

        # Pre-populate the result folder to simulate a partial write.
        result_dir = store.job_result_dir(job.job_id)
        result_dir.mkdir(parents=True, exist_ok=True)
        (result_dir / "partial.parquet").write_bytes(b"partial")

        runner._run_job(job, threading.Event())

        assert not result_dir.exists()


# ── orphan recovery ─────────────────────────────────────────────────────────


class TestOrphanRecovery:
    def test_running_jobs_are_failed_on_recover(self, tmp_path):
        runner, store, _ = _runner_with_fake_backends(tmp_path)
        job = _approve_and_claim(store, _job())

        runner._recover_orphans()

        hist = store.iter_history()
        assert hist[0].job_id == job.job_id
        assert hist[0].status == "failed"
        assert "runner restarted" in hist[0].error

    def test_recover_with_no_orphans_is_noop(self, tmp_path):
        runner, store, _ = _runner_with_fake_backends(tmp_path)
        runner._recover_orphans()
        assert store.iter_history() == []


# ── _drain_once ─────────────────────────────────────────────────────────────


class TestDrainOnce:
    def test_drains_all_approved_jobs(self, tmp_path, monkeypatch):
        """With sync execution, the active dict empties between iterations,
        so a single _drain_once() walks all approved jobs."""
        cfg = _cfg(tmp_path, max_concurrent=2)
        store = Store(jobs_dir=tmp_path)
        runner = Runner(cfg, store)
        runner._backends = {key: FakeBackend() for key in runner._backends}

        from concurrent.futures import Future

        def submit_sync(fn, *args, **kwargs):
            fut: Future = Future()
            try:
                fut.set_result(fn(*args, **kwargs))
            except Exception as e:
                fut.set_exception(e)
            return fut

        monkeypatch.setattr(runner._pool, "submit", submit_sync)

        for i in range(3):
            j = _job(job_id=f"pg_{i:08x}")
            store.submit(j)
            store.approve(j.job_id)

        runner._drain_once()

        # All three drained → all in history, none still approved.
        assert len(store.iter_history()) == 3
        assert list(store.approved.glob("*.json")) == []

    def test_capacity_caps_active_set(self, tmp_path, monkeypatch):
        """With max_concurrent=1 and a non-completing future, the second
        approved job stays in approved/ until the first frees a slot."""
        cfg = _cfg(tmp_path, max_concurrent=1)
        store = Store(jobs_dir=tmp_path)
        runner = Runner(cfg, store)
        runner._backends = {key: FakeBackend() for key in runner._backends}

        from concurrent.futures import Future

        # Non-completing future — _deregister never fires.
        def submit_pending(fn, *args, **kwargs):
            return Future()

        monkeypatch.setattr(runner._pool, "submit", submit_pending)

        for i in range(2):
            j = _job(job_id=f"pg_{i:08x}")
            store.submit(j)
            store.approve(j.job_id)

        runner._drain_once()

        # Only one slot, one future in flight.
        assert len(runner.active_job_ids()) == 1
        # The other job sits in running/ (claimed by claim_approved) — wait,
        # actually claim_approved already moved it to running/. So with
        # max_concurrent=1 and the active dict full, the second should
        # never be claimed.
        running = list(store.running.glob("*.json"))
        approved = list(store.approved.glob("*.json"))
        assert len(running) == 1
        assert len(approved) == 1


# ── request_cancel + active_job_ids ─────────────────────────────────────────


class TestRequestCancel:
    def test_returns_false_for_unknown_job(self, tmp_path):
        runner, _, _ = _runner_with_fake_backends(tmp_path)
        assert runner.request_cancel("ghost") is False

    def test_calls_backend_cancel_for_active_job(self, tmp_path):
        runner, store, fakes = _runner_with_fake_backends(tmp_path)
        job = _approve_and_claim(store, _job())
        # Manually register as active without actually running.
        flag = threading.Event()
        from concurrent.futures import Future
        runner._active[job.job_id] = (Future(), flag)

        ok = runner.request_cancel(job.job_id)
        assert ok is True
        assert flag.is_set()
        assert len(fakes["postgres:warehouse"].cancelled) == 1

    def test_active_job_ids_lists_registered(self, tmp_path):
        runner, _, _ = _runner_with_fake_backends(tmp_path)
        from concurrent.futures import Future
        runner._active["a"] = (Future(), threading.Event())
        runner._active["b"] = (Future(), threading.Event())
        assert set(runner.active_job_ids()) == {"a", "b"}


# ── explain dispatch ────────────────────────────────────────────────────────


class TestExplain:
    def test_dispatches_to_backend(self, tmp_path):
        runner, store, fakes = _runner_with_fake_backends(tmp_path)
        job = _job()
        store.submit(job)

        plan = runner.explain(job.job_id)

        assert plan == "PLAN OK"
        assert fakes["postgres:warehouse"].explained == [job.sql]

    def test_unknown_job_raises(self, tmp_path):
        runner, _, _ = _runner_with_fake_backends(tmp_path)
        with pytest.raises(KeyError):
            runner.explain("ghost")

    def test_starrocks_explain_kwarg_compat(self, tmp_path):
        """SR backend's explain() doesn't accept `job=` kwarg in production —
        the dispatcher catches TypeError and retries without it. Verify the
        retry path with a backend whose explain() rejects the job kwarg.
        """
        runner, store, _ = _runner_with_fake_backends(tmp_path)

        class StrictBackend(FakeBackend):
            def explain(self, sql: str) -> str:  # no job kwarg
                self.explained.append(sql)
                return "STRICT PLAN"

        runner._backends["starrocks:olap"] = StrictBackend()
        job = _job(target="olap", backend="starrocks")
        store.submit(job)

        assert runner.explain(job.job_id) == "STRICT PLAN"


# ── list_mongo_databases dispatch ───────────────────────────────────────────


class TestListMongoDatabases:
    def test_dispatches_to_mongo_target(self, tmp_path):
        runner, _, fakes = _runner_with_fake_backends(tmp_path)
        out = runner.list_mongo_databases("docstore")
        assert out == [{"name": "x", "sizeOnDisk": 0, "empty": False}]
        assert fakes["mongodb:docstore"].list_db_calls == 1

    def test_unknown_target_raises(self, tmp_path):
        runner, _, _ = _runner_with_fake_backends(tmp_path)
        with pytest.raises(KeyError):
            runner.list_mongo_databases("ghost")
