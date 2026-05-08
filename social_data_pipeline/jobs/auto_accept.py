"""Runtime state for the web UI's auto-accept feature.

Auto-accept lets the operator opt in, per target, to having the runner
approve pending jobs automatically up to a configurable cap (FIFO).
State is persisted as JSON under ``<jobs_dir>/auto_accept.json`` so the
toggles survive container restarts. Setup config
(``config/jobs/config.yaml``) stays untouched — this is mutable runtime
state, written from the web handlers.

The eligibility computation is a pure function (``eligible_targets``) so
it can be tested without touching the filesystem; the runner calls it
once per poll tick to decide which pending jobs to approve next.
"""

from __future__ import annotations

import copy
import json
import os
import tempfile
import threading
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any


@dataclass
class TargetAutoAccept:
    enabled: bool = False
    limit: int = 0  # populated with max_limit on first write per target


@dataclass
class AutoAcceptState:
    targets: dict[str, TargetAutoAccept] = field(default_factory=dict)


class AutoAcceptStore:
    """Filesystem-backed auto-accept settings.

    Atomic write via ``<file>.tmp + os.replace`` (mirrors ``Store._write_json``).
    A single ``threading.Lock`` serializes concurrent writes from the web
    handlers and the runner thread; reads return deep-copied snapshots so
    callers can iterate without holding the lock.
    """

    def __init__(self, state_path: Path, max_limit: int):
        self.state_path = Path(state_path)
        # Slider values can never exceed the runner's hard concurrency cap.
        # Negative or zero would make the slider useless; clamp to >=1 so
        # the UI always has a meaningful range.
        self.max_limit = max(1, int(max_limit))
        self._lock = threading.Lock()
        self._state = self._load()

    # ------------------------------------------------------------------
    # public surface

    def get_state(self) -> AutoAcceptState:
        with self._lock:
            return copy.deepcopy(self._state)

    def set_target(
        self,
        name: str,
        *,
        enabled: bool | None = None,
        limit: int | None = None,
    ) -> AutoAcceptState:
        """Partial update for one target's settings.

        ``limit`` is clamped into ``[0, self.max_limit]``. A clamped value
        is silently accepted so a stale browser tab posting an
        out-of-range value doesn't 500 the request — the slider's max is
        a soft contract, the cap is the source of truth.

        First-time write defaults the limit to ``self.max_limit`` so a
        target enabled for the first time uses the full slot budget
        rather than the dataclass default (0).
        """
        with self._lock:
            current = self._state.targets.get(name)
            if current is None:
                current = TargetAutoAccept(limit=self.max_limit)
            if enabled is not None:
                current.enabled = bool(enabled)
            if limit is not None:
                current.limit = self._clamp_limit(limit)
            self._state.targets[name] = current
            self._persist()
            return copy.deepcopy(self._state)

    def target_settings(self, name: str) -> TargetAutoAccept:
        """Current settings for a target, or defaults if never written.

        First-touch default surfaces ``self.max_limit`` so the slider
        renders at its top end before the user has opted in.
        """
        with self._lock:
            t = self._state.targets.get(name)
            if t is not None:
                return copy.deepcopy(t)
            return TargetAutoAccept(limit=self.max_limit)

    def eligible_targets(
        self,
        running_counts: dict[str, int],
        approved_counts: dict[str, int],
    ) -> dict[str, int]:
        """Pure: how many more jobs each target is allowed to auto-approve.

        Returns ``{target_name: remaining_slots}``. A target is included
        only when its per-target switch is on and
        ``limit - (running + approved) > 0``. Targets that aren't in the
        state file at all (default ``enabled=False``) are skipped.
        """
        with self._lock:
            out: dict[str, int] = {}
            for name, t in self._state.targets.items():
                if not t.enabled:
                    continue
                in_flight = int(running_counts.get(name, 0)) + int(
                    approved_counts.get(name, 0)
                )
                slots = int(t.limit) - in_flight
                if slots > 0:
                    out[name] = slots
            return out

    # ------------------------------------------------------------------
    # internals

    def _clamp_limit(self, value: int) -> int:
        try:
            v = int(value)
        except (TypeError, ValueError):
            return 0
        if v < 0:
            return 0
        if v > self.max_limit:
            return self.max_limit
        return v

    def _load(self) -> AutoAcceptState:
        if not self.state_path.exists():
            return AutoAcceptState()
        try:
            raw = json.loads(self.state_path.read_text())
        except (OSError, json.JSONDecodeError):
            # Treat a corrupt file as "no settings" — the user can re-toggle.
            # The next persist() will overwrite with valid JSON.
            return AutoAcceptState()
        # Older state files may include `master_enabled` (pre-removal); it
        # is silently ignored on load so existing deployments don't break.
        targets_raw = raw.get("targets") or {}
        targets: dict[str, TargetAutoAccept] = {}
        for name, spec in targets_raw.items():
            if not isinstance(spec, dict):
                continue
            targets[name] = TargetAutoAccept(
                enabled=bool(spec.get("enabled", False)),
                limit=self._clamp_limit(spec.get("limit", self.max_limit)),
            )
        return AutoAcceptState(targets=targets)

    def _persist(self) -> None:
        data: dict[str, Any] = {
            "targets": {
                name: asdict(t) for name, t in self._state.targets.items()
            },
        }
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(
            prefix=self.state_path.name + ".",
            suffix=".tmp",
            dir=str(self.state_path.parent),
        )
        try:
            with os.fdopen(fd, "w") as f:
                json.dump(data, f, separators=(",", ":"))
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self.state_path)
        except Exception:
            try:
                os.unlink(tmp)
            except FileNotFoundError:
                pass
            raise
