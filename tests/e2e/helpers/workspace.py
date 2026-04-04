"""Workspace management for E2E tests.

Creates a fresh copy of the repo at /workspace for each test, so that
generated configs, .env, and data directories don't leak between tests.
"""

import shutil
import subprocess
from pathlib import Path

REPO = Path("/repo")
WORKSPACE = Path("/workspace")


def create_workspace():
    """Create /workspace from /repo via rsync.

    Excludes .git, data/, .env, and generated config files (config/db/,
    config/sources/, postgresql.local.conf, pg_hba.local.conf, docker-compose.override.yml).
    Copies test fixtures into the workspace so they're accessible to inner Docker volumes.
    """
    cleanup_workspace()
    WORKSPACE.mkdir(exist_ok=True)

    subprocess.run(
        [
            "rsync", "-a", "--delete",
            "--exclude", ".git",
            "--exclude", "data/",
            "--exclude", ".env",
            "--exclude", "config/db/",
            "--exclude", "config/sources/",
            "--exclude", "config/postgres/postgresql.local.conf",
            "--exclude", "config/postgres/pg_hba.local.conf",
            "--exclude", "docker-compose.override.yml",
            f"{REPO}/",
            f"{WORKSPACE}/",
        ],
        check=True,
        capture_output=True,
    )

    # Create data directories
    for subdir in ["dumps", "extracted", "parsed", "output", "database/postgres", "database/mongo"]:
        (WORKSPACE / "data" / subdir).mkdir(parents=True, exist_ok=True)

    # Ensure config/db and config/sources exist (empty)
    (WORKSPACE / "config" / "db").mkdir(parents=True, exist_ok=True)
    (WORKSPACE / "config" / "sources").mkdir(parents=True, exist_ok=True)

    # Copy test fixtures into the workspace so they're available to Docker volumes
    # (avoids cross-mount issues with /repo being a read-only bind mount)
    fixtures_src = REPO / "tests" / "fixtures"
    fixtures_dst = WORKSPACE / "tests" / "fixtures"
    if fixtures_src.exists():
        shutil.copytree(fixtures_src, fixtures_dst, dirs_exist_ok=True)


def cleanup_workspace():
    """Remove /workspace contents."""
    if WORKSPACE.exists():
        shutil.rmtree(WORKSPACE, ignore_errors=True)


def teardown_compose():
    """Stop all docker compose services and remove volumes."""
    subprocess.run(
        ["docker", "compose", "down", "--volumes", "--remove-orphans", "--timeout", "10"],
        cwd=WORKSPACE,
        capture_output=True,
        timeout=120,
    )
