"""E2E test fixtures — workspace lifecycle and helpers.

Each test function gets a fresh /workspace (copy of repo), runs real
sdp.py commands, and tears down all docker compose services afterward.
"""

import pytest

from tests.e2e.helpers.workspace import create_workspace, cleanup_workspace, teardown_compose, WORKSPACE


@pytest.fixture(autouse=True)
def workspace():
    """Create a fresh workspace before each test, teardown after."""
    create_workspace()
    yield WORKSPACE
    teardown_compose()
    cleanup_workspace()
