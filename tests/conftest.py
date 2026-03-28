import json
import pytest
from pathlib import Path


def pytest_configure(config):
    config.addinivalue_line("markers", "slow: tests that take >5s (decompression, DB)")
    config.addinivalue_line("markers", "postgres: requires PostgreSQL container")
    config.addinivalue_line("markers", "mongo: requires MongoDB container")


@pytest.fixture
def fixtures_dir():
    return Path(__file__).parent / "fixtures"

@pytest.fixture
def reddit_fixtures_dir(fixtures_dir):
    return fixtures_dir / "reddit"

@pytest.fixture
def custom_fixtures_dir(fixtures_dir):
    return fixtures_dir / "custom"

@pytest.fixture
def config_fixtures_dir(fixtures_dir):
    return fixtures_dir / "config"

@pytest.fixture
def state_fixtures_dir(fixtures_dir):
    return fixtures_dir / "state"

def load_ndjson(path):
    """Load NDJSON file into list of dicts."""
    records = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records

def count_ndjson_lines(path):
    """Count non-empty lines in an NDJSON file."""
    count = 0
    with open(path) as f:
        for line in f:
            if line.strip():
                count += 1
    return count
