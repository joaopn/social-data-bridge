# Testing

## Setup

Install test dependencies (on top of the project's own deps: `polars`, `pyarrow`, `pyyaml`):

```bash
pip install -r requirements-test.txt
```

## Running tests

All commands require `--override-ini="pythonpath=."` so pytest can import the project package.

```bash
# Run all tests
pytest --override-ini="pythonpath=." 2>&1 | tee test-results.log

# Skip slow decompression tests
pytest --override-ini="pythonpath=." -m "not slow" 2>&1 | tee test-results.log
```

## Test structure

```
tests/
    conftest.py              # Root fixtures (paths, NDJSON helpers, marker registration)
    fixtures/                # Test data files
        reddit/              # NDJSON dumps, compressed files
        custom/              # NDJSON, CSV, Parquet
        config/              # Platform and pipeline YAML configs
        state/               # Pipeline state JSON files
    core/                    # config, parser, state, decompress
    platforms/               # Reddit and custom parser end-to-end
    orchestrators/           # File detection functions
    setup/                   # CLI input helpers (mocked stdin)
    e2e/                     # (TODO) YAML-driven DB end-to-end tests
        cases/               # YAML assertion definitions (ready)
```

## What's tested

**Unit tests** (`tests/core/`, `tests/platforms/`, `tests/setup/`, `tests/orchestrators/`) test pure logic — config merging, type enforcement, deletion waterfall, base36 conversion, decompression, file detection, CLI input helpers. No Docker needed.

## Planned work

- **CI workflow**: Unit tests and static checks (ruff, shellcheck, hadolint) will move to a GitHub Actions workflow. See `tests/PLAN_CI.md`.
- **DinD E2E tests**: Full pipeline testing in sandboxed containers. See `tests/PLAN_E2E.md`.

## Swapping test data

To test against different fixture data (e.g., a new upstream Reddit schema):

1. Drop the new NDJSON into `tests/fixtures/reddit/`
2. Run tests — structural checks catch column/type drift, row count mismatches
3. No Python test code changes needed
