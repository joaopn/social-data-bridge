# Testing

## CI

Two GitHub Actions workflows run on every push and PR (path-filtered):

| Workflow | File | Trigger | Purpose |
|----------|------|---------|---------|
| ShellCheck | `.github/workflows/shellcheck.yml` | `**/*.sh` changes | Lint entrypoint scripts in `config/` |
| Tests | `.github/workflows/tests.yml` | `**/*.py`, `requirements*.txt`, `tests/**`, `config/**/*.yaml` | Run unit test suite (Python 3.13) |

Both also support `workflow_dispatch` for manual runs. All actions are pinned to full commit SHAs for supply chain hardening.

CI installs only `requirements-test.txt`, which is self-contained (includes project deps `pyyaml`, `polars`, `pyarrow` — excludes heavy runtime deps like `lingua` and `psycopg` that no test imports). Pytest output goes to the Actions step log directly (no artifact upload).

## Running tests locally

Install test dependencies:

```bash
pip install -r requirements-test.txt
```

All commands require `--override-ini="pythonpath=."` so pytest can import the project package.

```bash
# Run all tests (with local log file)
pytest --override-ini="pythonpath=." -v 2>&1 | tee test-results.log

# Skip slow decompression tests
pytest --override-ini="pythonpath=." -m "not slow" -v 2>&1 | tee test-results.log
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

## Swapping test data

To test against different fixture data (e.g., a new upstream Reddit schema):

1. Drop the new NDJSON into `tests/fixtures/reddit/`
2. Run tests — structural checks catch column/type drift, row count mismatches
3. No Python test code changes needed
