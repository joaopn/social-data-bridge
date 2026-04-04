# Testing

## CI

Two GitHub Actions workflows run on every push and PR (path-filtered):

| Workflow | File | Trigger | Purpose |
|----------|------|---------|---------|
| ShellCheck | `.github/workflows/shellcheck.yml` | `**/*.sh` changes | Lint entrypoint scripts in `config/` |
| Tests | `.github/workflows/tests.yml` | `**/*.py`, `requirements*.txt`, `tests/**`, `config/**/*.yaml` | Run unit test suite (Python 3.13) |

Both also support `workflow_dispatch` for manual runs. All actions are pinned to full commit SHAs for supply chain hardening.

CI installs only `requirements-test.txt`, which is self-contained (includes project deps `pyyaml`, `polars`, `pyarrow` — excludes heavy runtime deps like `lingua` and `psycopg` that no test imports). Pytest output goes to the Actions step log directly (no artifact upload). E2E tests are excluded from CI (`--ignore=tests/e2e`) — they require sysbox and run locally only.

## Running unit tests locally

Install test dependencies:

```bash
pip install -r requirements-test.txt
```

All commands require `--override-ini="pythonpath=."` so pytest can import the project package.

```bash
# Run all unit tests (with local log file)
pytest --override-ini="pythonpath=." --ignore=tests/e2e -v 2>&1 | tee test-results.log

# Skip slow decompression tests
pytest --override-ini="pythonpath=." --ignore=tests/e2e -m "not slow" -v 2>&1 | tee test-results.log
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
    e2e/                     # E2E tests (local only, requires sysbox)
        Dockerfile.e2e       # Sysbox container image
        run.sh               # Host-side runner
        questions.yaml       # Question dictionary (tagged prompt docs)
        conftest.py          # Per-test workspace lifecycle
        helpers/             # pexpect wrapper, fixtures, DB assertion helpers
        tests/               # Test scenarios (parse, ingest, auth, idempotency)
```

## Unit tests

Unit tests cover pure logic with no Docker or database dependencies. All external I/O (stdin, filesystem) is mocked or uses small fixture files.

| Directory | What's tested |
|-----------|---------------|
| `core/` | YAML config loading and deep merge, type enforcement and CSV escaping, pipeline state resume/recovery, multi-format decompression (.zst, .gz, .xz, .tar.gz) |
| `platforms/` | Reddit parser: base36 ID conversion, deletion waterfall algorithm, field extraction. Custom parser: dot-notation field access, CSV/Parquet input, NDJSON→Parquet/CSV output |
| `orchestrators/` | File detection: regex pattern matching against file lists, data type routing |
| `setup/` | CLI input helpers (`ask`, `ask_bool`, `ask_choice`, `ask_multi_select`, etc.) with mocked stdin. Glob-to-regex conversion, file pattern derivation |

Markers: `@pytest.mark.slow` for decompression tests (>5s), `@pytest.mark.postgres` and `@pytest.mark.mongo` registered but not yet used.

## E2E tests

E2E tests exercise the real pipeline inside a sysbox Docker-in-Docker container. Nothing is mocked — each test runs real `sdp.py` commands, builds real Docker images, starts real databases, and verifies results by querying the database and reading output files.

### How it works

1. `run.sh` builds a sysbox container image with Python 3.13, docker compose, and all test deps (pexpect, psycopg, pymongo, polars)
2. The host repo is bind-mounted read-only at `/repo` inside the sysbox container
3. Each test gets a fresh `/workspace` (rsync copy of the repo, excluding `.git` and generated files)
4. Interactive commands (`sdp db setup`, `sdp source add`) are driven by pexpect, which matches prompts by their `[tag_id]` prefix (enabled via `sdp.py --tag`). Answers are provided as a simple `{tag: answer}` dict per test
5. Non-interactive commands (`sdp run parse`, `sdp db start`) use subprocess directly
6. Verification uses psycopg (PostgreSQL), pymongo (MongoDB), and polars (CSV/Parquet file reads)
7. Teardown runs `docker compose down --volumes` and removes `/workspace`

### Tagged prompt automation

Every interactive prompt in `sdp.py` setup flows has a stable `tag=` identifier (e.g., `db_data_path`, `src_profiles`). When `--tag` is passed, prompts are prefixed with `[tag_id]`, allowing pexpect to match by tag rather than prompt text. This makes tests robust to prompt reordering or rewording. Tags are documented in `tests/e2e/questions.yaml`.

### Running E2E tests

Requires [sysbox](https://github.com/nestybox/sysbox) installed on the host.

```bash
./tests/e2e/run.sh               # Run all E2E tests
./tests/e2e/run.sh -k parse      # Run tests matching "parse"
./tests/e2e/run.sh -x            # Stop on first failure
```

First run builds Docker images inside the sysbox container (~3-5 min). Subsequent runs use the Docker layer cache (~5-10 min total for the full suite).

### Test scenarios

| Test | What it exercises |
|------|-------------------|
| `test_parse_reddit` | `db setup` → `source add reddit` → compress fixtures to .zst → `run parse` → verify CSV output (row counts, mandatory columns, field presence) |
| `test_parse_custom` | `db setup` → `source add` custom platform → place NDJSON in extracted/ → `run parse` → verify Parquet output (row counts, dot-notation field resolution) |
| `test_postgres_flow` | Full PostgreSQL lifecycle: setup → add → parse → `db start` → `run postgres_ingest` → verify schema, row counts, no duplicate IDs, indexes, column types |
| `test_mongo_flow` | Full MongoDB lifecycle: setup → add → `db start` → `run mongo_ingest` → verify database, collections, document counts |

## Swapping test data

To test against different fixture data (e.g., a new upstream Reddit schema):

1. Drop the new NDJSON into `tests/fixtures/reddit/`
2. Run tests — structural checks catch column/type drift, row count mismatches
3. No Python test code changes needed
