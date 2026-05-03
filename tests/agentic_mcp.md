# Agentic MCP Test Suite

This document is a runnable checklist for an LLM agent (e.g. Claude Code) to exercise the MCP servers shipped with this repo:

- **`postgres`** — `crystaldba/postgres-mcp`, SSE on `POSTGRES_MCP_PORT` (default 8002), `/sse`
- **`mongodb`** — patched `kiliczsh/mcp-mongo-server`, streamable HTTP on `MONGO_MCP_PORT` (default 3002), `/mcp`
- **`starrocks`** — `mcp-server-starrocks`, streamable HTTP on `STARROCKS_MCP_PORT` (default 9002), `/mcp`
- **`scheduler`** (a.k.a. `jobs`) — the SDP query scheduler, streamable HTTP on `JOBS_PORT` (default 8050), `/mcp`

## How to run this suite

1. The MCP client wiring is **the human's job** — this doc assumes the agent already has the four MCP servers connected and their tools visible. If a tool listed below is missing from the agent's tool inventory, stop and report it; do not try to "fix" the connection.
2. Walk the phases below in order. Phases 1–4 exercise each MCP in isolation; Phase 5 exercises the scheduler end-to-end and **requires a human to approve queries in the jobs UI** (`http://<host>:<JOBS_PORT>/`). The agent must not skip these — pause and wait.
3. For each task: run the listed tool calls verbatim (substituting placeholders), then check the **Pass criteria** before moving on. On a failure, record the tool, args, and observed response, then continue with the rest of the phase so the human gets a complete report.
4. At the end, write a single summary block grouped by MCP: `pass / fail / skipped`, with one-line evidence per task (tool name + key field from the response).

**On Phase 5 polling.** Most agent harnesses can't sleep for minutes at a time, and the scheduler MCP has no "wait for job to finish" call. The expected pattern is: submit, tell the human exactly which `job_id`s need approval, then either poll `query_status` periodically or pause and ask the human to say "approved" / "all ran" before re-checking. Pausing for the human is normal, not a stuck suite.

**The agent must not:**
- Attempt write operations against any DB MCP except where Phase 6 explicitly tells it to (write attempts are checked for *correct rejection*, not for landing data).
- Approve its own scheduler jobs. Approval is the human's hand on the wheel — this is the entire point of the scheduler.
- Drop, truncate, or alter user data. The fixtures created in Phase 5 are scoped to a throwaway database/schema; if those names already exist, stop and ask.
- Invent table/database/collection names. Use `list_*` / `db_summary` / `list_objects` to discover what's actually there before issuing queries.

## Glossary of placeholders

| Placeholder | Meaning | How to resolve |
|---|---|---|
| `<PG_DB>` | A PostgreSQL database with at least one schema and table | `mcp__postgres__list_objects` (object_type=`schema`), pick a non-system schema, then `list_objects` for tables in it |
| `<PG_SCHEMA>` | Non-system schema in `<PG_DB>` | same |
| `<PG_TABLE>` | Any table in `<PG_SCHEMA>` | same |
| `<MONGO_DB>` | A user MongoDB database (skip `admin`, `config`, `local`) | `mcp__mongodb__listDatabases` |
| `<MONGO_COLL>` | A user collection in `<MONGO_DB>` (skip `_sdp_metadata` and any other collection starting with `_sdp_` — those are pipeline-internal bookkeeping) | `mcp__mongodb__listCollections database=<MONGO_DB>` |
| `<SR_DB>` | A user StarRocks database (skip `_statistics_`, `information_schema`, `sys`) | `mcp__starrocks__db_summary` (or `read_query` against `information_schema.schemata`) |
| `<SR_TABLE>` | A table in `<SR_DB>` | `mcp__starrocks__table_overview database=<SR_DB>` |
| `<PG_TARGET>` / `<SR_TARGET>` / `<MONGO_TARGET>` | Configured scheduler target names | Call `mcp__scheduler__list_targets` (preferred). Fall back to scraping the `Configured targets:` line in each `submit_*` tool's description. |

If any placeholder cannot be resolved (no user data exists yet for that backend), mark the dependent tasks **skipped — no fixtures** rather than fabricating names.

---

## Phase 1 — PostgreSQL MCP

**What this catches:** broken `entrypoint-postgres.sh` credential plumbing, port not bound, `crystaldba/postgres-mcp` access-mode regressions, schema-discovery rot.

### 1.1 Connectivity & schema discovery
- Call `mcp__postgres__list_schemas`.
- Call `mcp__postgres__list_objects` with `object_type="table"` for one user schema.
- **Pass:** both return non-empty lists. Record the schema name as `<PG_SCHEMA>` and one table as `<PG_TABLE>` for later tasks.

### 1.2 Object detail
- Call `mcp__postgres__get_object_details` with `schema_name=<PG_SCHEMA>`, `name=<PG_TABLE>`, `object_type="table"`.
- **Pass:** response includes column list and at least one column with a real type (not all `unknown`).

### 1.3 Read query
- Call `mcp__postgres__execute_sql` with `sql="SELECT count(*) AS n FROM <PG_SCHEMA>.<PG_TABLE>"`.
- **Pass:** returns a single row with an integer `n` ≥ 0.

### 1.4 EXPLAIN
- Call `mcp__postgres__explain_query` with the same SQL as 1.3.
- **Pass:** plan text mentions a node type (`Seq Scan`, `Aggregate`, `Index Scan`, etc.) for `<PG_TABLE>`.

### 1.5 Read-only enforcement (NEGATIVE TEST)
The MCP runs in `restricted` access mode (read-only transactions). All writes must be refused.
- Call `mcp__postgres__execute_sql` with `sql="CREATE TABLE <PG_SCHEMA>.__mcp_probe__ (id int)"`.
- **Pass:** the call returns an error mentioning read-only / permission denied / restricted. **No table is created** (verify with `list_objects`).
- Repeat with `sql="INSERT INTO <PG_SCHEMA>.<PG_TABLE> SELECT * FROM <PG_SCHEMA>.<PG_TABLE> LIMIT 0"` — must also be refused.

### 1.6 Health probe (read-only diagnostic)
- Call `mcp__postgres__analyze_db_health`.
- **Pass:** returns a structured response (no exception). Record the top-level keys for the report.

---

## Phase 2 — MongoDB MCP

**What this catches:** broken `entrypoint-mongo.sh` credential plumbing, `MONGO_MCP_READ_ONLY=true` not honored, the multi-DB patch (custom `database` param + `listDatabases`) regressing.

### 2.1 Server info & listing
- Call `mcp__mongodb__serverInfo`.
- Call `mcp__mongodb__listDatabases`.
- **Pass:** `serverInfo` returns a Mongo version. `listDatabases` returns at least one non-system database. Record one as `<MONGO_DB>`.

### 2.2 Multi-DB patch — collections via `database` param
- Call `mcp__mongodb__listCollections` with `database=<MONGO_DB>`.
- **Pass:** returns the collections in `<MONGO_DB>`, **not** in whichever DB the connection URI defaults to. Pick one as `<MONGO_COLL>`.
- *If the tool rejects the `database` parameter, the multi-DB patch is broken — flag this loudly. The patch is at `config/mcp/mongo-mcp-multidb.patch`.*

### 2.3 Count & query
- Call `mcp__mongodb__count` with `database=<MONGO_DB>`, `collection=<MONGO_COLL>`, `query={}`.
- Call `mcp__mongodb__query` with `database=<MONGO_DB>`, `collection=<MONGO_COLL>`, `filter={}`, `limit=2`.
- **Pass:** count returns an integer; query returns ≤2 documents.

### 2.4 Aggregate
- Call `mcp__mongodb__aggregate` with `database=<MONGO_DB>`, `collection=<MONGO_COLL>`, `pipeline=[{"$count": "n"}]`.
- **Pass:** returns one document `{"n": <int>}` matching the count from 2.3.

### 2.5 Read-only enforcement (NEGATIVE TESTS)
`MONGO_MCP_READ_ONLY=true` enforces read-only at two layers:
1. **Tool surface (preferred):** the `insert`, `update`, and `createIndex` tools are *not advertised* when the server runs with `--read-only`. Per `config/mcp/mongo-mcp-readonly.patch`, the upstream tools array is filtered before being returned from `tools/list`. Inspect the agent's MCP tool list — those three tool names should be absent.
2. **Runtime refusal (defense-in-depth):** if the tools were somehow callable, `checkReadOnlyMode()` rejects them with `ReadonlyError: Operation '<op>' is not allowed in read-only mode (started with --read-only or MCP_MONGODB_READONLY=true)`.

Test:
- Inspect the tool list. **Pass:** `insert`, `update`, `createIndex` are not present. If they are, the read-only patch isn't deployed (older build, or the env var is unset) — fall through to layer 2.
- *Layer 2 fallback (only if the write tools are still registered):* call `insert` / `update` / `createIndex` and assert each returns a `ReadonlyError` mentioning the env var. Verify nothing landed by re-running 2.3's count and by `listCollections` not showing `__mcp_probe__`.

---

## Phase 3 — StarRocks MCP

**What this catches:** broken `entrypoint-starrocks.sh` RO-user sync, `enable_auth_check` misconfig, `mcp-server-starrocks` regressions, BITMAP-index discovery rot.

### 3.1 Database summary
- Call `mcp__starrocks__db_summary`.
- *Note: this deployment doesn't set a default database, so `db_summary` with no args returns `Database name not provided and no default database is set.` — that's expected, not a failure. When that happens, fall back to `mcp__starrocks__read_query` with `query="SELECT schema_name FROM information_schema.schemata"`, pick a non-system schema (skip `information_schema`, `_statistics_`, `sys`), then re-run `db_summary` with `db=<that>`.*
- **Pass:** the fallback (or the no-arg call, if a default DB is configured) yields at least one user database. Pick one as `<SR_DB>` and use `db_summary db=<SR_DB>` to inspect it.

### 3.2 Table overview
- Call `mcp__starrocks__table_overview` with `database=<SR_DB>`.
- **Pass:** returns at least one table with column types. Pick one as `<SR_TABLE>`.

### 3.3 Read query
- Call `mcp__starrocks__read_query` with `query="SELECT count(*) AS n FROM <SR_DB>.<SR_TABLE>"`.
- **Pass:** returns one row with integer `n`.
- *Note: SR has no default database in this deployment — queries must always be fully qualified `<db>.<table>`. If you forget the database prefix the response will say "No database selected" — that's the expected error, not a bug.*

### 3.4 Plan inspection
- Call `mcp__starrocks__analyze_query` with the same query as 3.3.
- **Pass:** plan output includes scan/aggregate operators for `<SR_TABLE>`.

### 3.5 Read-only enforcement (NEGATIVE TEST)
StarRocks read-only is enforced at the **database level** (the RO user has SELECT-only grants), not by the MCP server. So `write_query` is exposed but should be rejected by the DB.
- Call `mcp__starrocks__write_query` with `query="CREATE TABLE <SR_DB>.__mcp_probe__ (id INT) PROPERTIES('replication_num'='1')"`.
- **Pass:** returns an error from StarRocks about denied privilege / access denied for the RO user. **No table is created** (verify with `table_overview`).

---

## Phase 4 — Scheduler MCP — read-only surface

**What this catches:** scheduler MCP discovery (tools should only appear for *configured* backends), cancel/status semantics, `list_mongo_databases` behaviour. No queries are submitted yet.

### 4.1 Tool discovery
- Inspect the scheduler MCP's tool list (whatever the agent's harness exposes — for Claude Code these arrive as `mcp__scheduler__*` deferred tools).
- **Pass:** the set matches the backends configured for the scheduler (the human running this suite knows which ones). For each backend with at least one configured target, the corresponding `submit_<backend>_query` tool must be present. If a backend has zero targets, its `submit_*` tool **must not** be present. `query_status`, `query_cancel`, and `list_targets` are always present.
- Call `mcp__scheduler__list_targets`. **Pass:** returns `{"targets": [{"name": ..., "backend": ...}, ...]}`. Record the entries as `<PG_TARGET>` / `<SR_TARGET>` / `<MONGO_TARGET>` for the rest of Phase 5. Cross-check that each name also appears in the `Configured targets:` line of the matching `submit_*` description (Phase 6.2 promotes this to a hard check).

### 4.2 List Mongo databases (only if Mongo target configured)
- Call `mcp__scheduler__list_mongo_databases` with `target=<MONGO_TARGET>`.
- **Pass:** returns a `databases` array; system DBs (`admin`, `config`, `local`) are filtered out.
- Negative: call with `target="not_a_target"`. **Pass:** returns `{"error": "..."}` mentioning the configured target list, no exception.

### 4.3 Status of a non-existent job
- Call `mcp__scheduler__query_status` with `job_id="does-not-exist"`.
- **Pass:** returns `{"job_id": "does-not-exist", "status": "unknown", "error": "job not found"}`.

### 4.4 Cancel a non-existent job
- Call `mcp__scheduler__query_cancel` with `job_id="does-not-exist"`.
- **Pass:** same shape — `status: unknown`, error explains the job wasn't found.

---

## Phase 5 — Scheduler MCP — full submit → approve → done

**This phase requires a human.** The agent submits, the human approves in the web UI, the agent verifies completion. Do not skip the wait — the whole point of the scheduler is human-in-the-loop approval.

For each configured backend, run one happy-path query and one negative case. Use a tiny `LIMIT 5` style query so approval is cheap and result files stay small.

### 5.1 PostgreSQL submit & approve
1. Call `mcp__scheduler__submit_postgres_query` with:
   - `target=<PG_TARGET>`
   - `sql="SELECT 1 AS n"` (or a `SELECT … LIMIT 5` against a known table — your call)
   - `output_filename="probe.parquet"`
   - `description="Agentic MCP suite probe — Phase 5.1"`
2. Record the returned `job_id`. Status should be `pending`.
3. **Tell the human:** "Job `<job_id>` is pending — approve it in the jobs UI." Then poll `mcp__scheduler__query_status` every ~10s. Stop polling after 5 min and mark the task **skipped — no human approval** if status is still `pending`.
4. **Pass:** status transitions `pending → approved → running → done`. Final response has a non-empty `result_path`, `rows ≥ 1`, `size_bytes > 0`, no `error`.

### 5.2 PostgreSQL — bad submission (NEGATIVE)
- `submit_postgres_query` with `target="not_a_target"` and the same other args. **Pass:** returns `{"error": "..."}` listing the real targets — no `job_id`.
- `submit_postgres_query` with `output_filename="probe.txt"` (wrong extension). **Pass:** the MCP rejects at submit time (returns `{"error": ...}` mentioning the unsupported extension, no `job_id`). Older deployments may instead accept and let the runner fail it after approval — if a `job_id` comes back, get it approved and verify `query_status` ends in `status="failed"` with a clear message. Either path is acceptable, but submit-time rejection is the modern behavior.
- `submit_postgres_query` with `output_filename="sub/probe.parquet"` (slash in filename). **Pass:** same as above — submit-time rejection (`output_filename must be a basename (no slashes): 'sub/probe.parquet'`) is preferred; runner-time rejection is the legacy fallback.

### 5.3 StarRocks submit & approve
1. Call `mcp__scheduler__submit_starrocks_query` with:
   - `target=<SR_TARGET>`
   - `sql="SELECT 1 AS n"` or `"SELECT count(*) AS n FROM <SR_DB>.<SR_TABLE>"`
   - `output_filename="probe.parquet"`
   - `description="Agentic MCP suite probe — Phase 5.3"`
2. Wait for human approval, poll status. **Pass:** ends in `done`. SR chunks output, so `result_path` will be a directory containing `probe_0.parquet`, `probe_1.parquet`, …; that's expected, not a bug.
3. **Negative:** submit a query that doesn't qualify the database (e.g. `SELECT count(*) FROM <SR_TABLE>` with no `<SR_DB>.` prefix). The `submit_*` call itself succeeds; **after approval** the job should fail with a "no database selected" error in `query_status`. Mark **pass** if the failure is reported cleanly with `status="failed"` and a non-empty `error`.

### 5.4 MongoDB submit & approve
1. Call `mcp__scheduler__submit_mongo_query` with:
   - `target=<MONGO_TARGET>`
   - `database=<MONGO_DB>`  *(use the value from Phase 2.1 — do not omit)*
   - `collection=<MONGO_COLL>`
   - `pipeline=[{"$limit": 5}]`
   - `output_filename="probe.ndjson"`
   - `description="Agentic MCP suite probe — Phase 5.4"`
2. Wait for human approval, poll status. **Pass:** ends in `done`, `result_path` points at a folder containing `probe.ndjson` with up to 5 lines.
3. **Negative — missing database:** submit with `database=""`. **Pass:** the MCP returns `{"error": "..."}` immediately, no `job_id`.
4. **Negative — CSV terminal-stage pre-check:** the MCP rejects, at submit time, any CSV pipeline whose terminal stage cannot produce a flat document. Submit with `output_filename="probe.csv"` and `pipeline=[{"$limit": 1}]` (terminal stage is `$limit`, which preserves shape). **Pass:** the call returns `{"error": "..."}` mentioning that the terminal stage must be one that flattens (e.g. `$project`); no `job_id`.
5. **Negative — runtime flat-scalars guard:** the runner is the source of truth: even when the terminal stage *could* produce flat scalars, it might not in practice. Force a nested value into the projection to exercise the runtime guard:
   - `output_filename="probe.csv"`
   - `pipeline=[{"$addFields": {"nested_probe": {"a": 1, "b": [2, 3]}}}, {"$limit": 1}]`
   The submit-time pre-check accepts this (`$addFields` is permissive). After human approval, the runner should fail the job with `CSV output requires flat scalars; field 'nested_probe' contains a dict. Use .ndjson for nested output, or add a $project stage that flattens the document.` **Pass** if `status="failed"` and the error names the offending field. *Note: older deployments did not ship the submit-time pre-check; on those, step 4 will return a `job_id` and only fail post-approval — record it but treat as PASS if the runtime error matches step 5's expected message.*

### 5.5 Cancel a pending job
1. Submit any valid scheduler query. Record `job_id`. **Do not ask the human to approve it.**
2. Immediately call `mcp__scheduler__query_cancel` with that `job_id`.
3. **Pass:** the response shows `status="cancelled"` (or equivalent terminal state), and a follow-up `query_status` agrees.
4. Negative: prove the MCP refuses cancellation for any job that's past the `pending` phase. The doc-spec case is "cancel while running" — but probe queries (`SELECT 1`, `SELECT count(*)`) finish in tens of milliseconds, so a human cannot approve fast enough for the agent to catch the running window. Two acceptable forms:
   - **Slow-query form (preferred):** submit a deliberately slow query — `SELECT pg_sleep(10), 1 AS n` for PG, `SELECT sleep(10), 1 AS n` for SR — ask the human to approve, then call `query_cancel` once status is `running`. **Pass:** error mentions you can't cancel via MCP once running.
   - **History-phase proxy:** call `query_cancel` on a job you already let finish in 5.1/5.3/5.4 (status `done` or `failed`). **Pass:** error reads `cannot cancel via MCP while in phase 'history'` — same underlying invariant, just demonstrated post-mortem instead of mid-run.

---

## Phase 6 — Cross-cutting checks

### 6.1 RO credential rotation isn't required
You should not need to authenticate at any point in this suite. If any MCP returns a 401/403/connection-refused, the entrypoint scripts (`config/mcp/entrypoint-*.sh`) couldn't read `.ro_credentials` from the data volume. Report which MCP and the exact error — don't try to fix it.

### 6.2 Tool descriptions match reality
Compare `mcp__scheduler__list_targets` (machine-readable) against the `Configured targets:` line in each `submit_*` tool's description (free-text). They should agree exactly per backend. A mismatch suggests stale tool metadata (the scheduler MCP was started with an old config). Worth flagging but not failing the suite.

### 6.3 Schema parity (optional, only if all three DBs are populated)
If the same source has been ingested into PG, Mongo, and SR, run a `count(*)` on each via the appropriate read tool and report the three numbers. They don't have to match exactly (different ingestion timestamps), but order-of-magnitude differences are a signal worth surfacing.

### 6.4 PG↔SR aggregate parity (optional, only if the same source is in both)
A stronger version of 6.3 for the two backends that should agree to the cell. Skip if `<PG_TABLE>` and `<SR_TABLE>` aren't the same logical dataset.

Pick low-cardinality grouping columns and integer aggregates only. **Avoid text payloads** (`body`, `selftext`, `title`, …) — those can legitimately differ between backends (CSV-escape edge cases, null-byte stripping, length truncation are out of scope here). **Avoid floating-point aggregates** (`avg`, `stddev`, …) — PG and SR diverge in last-digit precision and you'll chase a non-bug.

Run the same query against both via `mcp__postgres__execute_sql` and `mcp__starrocks__read_query`. Reddit example (substitute equivalent columns for other sources):

```sql
SELECT
  dataset, lang,
  count(*) AS n,
  count(DISTINCT author) AS n_authors,
  count(DISTINCT subreddit) AS n_subreddits,
  sum(score) AS total_score,
  min(score) AS min_score,
  max(score) AS max_score,
  sum(CASE WHEN is_deleted THEN 1 ELSE 0 END) AS n_deleted,
  sum(CASE WHEN stickied THEN 1 ELSE 0 END) AS n_stickied,
  sum(lang_chars) AS sum_lang_chars
FROM <schema_or_db>.comments
WHERE lang IS NOT NULL
GROUP BY dataset, lang
ORDER BY dataset, lang
LIMIT 20
```

This exercises: cardinality, distinct counts, signed integer sums, min/max, boolean→int coercion via `CASE WHEN bool`, grouping, deterministic ordering.

**Pass:** every cell in every row matches byte-for-byte between PG and SR. **Fail:** on a mismatch, name the exact `(group_key…, column)` that diverged with both values — a single integer-aggregate divergence is an ingestion-parity bug worth chasing, not noise.

---

## Output format

End your run with a single fenced block like this. Keep evidence to one line per task.

```
=== AGENTIC MCP SUITE RESULT ===

Phase 1 (postgres):
  1.1 connectivity      PASS  — list_schemas returned 4 schemas
  1.2 object detail     PASS  — get_object_details returned 12 columns
  1.3 read query        PASS  — count = 1,234,567
  1.4 explain           PASS  — Seq Scan on <PG_TABLE>
  1.5 read-only deny    PASS  — execute_sql refused CREATE TABLE
  1.6 health            PASS  — keys: connections, cache, replication, …

Phase 2 (mongodb):
  …

Phase 3 (starrocks):
  …

Phase 4 (scheduler r/o):
  …

Phase 5 (scheduler full):
  5.1 pg submit         PASS  — job pg_… reached done, rows=1, 4.2KB
  5.2 pg negatives      PASS  — bad target / bad ext / slash all rejected
  5.3 sr submit         PASS  — job sr_… done, result has probe_0.parquet
  5.4 mongo submit      PASS  — job mongo_… done, 5 lines in probe.ndjson
  5.5 cancel            PASS  — pending job cancelled, running job refused

Phase 6 (cross-cutting):
  …

Overall: <N> pass, <M> fail, <K> skipped.
```

Failures: cite the tool name, the args (redact any large payload), the response. Skipped: cite *why* (no fixtures, human didn't approve in time, etc.).
