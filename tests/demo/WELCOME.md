<div align="center">

# Social Data Pipeline

[![Docker](https://img.shields.io/badge/Docker-Compose_v2-2496ED.svg?logo=docker&logoColor=white)](https://www.docker.com/)
[![Python 3.11+](https://img.shields.io/badge/Python-3.11+-3776AB.svg?logo=python&logoColor=white)](https://www.python.org/)
[![PostgreSQL 18](https://img.shields.io/badge/PostgreSQL-18-4169E1.svg?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![MongoDB 8](https://img.shields.io/badge/MongoDB-8-47A248.svg?logo=mongodb&logoColor=white)](https://www.mongodb.com/)
[![StarRocks](https://img.shields.io/badge/StarRocks-OLAP-FF6D00.svg?logo=starrocks&logoColor=white)](https://www.starrocks.io/)

### Codespace demo

End-to-end ingestion, classification, and agentic querying of large-scale social-media dumps — running entirely in your browser.

</div>

---

You're operating a working SDP install. PostgreSQL, MongoDB, and StarRocks are all pre-configured (along with their read-only MCP servers and the jobs scheduler). Nothing is running yet — pick a backend and start it.

## 1. Pick a database and start it

PostgreSQL is the recommended default for the free-tier Codespace (2 cores / 8 GB).

```bash
sdp db start postgres
```

This brings up PostgreSQL **plus** the PostgreSQL read-only MCP (port 8000) **plus** the jobs scheduler (port 8050) — they're auto-bundled with the database. Takes ~30 seconds.

### Alternatives

```bash
sdp db start mongo        # Mongo + mongo-mcp (port 3000) + jobs
sdp db start starrocks    # StarRocks + sr-mcp (port 9000) + jobs (heavier; see Limits)
```

> **Don't run bare `sdp db start`** on the free tier — that launches all three backends at once and will OOM the 8 GB box. Pick one.

## 2. Bring data — pick one path

### Path A · Reddit dump

Drop a `.zst` from [Arctic Shift](https://github.com/ArthurHeitmann/arctic_shift) into `data/dumps/reddit/`. A small monthly file (10–100 MB) gives the best demo cadence.

```bash
sdp run parse              # decompress + parse to Parquet
sdp run lingua             # add per-row language detection (configured for Reddit only)
sdp run postgres_ingest    # or: mongo_ingest, sr_ingest — pick what you started
```

### Path B · HuggingFace dataset (auto-configured)

`sdp source add --hf` queries the HF dataset API and pre-fills the platform config. Two datasets to try (multilingual, permissive licenses):

- `cardiffnlp/tweet_sentiment_multilingual` — 24k tweets, 8 languages, 4 MB, CC-BY 3.0. Fast.
- `wikimedia/wikipedia` configs `20231101.eu` + `20231101.simple` — 640k rows, 2 languages, ~400 MB, CC-BY-SA 3.0 + GFDL. Heavier.

```bash
sdp source add tweets --hf cardiffnlp/tweet_sentiment_multilingual
sdp source download tweets
sdp run parse --source tweets
sdp run postgres_ingest --source tweets     # or mongo_ingest / sr_ingest
```

## 3. Query via Copilot Chat (the agentic path)

Open the **Copilot Chat** sidebar (left edge of VS Code; View → Copilot Chat if hidden).

**Two first-time consent clicks:**

1. *"Trust this MCP server?"* for `sdp-jobs` → click **Trust**.
2. *"Allow tool?"* on the first query submission → click **Allow for workspace**.

Then ask Copilot anything that fits the data:

> How many rows are in the table?
>
> What columns does the table have and what types?
>
> Show me five random rows.

Copilot picks the right `submit_*_query` tool, fills in SQL, submits via the jobs scheduler.

## 4. Approve the query in the WebUI

Open the **Ports** panel (bottom of VS Code) → click the globe icon on port 8050. The jobs scheduler UI opens in a new tab. Submitted queries appear as `pending`; click **Approve**. Results land in `data/jobs-results/` and you can download from the UI.

## Limits and notes

- **Free Codespace tier:** 2 cores / 8 GB RAM / 32 GB disk / 60 core-hours per month — roughly 30 hours of wall time for this demo.
- **StarRocks is tuned aggressively low** (1 GB FE heap, 2 GB BE limit) to fit the free tier. Heavy queries may push it over budget; if SR crashes, restart with `sdp db stop starrocks && sdp db start starrocks` or use Postgres/Mongo instead.
- **Auth is off** — single-user demo, no passwords. Full setup with auth + read-only credentials is documented in the [main README](../../README.md).
