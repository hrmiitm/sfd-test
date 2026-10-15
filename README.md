# SFD preprocessing and database ingestion

One common pipeline streams bounded Arrow batches from the six compressed Parquet files into DuckDB,
extracts filing metadata and sections, creates token-bounded Markdown chunks,
embeds them through LLM Foundry's OpenRouter proxy, and ingests identical records
into LanceDB and Qdrant. The benchmark runner below adds retrieval, answering, and judging over the existing stores.
Pathway and question generation are not included.

## Run

```bash
uv sync --locked
cp .env.example .env
# Edit .env: set LLMFOUNDRY_API_KEY and check the embedding model/endpoint.

# Inspect a small sample first; this stage makes no network requests.
uv run sfd-pipeline prepare --limit 10 --work data/pipeline/sample-10

# Complete all stages for that sample (embedding API calls are billable).
./scripts/ingest.sh --limit 10 --work data/pipeline/sample-10

# Default development run: 1,000 filings, both databases.
./scripts/ingest.sh
```

The default model is `openai/text-embedding-3-small`, with 1,536 dimensions.
Set `EMBEDDING_MODEL` and `EMBEDDING_DIMENSIONS` together if changing it. The
client sends `POST /openrouter/v1/embeddings` with bearer authentication, using
`LLMFOUNDRY_BASE_URL`. `LLMFOUNDRY_EMBEDDINGS_URL` overrides the entire URL.
The example chat route establishes the proxy convention; verify that embeddings
are enabled for your account. There is no fallback to another embedding provider.
HTTP 429, server errors, and transport errors are retried with bounded backoff.
Authentication or invalid model errors stop the run. Missing credentials do not
prevent preprocessing.

## Full corpus and Qdrant server

Embedded Qdrant is the default for development, with files in the work directory.
Use a Qdrant server for the full corpus; embedded mode is not a production-scale
substitute. For example, with Docker installed:

```bash
# Runs a local server with persistent storage, accessible only on localhost.
docker run -d --name sfd-qdrant \
  -p 127.0.0.1:6333:6333 \
  -v "$PWD/data/qdrant-server:/qdrant/storage" \
  qdrant/qdrant

# Use a separate corpus workspace for each sample size/configuration.
./scripts/ingest.sh --limit 1000 --work data/pipeline/sample-1000 --qdrant-url http://localhost:6333
./scripts/ingest.sh --limit 10000 --work data/pipeline/sample-10000 --qdrant-url http://localhost:6333
./scripts/ingest.sh --limit 0 --work data/pipeline/full --qdrant-url http://localhost:6333
```

Alternatively set `QDRANT_URL` and optional `QDRANT_API_KEY` in `.env`.
Do not run these three example sizes together unless you want three independent
corpora and embedding bills. Start small, inspect chunk/token counts, and budget
for embeddings and disk space: the original compressed data is about 6.9 GB
(decimal), while chunks, cached vectors, and two databases can be much larger.
The pipeline intentionally does not automatically launch Docker or ingest the
full corpus on installation.

## Independent stages and recovery

```bash
uv run sfd-pipeline prepare --limit 1000 --work data/pipeline/sample-1000
uv run sfd-pipeline embed --work data/pipeline/sample-1000
uv run sfd-pipeline ingest --work data/pipeline/sample-1000 --backend lancedb
uv run sfd-pipeline ingest --work data/pipeline/sample-1000 --backend qdrant
uv run sfd-pipeline status --work data/pipeline/sample-1000
```

Repeat the same command to resume. Completed chunks and embeddings are immutable
Parquet shards committed by atomic rename. Database progress uses SQLite;
LanceDB merges on chunk ID and Qdrant upserts UUIDs, so replay after a crash does
not duplicate rows. A workspace lock prevents two writers. Source paths, sizes,
modification times, chunk configuration, and embedding settings are recorded;
changes require a fresh workspace. Do not modify/delete individual artifacts or
edit manifests. A changed input file is not an incremental update: create a new
workspace. Different workspaces do not share an embedding cache.

`--read-batch` (default 8 filings) bounds source batches. `--embed-batch`
(default 128 chunks) controls request size and embedding checkpoints. `--embed-workers` defaults to 4
concurrent requests. Batches split automatically below the proxy request-size
limit; responses use base64 float32 vectors to reduce transfer overhead. One very
large filing still has to fit into memory; DuckDB is configured with a 1 GB memory
limit, which does not cap Python, Arrow, or database memory. Chunk size defaults
to 700 `cl100k_base` tokens; this tokenizer matches the default embedding model.
Choose a conservative size for other models. Interrupted preprocessing rescans
source batches but skips completed chunk generation. DuckDB receives bounded
Arrow batches to avoid buffering large ordered Parquet query results.

## Artifacts and metadata

Inside each work directory:

- `chunks/`: shared text/metadata Parquet shards plus filing/empty-row counts.
- `embeddings/`: the same records with fixed-size float32 vectors and model name.
- `prepare.json`, `prepared.json`, `embedding.json`, `embedded.json`: configuration
  and stage completion markers.
- `lancedb/`, `qdrant/`: local databases (Qdrant directory only in local mode).
- `ingestion.sqlite`: per-destination shard checkpoints.
- `ingested-*.json`: collection/table name and verified exact chunk counts.

The raw dataset's `accession` field is a company identifier. Filing accession
comes from the Markdown header, falling back to `file_stem`; CIK comes from its
header, falling back to the raw identifier. Company name, form, and filed date
are parsed conservatively from headers; missing values stay empty. Multi-filer
headers currently use the first matching company. Original file/row, MD5,
SHA-256 of Markdown, OCR flag, section, and exact character offsets support
citations without inventing page numbers. IDs include source provenance and
content, so duplicate source rows remain independently traceable.

Markdown headings and ITEM headings define sections. Paragraphs/tables that fit
remain intact. Oversized tables split along rows; a single oversized row or line
splits on Unicode boundaries to respect the token cap. Source text is retained
exactly, with no numeric normalization or deduplication; no overlap is added.
Split tables retain section and source offsets but do not repeat table headers.
Whitespace-only filings are counted as empty and produce no vectors.

LanceDB has a native BM25 full-text index and cosine IVF_FLAT index once there
are at least 256 chunks; smaller samples use exact vector search. Qdrant stores
named `dense` and `bm25` vectors with IDF enabled and server-side metadata
indexes. BM25 sparse vectors use FastEmbed `Qdrant/bm25` (first use may download
its tokenizer resources); this is lexical preprocessing, not a separate semantic
embedding provider. Fixed defaults are recorded in the ingestion report. Future
Qdrant queries must use the same sparse model's **query** embedding method and
same parameters. LanceDB and Qdrant have different BM25 implementations, so
ranking parity is not implied. The benchmark runner uses these same dense and sparse settings.

## Validation

```bash
uv run pytest -q
```

Tests cover exact citation offsets, Unicode chunk limits, metadata extraction,
embedding response ordering/validation, resume without repeated embeddings,
both real database writes, replay after checkpoint loss, and configuration drift.
Tests use synthetic dense and sparse vectors and never contact the paid proxy.

## Local installation and measurement

For one command that starts local Qdrant and runs all stages:

```bash
./scripts/run-local-pipeline.sh --limit 1000 --work data/pipeline/sample-1000
```

Or run startup, ingestion, and verification separately:

```bash
./scripts/start-local-databases.sh
./scripts/ingest.sh --limit 1000 --work data/pipeline/sample-1000 --qdrant-url http://localhost:6333
uv run python scripts/verify-local-databases.py
uv run python scripts/corpus-stats.py
uv run python scripts/pipeline-report.py
```

The Qdrant container is configured to restart automatically and stores its data
under `data/qdrant-server`. LanceDB is an embedded database, so it requires no
server process. The work directory's `ingested-both.json` identifies both the
LanceDB table and Qdrant collection. `verified.json` records counts and index
smoke checks; these are storage readiness checks, not a retrieval benchmark.

`embedding-requests.jsonl` records successful API requests, input token usage,
reported costs, retries, and latency without storing credentials. `stage-timings.jsonl`
records completed stage timings. The measured report is in
`reports/ingestion-benchmark.json`; `reports/ingestion-benchmark.md` explains
its projections and limitations. Larger cost figures are for a fresh run;
embeddings are shared between the two databases, not paid for twice. A separate
workspace currently re-embeds its own corpus rather than sharing a cross-workspace
cache. No separate Foundry billing surcharge can be verified from API usage alone.

API references: [OpenRouter embeddings](https://openrouter.ai/docs/api/api-reference/embeddings/create-embeddings),
[LanceDB Python](https://lancedb.github.io/lancedb/python/python/),
[Qdrant BM25](https://qdrant.tech/documentation/inference/inference-bm25/).

## Annual and quarterly reports only

The pipeline supports exact submission-type filtering before chunking or embedding:

```bash
# Free preprocessing only, across all six source files.
uv run sfd-pipeline prepare --forms 10-K 10-Q --limit 0 \
  --work data/pipeline/financial-10k-10q

# Starts local Qdrant and runs/resumes the complete filtered pipeline.
# This command makes billable embedding requests for uncached chunks.
./scripts/run-financial-subset.sh
```

`--limit` counts matching filings, not rows scanned. `--limit 0` selects every
matching report. `10-K/A` and `10-Q/A` amendments are excluded; include those exact
labels in `--forms` if needed. Selection uses the reconstructed Markdown's
`CONFORMED SUBMISSION TYPE` header, not independently validated SEC metadata.
Original file/row references and chunk IDs remain tied to the unfiltered source.
The dedicated workspace creates its own LanceDB table and Qdrant collection and
does not replace the existing 1,000-filing databases.

The source inventory and sampled estimate can be regenerated without embedding calls:

```bash
uv run python scripts/profile-financial-filings.py
uv run python scripts/estimate-financial-subset.py
```

See `reports/financial-subset-estimate.json` for measured counts and estimated
resource requirements. Estimates use reports sampled separately for each form
and month, rather than assuming annual/quarterly reports have the same size as
the mixed-filings benchmark. The dedicated subset workspace does not automatically
reuse embeddings from other workspaces.

The existing benchmark cites five additional 8-K/Form 4 sources for six questions.
To retain just those exceptions alongside 10-K/10-Q, use
`./scripts/run-financial-benchmark-subset.sh`. The generic
`--include-accessions ID ...` option adds explicit source IDs to a form filter.
See [the subset assessment](reports/financial-subset-estimate.md) before starting
its paid embedding run.

## LLM retrieval benchmark

```bash
# Live smoke: 2 answer models × 2 databases × 3 methods, one question, Haiku judge.
./scripts/benchmark.sh --limit 1 --top-k 4 --methods bm25 vector hybrid --output reports/my-smoke

# All 100 questions; billable. Two concurrent chat requests by default.
./scripts/benchmark.sh --output reports/rag-full --concurrency 2

# Select another populated corpus (use its actual collection name if still ingesting).
./scripts/benchmark.sh --work data/pipeline/financial-10k-10q \
  --collection YOUR_COLLECTION --output reports/financial-rag

# Restrict a run, or use exact alternative gateway model IDs.
./scripts/benchmark.sh --ids Q001 Q050 Q096 --backends qdrant \
  --methods bm25 hybrid --output reports/selected-rag
```

Defaults are `google/gemini-3.8-flash` (temperature 0, low reasoning),
`openai/gpt-6-luna` (low reasoning), and judge `anthropic/claude-haiku-5.5`.
Models are validated against the authenticated gateway catalog before inference;
no models are silently substituted. Temperature is sent only when advertised by
the model catalog. `--models` and `--judge` override IDs. The chat route defaults
to `${LLMFOUNDRY_BASE_URL}/openrouter/v1/chat/completions`; optionally set
`LLMFOUNDRY_CHAT_URL`. Credentials are read from `.env`, never copied to reports.

The eight table columns are LanceDB and Qdrant, each with BM25, cosine vector,
hybrid reciprocal-rank fusion (RRF, constant 60, 3×top-k candidates per arm),
and agentic search. Agentic search also has access to the original source Parquet
files, including filings that have not yet been indexed. It uses bounded concurrent chat
requests and batches query embeddings (`--embed-batch 64`); it does **not** submit
provider Batch API jobs or assume batch discounts. HTTP rate-limit/server errors
receive bounded retries. Change `--concurrency` to tune throughput.

For BM25/vector/hybrid, each question's context is retrieved once and reused across answer models.
Agentic contexts depend on each model's own tool choices and are saved separately. Gold
answers, accessions and quotes are hidden from answering/retrieval inputs and
provided only to evaluation. Haiku produces separate correctness and citation
support scores from 0–100 plus a rationale. Scores are model judgments, not proof
of correctness; manually audit a sample, especially numerical partial credit.
Accession recall and literal whole-quote recall are also recorded independently.
The latter undercounts evidence split across chunk boundaries.

Outputs in `--output`:

- `summary.md`: requested model-row × retrieval-column table, correctness, cost, completion counts.
- `summary.csv` / `summary.json`: scores, citation support, accession recall, latencies, token usage, reported chat costs and error counts.
- `results/*.json`: answers, judge rationales, raw usage, per-question retrieval metrics, failures.
- `retrieval.json`: cached excerpts, LanceDB version, before/after counts, and evaluation-only per-accession corpus coverage.
- `manifest.json` / `models.json`: run configuration, question snapshot, gateway model metadata.
- `requests.jsonl` / `embedding-requests.jsonl`: billing audit and shared query embedding usage.

Unknown costs stay unknown. Cell costs include answering and judging, but exclude
shared query embeddings and earlier ingestion/indexing expenses. Agentic cell costs include all model planning/tool-call turns and the final answer. The request
audit also includes charged responses that later fail validation; consult it for
total spend after retries or failures. Latencies exclude semaphore queue time;
retrieval timing excludes shared embedding time, which has its own audit.

Rerun the same command/output to resume without repeating completed answers or
retrieval. Agentic model turns and completed tool results are checkpointed too. A changed configuration requires a new output directory. A process
lock prevents two runs from writing the same output. Keep each output directory
as an experiment snapshot; use a new directory to evaluate newly ingested data.
LanceDB reads a pinned version. Qdrant is live: count changes are flagged, but
same-count updates cannot be detected. For a strict cross-store comparison,
pause ingestion or use database snapshots. Cached retrieval makes subsequent
model comparisons/resumes stable even while ingestion continues.

The default corpus is the existing `sample-1000` workspace, not the entire source
corpus. Inspect coverage before interpreting scores. A financial 10-K/10-Q-only
corpus cannot cover the benchmark's 8-K/Form 4 questions. Missing source filings
remain in the denominator; scores are not silently inflated by skipping them.

### Resource limits

Use `./scripts/benchmark.sh` for live runs. On this Linux machine it runs the
**entire process tree** in a systemd user scope capped at **2 GiB RAM**, **zero
swap**, and **100% CPU quota (one core's worth)**. It fails if the user systemd
manager is unavailable instead of silently running without these limits. Direct
`python -m` / `sfd-benchmark` invocations retain individual source-worker limits,
but do not apply the whole-process-tree cap. Default chat concurrency is now two.
Concurrent benchmark invocations each get their own scope; run one benchmark at
a time on this machine. If the total cap is reached, the scope is stopped and the
run can be resumed from checkpoints after reducing the workload.

### Agentic DuckDB / Parquet search

```bash
# Only the new agentic method, on both database backends.
./scripts/benchmark.sh --methods agentic --ids Q001 --agent-max-tools 6 \
  --output reports/agentic-example

# Compare every method on all questions (agentic is included by default).
./scripts/benchmark.sh --output reports/all-methods

# Run just the original three methods.
./scripts/benchmark.sh --methods bm25 vector hybrid --output reports/indexed-only
```

The answering model decides which tools to call and uses returned evidence to
refine its next search. Its tools are:

- `search_index(query)`: BM25 search in the column's LanceDB or Qdrant collection.
- `duckdb_query(sql)`: one read-only SELECT over a `filings` view of the original
  source Parquet files listed in the workspace's `prepare.json`. Available columns
  include `source_path`, `file_stem`, `accession`, `year`, `month`, and `char_count`.
  `file_stem` is the filing accession; raw `accession` is a company identifier.
  The SQL view deliberately excludes full Markdown to avoid large vectorized
  text scans. SQL can filter and aggregate metadata. Optional `source_path`
  restricts the query to one input file.
- `search_parquet_company(source_path, company, form_type='', start_row=0)`:
  streams issuer headers from one original Parquet file, one filing at a time.
  Returns the first matching issuer/form and a pagination cursor, scanning at
  most 50,000 rows per call. A partial or failed search is not proof of absence.
- `read_parquet_filing(source_path, file_stem, start=0, needle='')`: PyArrow parses
  the original Parquet and returns a bounded Markdown passage with exact character
  offsets, preserving the original table text. `needle` locates a passage;
  `next_start` supports paging. Only approved source files can be read.

No gold answers or gold source IDs are sent to the agent. The judge receives both
parsed excerpts and the tool trace, including SQL evidence. Retrieval recall
counts parsed passages and index chunks; arbitrary SQL outputs are not counted as
whole-quote retrieval. Per-cell `*.agent.json` files checkpoint the full conversation,
planning responses, tool arguments, results, and timings. Completed result JSON
also retains the trace. A crash during an in-flight request may still require that
request to be repeated; the audit preserves every received response's usage.

There are at most six tool executions by default (`--agent-max-tools`), followed
by a final answer request. SQL results are limited to 20 rows / 12,000 characters;
filing passages are limited to 12,000 characters. Each SQL/Parquet operation runs
in a fresh worker with an OS address-space limit of 1.5 GiB, a 20-second CPU
budget, one-core affinity, lower scheduling priority, and a 30-second wall timeout.
DuckDB itself has a 128 MB memory limit and no disk spilling. Source reads use
non-parallel, one-filing batches; identifiers and Markdown are decoded separately
to avoid buffering entire multi-column row groups. Tool executions are serialized
across cells while model requests remain concurrent. Worker peak RSS and CPU/wall
time are saved in every source-tool result; summary JSON/CSV includes maximum
observed worker RSS. A resource failure becomes a tool error, allowing the agent
to refine its request. DuckDB
configuration is locked, extension auto-loading is disabled, and external access
is restricted to the approved source files. Only SELECT statements are accepted.
Source paths, sizes, and modification times are recorded in the run manifest and
validated on resume. These controls follow the [DuckDB security configuration](https://duckdb.org/docs/current/operations_manual/securing_duckdb/overview).

**Corpus scope differs:** agentic search can inspect all original Parquet inputs
listed by the workspace, even when that workspace indexed only a sample. Its
scores therefore measure expanded-source agent performance, not only retrieval
algorithm quality. Original corpus coverage is not represented by the indexed
coverage counts in `retrieval.json`. Tool transcripts show which sources were
actually used. Agentic live database searches are also model-specific and may see
newly ingested data; pause ingestion for strict reproducibility.

### Add the benchmark source filings to the existing databases

Run `./scripts/run-benchmark-additions.sh` to resume preprocessing, embedding,
and appending the cached 27 cited source filings plus 25 randomly sampled
additional filings into the existing `sample-1000` LanceDB table and Qdrant
collection. Only source filing text and metadata are ingested; question and
answer content is not copied. The additions use 2,000-token chunks capped at
24,000 characters, alongside the existing 700-token chunks, with the same
1,536-dimensional embedding model. Embeddings are shared by both databases.

The append command checks embedding compatibility, locks both workspaces,
saves the original chunk IDs before writing, and checkpoints uploads. Repeating
the command resumes without duplicating records. Source files and embedding
checkpoints are under `data/pipeline/benchmark-additions`; the existing database
locations remain unchanged. `extensions.json` tracks added counts separately
from the original corpus. Embedding API requests are billable.

### Recorded random-20 comparison (seed 20261015)

The completed experiment is in `reports/random20-seed20261015/report.html`.
It contains a model-by-retrieval matrix, detailed metrics, a log-cost/correctness
scatter plot with all 16 model/database/method combinations, and an answer/evidence
inspector. The single HTML file works offline; keep the run directory beside it
for links to the full raw request, result, and resource audits.

The 20 questions were sampled without replacement using seed `20261015`, before
looking at results. `selection.json` records IDs, difficulty distribution and the
question-source hash. This draw contains no extreme or unanswerable questions.
Preflight checks confirmed model/judge liveness, embedding and retrieval health,
and source coverage for all selected questions in both databases.

To reproduce the selection and launch another run, use a new output directory:

```bash
systemd-run --user --scope --quiet -p MemoryMax=2G -p MemorySwapMax=0 \
  -p CPUQuota=100% -p TasksMax=128 -p OOMPolicy=kill \
  uv run python scripts/benchmark-preflight.py \
  --output reports/random20-repeat --seed 20261015

# Starts only if preflight passed; preserves all completed checkpoints on resume.
systemd-run --user --scope --quiet -p MemoryMax=2G -p MemorySwapMax=0 \
  -p CPUQuota=100% -p TasksMax=128 -p OOMPolicy=kill \
  uv run python scripts/run-recorded-benchmark.py \
  --output reports/random20-repeat --concurrency 8

uv run python scripts/render-benchmark-report.py reports/random20-repeat
```

The report uses the complete request audit for cost, including charged responses
from failed finalization attempts. Shared query embeddings and preflight costs
are listed separately. `recovery.json` records the seven Gemini agentic cases that
needed an answer-only finalization after exhausting their tool budget. No extra
retrieval calls were allowed for those recoveries. `execution-history.jsonl`
preserves the first pass, and `resources.jsonl` includes both passes. All 320 final
cases completed, and the postflight database counts/version matched preflight.

## Publish the results on GitHub Pages

`.github/workflows/pages.yml` builds the saved random-20 report and deploys it on
pushes to `main` (or manually with **Run workflow**). In the GitHub repository,
select **Settings → Pages → Build and deployment → Source → GitHub Actions**.
The workflow uses GitHub's standard Pages artifact/deployment actions; no API
keys, databases, corpus downloads, or paid benchmark runs are needed.

`python3 scripts/build-pages.py --output _site` builds the same site locally using
only the Python standard library. Open `_site/index.html` or serve `_site` with
`python3 -m http.server --directory _site`. The build validates all static links
and includes the 320 final result files used by the report inspector.

Git includes code, benchmark questions, documentation, and recorded experiment
results. `/data/` (including source Parquet, embeddings, LanceDB and Qdrant), `.env`,
local caches, archives, agent resume checkpoints, and generated HTML/JSON/screenshots
are excluded. The HTML is rebuilt from committed results during deployment.
Source excerpts supporting benchmark answers remain part of the recorded results;
the full corpus is never copied into the Pages artifact.

See [GitHub's custom Pages workflow guide](https://docs.github.com/en/pages/getting-started-with-github-pages/using-custom-workflows-with-github-pages)
for repository setup. A remote repository must be configured before pushing.

## Standalone Parquet-agent benchmark

The independent benchmark, code, scripts, models configuration, questions, tests
and raw reports are in [`agentic-search-only/`](agentic-search-only/README.md).
The shared corpus remains in `data/parsed/`.

Run all 100 questions from this directory:

```bash
agentic-search-only/scripts/parquet-benchmark.sh --count 100 --output reports-whole-data/all100
```

Results are stored in `agentic-search-only/reports-whole-data/all100/`.

The deployed Pages site includes this benchmark as its second tab. The Pages
build expands its compressed, audited report exports; the corpus, original raw
runs and smoke outputs are excluded. Build both tabs with
`python3 scripts/build-pages.py --output _site`.
