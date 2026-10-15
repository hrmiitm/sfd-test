# Agentic search only

Independent Parquet-agent benchmark. Code, configuration, scripts, question inputs,
tests and reports live in this folder. The existing source corpus stays outside
at `../data/parsed/`; no data files are moved or duplicated.

Run all 100 questions from the repository root:

```bash
agentic-search-only/scripts/parquet-benchmark.sh --count 100 --output reports-whole-data/all100
```

The launcher changes into this project, so the relative output above is
`agentic-search-only/reports-whole-data/all100/`. It works from any starting
directory. From inside this folder, use `scripts/parquet-benchmark.sh`.

Dependencies are isolated and minimal (DuckDB, PyArrow, httpx, python-dotenv).
`uv run --locked` creates/uses this project's `.venv`; the existing root `.venv` is
not needed. For local setup or tests: `uv sync --project agentic-search-only`, then
`uv run --project agentic-search-only pytest agentic-search-only/tests`.

Use this folder's `.env` for independent credentials, or reuse the existing
repository-root `.env` automatically. Environment variables take precedence, then
the local `.env`, then the root fallback. No credentials are copied or committed.
See `.env.example` and `config/parquet-models.json`.

## Whole-corpus Parquet agent benchmark

`src/agentic_search_only/parquet_agent.py` runs a separate experiment directly over **all
Parquet files discovered recursively in `../data/parsed`**. It does not open LanceDB,
Qdrant, embedding shards, or the old prepared/indexed corpus. No chunking or vector
index is built. The current snapshot is six files and 638,067 filing rows. This
avoids additional indexing; the existing parsed corpus is still required.

The agent gets the question and source filenames, discovers schemas, uses DuckDB
SELECT queries to find filings, and reads evidence before answering. CIK filters normalize
zero padding with `ltrim(accession, '0')`. The gold
answer, expected accessions, and gold quotes are sent only to the Haiku judge.
The two comparison methods are:

- `sql`: schema discovery plus read-only DuckDB SELECT over the complete Parquet
  schema, including `parsed_md`. Text selection should be narrowed by file and CIK.
  For text SQL, also pass `file_stem` with exactly one `file_id`; the worker streams
  that source row into DuckDB before running SQL, avoiding large text scan batches.
- `sql_streaming`: the same SQL tools plus a paginated company-header search and
  exact filing reads with literal-needle search and character-offset pagination.

`accession` in these files is a company CIK; `file_stem` is the filing accession.
Company names and financial tables are inside large Markdown cells. Full-text SQL
can exceed the worker budget even with LIMIT, since decoding can precede limiting.
The streaming method reads one filing at a time and returns excerpts; it may still
need to decompress preceding rows within a row group. Neither approach guarantees
that an eight-tool budget will find an answer across the entire corpus. A timeout
or partial search is explicitly not evidence of absence.

### Run and resume

Install dependencies with `uv sync` from this folder, configure `.env`, and use the bounded launcher:

```bash
# Live model/tool/judge checks and Parquet metadata checks, without benchmark cases.
scripts/parquet-benchmark.sh --output reports-whole-data/my-run --preflight-only

# Same output and inputs: proceeds after preflight; resumes completed cases/turns.
scripts/parquet-benchmark.sh --output reports-whole-data/my-run

# Small live test (2 questions × 2 models × 2 methods = 8 judged cases).
scripts/parquet-benchmark.sh --output reports-whole-data/my-smoke --ids Q001 Q002

# All 100 questions. For a comparison without shared tool-cache effects:
scripts/parquet-benchmark.sh --output reports-whole-data/all100 \
  --count 100 --cache-policy off
```

Default selection: all 100 questions. Pass `--count 20 --seed 20261015` for a seeded random subset. Defaults
also include eight tool calls per answer, four concurrent case workers/API calls,
and **one data tool at a time**. `--methods sql_streaming` runs just the assisted
method. `--max-tools`, `--concurrency` (maximum eight), and `--seed` are configurable.
A directory's manifest freezes questions, source size/mtime, model/provider
configuration, code hashes, prompts, and tool definitions; changed inputs require a new output
directory. Completed answers are not regenerated when only judging needs retrying.
The output directory is locked against simultaneous writers. Files arriving after
the initial snapshot are recorded and excluded from that run; modifying or removing
an existing source file invalidates it.

The launcher enforces **2 GiB for the entire process tree, no swap, one CPU
equivalent, and 128 tasks** using a systemd user scope. The Python entry point
refuses to run without the memory/swap/CPU limits. Every data operation additionally
runs in a fresh process with 1.5 GiB address space, 20 CPU seconds, a 30-second wall
timeout, one-core affinity, and reduced scheduling priority. DuckDB uses one thread,
128 MiB, no spill, no extension loading, and an exact source-file allowlist. Only
one SELECT statement is accepted; external files and networking are unavailable.

### Models, providers, and audit files

`config/parquet-models.json` pins the answer models to `google/gemini-3.8-flash`
with `google-ai-studio`, and `openai/gpt-6-luna` with `openai`. The judge is
`anthropic/claude-haiku-5.5` with `anthropic`. Every POST sends `provider.only`,
`provider.order`, `allow_fallbacks: false`, and `require_parameters: true`. Model
availability, the exact endpoint tag, parameter support, and reasoning effort are
checked against fresh gateway metadata before running. Unsupported or unavailable
pins fail explicitly. The actual model and reported provider are checked on every
successful response; provider identity is marked unverified if absent.

Reasoning effort is `low`. Temperature is requested as zero in configuration,
sent only where the pinned endpoint advertises support (currently Gemini), and
recorded as omitted/unsupported for Luna and Haiku. This is not a claim of fully
deterministic output. Provider routing follows the
[OpenRouter provider-selection API](https://openrouter.ai/docs/guides/routing/provider-selection).

Each run under `reports-whole-data/` contains:

| Artifact | Contents |
|---|---|
| `raw/*.request.json`, `raw/*.response.json` | Full request body saved **before sending**; full response body, HTTP status and selected diagnostic headers for every attempt. No authorization headers. |
| `requests.jsonl` | Tag, UTC start/end, model/provider requested and returned, temperature, reasoning, token limit, routing, queue time, HTTP round-trip time, finish reason, response ID, complete usage/token/cache/reasoning/cost details, status, error, retry attempt and raw-file links. |
| `metadata-requests.jsonl` | Discovery GET request URLs/status/timing; corresponding complete payloads are in `raw/`. |
| `tools.jsonl` | Every tool call/arguments/output, timestamps, queue/execution time, cache hits and worker resource metrics. |
| `results/*.json` | Question/reference, answer, full tool evidence, judge reply, scores/rationale, case duration and errors. |
| `models.json`, `model-pins.json`, `endpoints-*.json` | Fresh model catalog, exact provider endpoint, capabilities and advertised prices. |
| `manifest.json`, `sources.json`, `selection.json` | Reproducible inputs, schemas, row counts and metadata snapshot; corpus files are not copied or hashed in full. |
| `preflight.json`, `postflight.json` | Live health checks and source-change detection. |
| `resources.jsonl`, `execution.jsonl` | Cgroup memory/CPU/OOM counters, limits, code hashes and run/resume timestamps. |
| `report.html` | Offline table, log-cost/correctness scatter plot, and per-answer inspector with links to raw results. |
| `summary.json`, `summary.csv`, `billing.json` | Per-model/method quality, cost, timing, errors and resource aggregates; total bill includes preflight and failed/retried calls. |

Interrupted in-flight requests remain visible as request files without completed
audit entries; their billing is unknown and listed as interrupted requests in `billing.json`.
Reconcile them before treating the total as final. `cost_complete` is false while
any inference attempt has unknown billing. HTTP/transport failures and malformed responses retain their
raw files. A missing API cost is unknown, never a measured zero. Latency is local
HTTP wall time including gateway/provider work, not a separately measured provider
compute time. Complete raw responses preserve additional fields the gateway returns.

The default shared cache reuses identical successful tool operations across cases
and models within a fixed source snapshot. Cache hits are recorded; use `off` for
an uncached latency comparison. API calls are concurrent, not a provider Batch API;
no batch discount is assumed. Exact reported API costs take precedence over catalog
price estimates. This is a bounded agent-retrieval benchmark, not guaranteed coverage
of every document. Raw results contain selected filing evidence, not the full corpus.
Tool caches and resume checkpoints remain local and are excluded from Git.

### Inspect every question/model/strategy

The Parquet report now has question, model and strategy filters. Each matching
case shows exact tool counts, every query/argument and returned output, the full
context actually sent for the final answer, final answer, exact Haiku input,
correctness/citation-support scores and rationale, plus all API attempt metadata
and raw request/response links. Requested calls, dispatched calls, cache hits,
errors and budget rejections are separate counts.

`cases-index.json` / `cases-index.csv` index the complete `cases/<task_id>.json`
records. Rebuild exports and HTML for an existing run **without paid model calls**:

```bash
.venv/bin/python -m agentic_search_only.parquet_report reports-whole-data/smoke-verified-20261015

.venv/bin/python -m agentic_search_only.case_audit \
  reports-whole-data/smoke-verified-20261015 \
  --question Q001 --model openai/gpt-6-luna --method sql --full
```

See [the architecture and audit guide](docs/parquet-agent-architecture.md) for data
sizes, data flow, resource limits, count definitions, measured smoke results and
qualified cost/time expectations. Case exports are derived artifacts; original
request logs, benchmark results and their provenance remain unchanged.


### Saved runs after relocation

The existing reports were moved intact. Historical raw payloads, source paths,
module names and original code hashes still describe how those runs were actually
executed. Their HTML/case exports can be rebuilt offline with this package. Start
new benchmark executions in a fresh output directory; the old smoke directories
retain their original immutable inputs and are not resumed across the package
move. Future runs resume normally within the new layout.

Folder layout:

```text
agentic-search-only/
├── src/agentic_search_only/   # Agent, isolated data workers, API client, audits/report
├── scripts/                  # Resource-limited launcher and browser verification
├── config/                   # Model/provider pins
├── questions/                # Independent 100-question input snapshot
├── docs/                     # Architecture, data sizes, audit schema
├── tests/                    # Independent tests
├── reports-whole-data/       # Raw requests, results, HTML, CSV, JSON
├── pyproject.toml            # Four runtime dependencies
├── uv.lock                   # Reproducible dependency versions
└── .env.example              # Optional local credential settings
```

`../data/parsed/` is shared and stays at its original location.

## Audit and dashboard for the saved 100-question run

Open [`index.html`](index.html) directly in a browser. It works offline and includes:

- Brief corpus/model/provider setup and an explicit completeness/billing warning.
- A sortable model summary, optionally split into four model/strategy rows.
- A 100-question score comparison table and a 400-case answer/judge/tool table.
- Search, model/strategy/difficulty/status filters, page sizes/pagination, column
  visibility checkboxes and export of the filtered visible columns as CSV.
- Case inspection with final answer, reference, correctness/citation scores,
  rationale, every tool result, exact sent context, parameters, usage and raw links.
- Cost/correctness plot and an audit section describing failures and limitations.

The saved run has **399 scored cases and one failed case**, Q024 / Gemini Flash /
SQL + streaming. Four failed HTTP attempts recovered on retry but have unknown
billing. This dashboard preserves those failures rather than completing them or
inventing scores. The original run remains unchanged.

The analyzer and derived files are separate from `reports-whole-data/all100/`:

```bash
# From the repository root, rebuild offline under memory/CPU limits:
agentic-search-only/scripts/analyze-results.sh
```

Outputs in `analysis/all100/`:

| File | Contents |
|---|---|
| `audit-report.md`, `audit.json` | Human-readable findings and mechanical sanity checks. |
| `benchmark.json` | Full formatted JSON with all 400 case audits, evidence, exact sent contexts, answers, judge inputs/scores and aggregate metrics; about 85 MiB. |
| `dashboard.json`, `dashboard-data.js` | Compact metrics and table rows, about 2 MiB; used by the page. |
| `details/*.json`, `details/*.js` | Individual full cases, loaded only when inspected. |
| `cases.csv` | Flat case metrics, answers and scores. |
| `corpus-audit.json` | Identifier/checksum metadata counts and duplicate examples; no full Markdown scan. |
| `original-files.sha256.json`, `preservation.json` | Before/after integrity checks for every original run file. |
| `browser-validation.json` | Real-browser feature and layout checks. |

The full formatted JSON is intentionally large; the dashboard loads only the
compact data plus the selected case, with a four-case memory cache. It does not
load every full conversation into browser memory. No CDN or external JavaScript
library is required. Source data is not bundled with the page.

For HTTP serving instead of opening the file directly:

```bash
python3 -m http.server 8000 --directory agentic-search-only
# Open http://localhost:8000/
```

Analysis does not invoke the LLM client, retry missing answers, rejudge cases,
rewrite summaries, or alter any original saved run files. Its CLI refuses to run
without the existing 2 GiB / zero-swap / one-CPU resource scope. Custom `--run` and
`--output` are supported; outputs must not overlap the original run directory.
The defaults target the saved `all100` run; another run requires
rebuilding the derived data referenced by the page.

The dashboard starts with section 01 and the cost/correctness chart, ahead of the
source/model setup. The summary's leading metric columns are **Avg score overall**,
**Total cost**, and **Avg time / question** (per question per strategy). A second
table inside the same section breaks scores down by the original six difficulty
labels, showing scored counts. The strategy-split toggle updates both tables;
pooled difficulty means weight actual scored cases, including the missing medium
case. The chart legend previews a strategy on hover/focus and isolates it on
click; **All strategies** restores both methods. Original results are unchanged.

## Published report

The GitHub Pages site has two tabs: the original 20-question indexed-retrieval
benchmark and this 100-question agentic-search benchmark. Build both locally from
the repository root:

```bash
python3 scripts/build-pages.py --output _site
python3 -m http.server 8000 --directory _site
```

Open `http://localhost:8000/`. The build expands the committed compressed report
in `published/`; no source corpus or model access is needed. The original run,
smoke outputs and generated `analysis/` directory remain local and ignored.
See [`published/README.md`](published/README.md) for archive contents and rebuilds.
