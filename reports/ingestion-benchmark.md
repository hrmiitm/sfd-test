# Local ingestion benchmark — 15 October 2026

**Step 2 is complete for 1,000 filings in both local databases.** Each contains
43,105 chunks with the same real Foundry-generated embeddings and citation
metadata. No answering LLM, retrieval application, or evaluation benchmark was
connected. A separate 10,000-filing corpus has been preprocessed, but not embedded
or ingested.

## Databases ready for the next stages

| Database | Location | Table / collection |
|---|---|---|
| LanceDB | `data/pipeline/sample-1000/lancedb` | `sfd_355dfd240a653fec` |
| Qdrant 1.19.2 | `http://localhost:6333` | `sfd_355dfd240a653fec` |

Qdrant runs in the `sfd-qdrant` Docker container, bound to localhost, with
`unless-stopped` restart policy and persistent storage in `data/qdrant-server`.
Its dashboard is at http://localhost:6333/dashboard. LanceDB is embedded and
needs no server process.

LanceDB has B-tree ID, BM25 full-text, and cosine IVF_FLAT vector indexes.
Qdrant has named `dense` and `bm25` vectors, IDF, metadata indexes, and disk-backed
vector/index storage. Qdrant reports green status. Both exact row counts are
43,105. Self-vector and lexical index checks passed on both stores. These verify
storage/index readiness; they are not a semantic retrieval quality benchmark.

Evidence: `data/pipeline/sample-1000/ingested-both.json` and `verified.json`.

## Cost, runtime, and storage

| Filings | Chunks | Embedding tokens | Embedding cost, USD | Overall pipeline time | Stored data + indexes |
|---|---:|---:|---:|---:|---:|
| **1,000 — completed** | 43,105 | 20,605,765 | **$0.4121 actual API-reported** | **21.9 min measured** | **1.23 GB measured** |
| **10,000 — preprocessing completed** | 406,987 | 194,845,149 | **$3.90 projected** | **3.6 hours projected** | **11.7 GB projected** |
| **638,067 — full-corpus projection** | ~43.25 million | ~20.71 billion | **~$414 projected** | **~15.8 days projected** | **~1.24 TB projected** |

Costs are for embedding each corpus from scratch **once**, shared by both databases.
They are not doubled for LanceDB and Qdrant. The API reports $0.02 per million
input tokens for `openai/text-embedding-3-small` at 1,536 dimensions. Any separate
Foundry account markup is unknown. Two connection probes added $0.00000030.
No 10,000-filing or full-corpus embedding charges were incurred.

The 1,000-filing run used four concurrent workers, batches of up to 128 chunks
(automatically split when necessary), and a 700-token chunk budget. It completed
410 successful API requests; seven successful requests required retrying. Local
and API token counts matched exactly. The measured embedding wall time includes
the initial request-limit failure, recovery, and the switch to compact base64
responses, making it a conservative baseline for the corrected pipeline.

For scheduling, allow **2.7–7.1 hours for 10,000 filings** and **12–47 days for the
full corpus**, conditional on sufficient storage and resources. These are broad
planning allowances, not statistical confidence intervals. Full-scale indexing
can grow nonlinearly, and gateway rate limits can change. The full-corpus cost
and chunk count are also estimates: the sample is the first 10,000 January
filings, not a representative sample across all months.

**The full run does not fit the current disk.** Around 322 GB (300 GiB) was free
after the completed runs, versus a projected 1.24 TB of artifacts and indexes.
Provide additional capacity and headroom for temporary files, index construction,
and compaction before a full run. At this scale, validate index training memory
and throughput on a 100,000-filing run before committing to the entire corpus;
the current machine has 12 logical CPUs and approximately 16 GB RAM.

## Measured stage timings

| Stage | 1,000 filings | 10,000 filings | Full corpus |
|---|---:|---:|---:|
| Preprocessing | 1.39 min actual | 19.70 min actual | 34.9 hours projected |
| Embedding | 18.38 min observed, including recovery | 2.90 hours projected | 12.83 days projected |
| Ingestion and indexes, both databases | 2.15 min actual | 20.3 min projected | 1.50 days projected |

Within the 1,000-filing ingestion stage, LanceDB writes took 13.48 seconds,
LanceDB indexing 10.90 seconds, and Qdrant writes 100.96 seconds. Qdrant performs
index work during/background to uploads; the verification confirmed green status.
The stage sum excludes initial software/image download and code development.
The separate 10,000-filing preprocessing job ran concurrently with embeddings.

The full dataset contains **77,421,389,550 source characters** across 638,067
filings. Full-corpus tokens and chunks are extrapolated from the ratio of that
character count to the first 10,000 filings' 728,508,206 characters. Embedding
runtime scales by token count, ingestion/storage by chunk count, and preprocessing
by source characters. These assumptions explain why estimates are not simply
1,000-filing results multiplied by the filing count.

## Run or resume

```bash
# Starts local Qdrant and resumes the existing 1,000-filing pipeline.
# Existing embedding shards are reused; there are no new embedding calls.
./scripts/run-local-pipeline.sh --limit 1000 --work data/pipeline/sample-1000

# Verify the existing databases without any embedding or LLM API calls.
uv run python scripts/verify-local-databases.py

# The 10k preprocessing artifacts already exist. This command would start
# the approximately $3.90 embedding run, then ingest both databases.
./scripts/run-local-pipeline.sh --limit 10000 --work data/pipeline/sample-10000
```

The 1k and 10k workspaces are independent. The 10k command currently re-embeds its
full selected corpus; it does not deduct the previously paid 1k embeddings.

Machine-readable measurements and projections are in `reports/ingestion-benchmark.json`.
Successful request usage/latencies are in
`data/pipeline/sample-1000/embedding-requests.jsonl`. Full source counts are in
`reports/corpus-stats.json`. Seven automated tests and the live database checks
passed. The pipeline now supports the existing `LLMFOUNDRY_TOKEN` configuration,
streaming without the large-query DuckDB memory failure, request-size splitting,
compact responses, and retry handling.

Pricing reference: [OpenRouter text-embedding-3-small](https://openrouter.ai/openai/text-embedding-3-small/pricing).
Actual cost figures above come from this run's API `usage.cost` fields.
