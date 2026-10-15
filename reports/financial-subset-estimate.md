# Annual and quarterly filing subset

The six local Parquet files contain **5,870 exact 10-K filings and 6,365 exact
10-Q filings: 12,235 total**. These counts match the screenshot. Their combined
source Markdown is **5.254 billion characters**, about **6.8% of the corpus's
text**, despite representing only 1.9% of its filings.

## Estimated resource use for both local databases

| Item | Financial subset | Previous full-corpus estimate |
|---|---:|---:|
| Filings | **12,235 exact** | 638,067 |
| Chunks | ~2.38 million | ~43.25 million |
| Embedding tokens | ~1.42 billion | ~20.71 billion |
| Embedding cost | **~$28.45** | ~$414 |
| Overall runtime | **~26 hours** | ~16 days |
| Artifacts + both indexed databases | **~68 GB** | ~1.24 TB |

The subset is about **93% cheaper and 95% smaller on disk** than the full-corpus
projection. It fits the roughly 300 GiB currently free. Allow **100 GB of free
space** for temporary files and index construction; more may be needed if old
versions, backups, or extra corpora are retained. This is considerably smaller,
but it is still approximately a day-long job, not a few-minute ingestion.

The central runtime estimate comprises roughly 2.5 hours of preprocessing,
21.2 hours of embedding, and 2 hours of database ingestion/indexing. These are
projections, not a completed subset benchmark. For planning, allow approximately
**$20–45, 20–50 hours, and 50–100 GB**; these are practical allowances, not
statistical confidence intervals. Index growth, report variability, gateway
latency, and rate limits can move results outside these ranges.

## What was actually measured

- All 638,067 source rows were scanned in about 245 seconds to count form types
  and source characters. No embedding calls were made.
- Four filings per form per month were randomly sampled with a deterministic
  seed: **48 filings total**, covering all six months and both form types.
- Those samples were passed through the actual 700-token chunker. Sample tokens,
  chunks, and chunking times were scaled to each month/form's exact text size.
- API rate and throughput, database times, and storage density come from the
  earlier completed 1,000-filing run. The observed rate was $0.02 per million
  tokens for `openai/text-embedding-3-small`, with 1,536 dimensions.
- Embeddings are generated once and shared between LanceDB and Qdrant. Any
  separate Foundry billing markup is unknown. Existing mixed-corpus embeddings
  are not automatically reused across workspaces.

Local labels come from `CONFORMED SUBMISSION TYPE` in reconstructed Markdown,
not independent SEC metadata validation. The strict subset excludes 566
`10-K/A` and 115 `10-Q/A` amendments.

## Existing benchmark coverage

A strict 10-K/10-Q subset removes evidence cited by **six existing questions**:
`Q020`, `Q076`, `Q079`, `Q080`, `Q081`, and `Q082`.

They refer to just five additional source documents: four 8-Ks and one Form 4.
Keeping those five alongside the annual/quarterly reports gives **12,240 filings**
and preserves the cited-source coverage of the current benchmark, with negligible
extra cost compared with the estimates above. This is only an accession coverage
check, not independent verification of the questions or their answers.

| Accession | Form |
|---|---|
| `0000829224-25-000006` | 8-K |
| `0000950170-25-011901` | 4 |
| `0001104659-25-001594` | 8-K |
| `0000077476-25-000008` | 8-K |
| `0000078003-25-000017` | 8-K |

No question files were changed.

## Implemented commands

Strict annual and quarterly reports:

```bash
./scripts/run-financial-subset.sh
```

Annual and quarterly reports plus those five benchmark sources:

```bash
./scripts/run-financial-benchmark-subset.sh
```

Both commands start local Qdrant, preprocess the selected filings, embed them
through Foundry, and ingest/index both databases. They use dedicated workspaces
(`data/pipeline/financial-10k-10q` and `data/pipeline/financial-benchmark`) and leave
the existing 1,000-filing databases intact. They make billable embedding requests.
Repeat the same command to resume a stopped run.

For preprocessing alone, without embedding charges:

```bash
uv run sfd-pipeline prepare --forms 10-K 10-Q --limit 0 \
  --work data/pipeline/financial-10k-10q
```

The general CLI accepts `--forms` and optional `--include-accessions` exceptions.
Limits count matching filings, while source row offsets and chunk IDs retain
original-file provenance. Nine tests pass, including filtered limits, exact
form matching, resume/configuration protection, and accession exceptions.

Machine-readable evidence: `reports/financial-subset-profile.json`,
`reports/financial-subset-estimate.json`, and
`reports/financial-subset-question-coverage.json`. Per-sample measurements are
in `data/pipeline/financial-profile/sample-measurements.json`.

The paid full-subset run has **not** been started as part of this assessment.
The outstanding choice is strict annual/quarterly reports versus retaining the
five benchmark source exceptions, and whether the estimated day-long/$28 run
meets the requested "small enough" condition.
