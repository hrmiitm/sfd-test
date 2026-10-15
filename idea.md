Your plan is good. I suggest building one common financial QA benchmark platform where LanceDB, Qdrant, and Pathway can be switched without changing the questions, documents, or LLMs.

## 1. Overall architecture

SFD-v1 — 638,067 financial filings

6 Parquet files · 6.44 GB compressed

Common preprocessing pipeline

DuckDB → metadata → sections → chunks → embeddings

LanceDB

Start here

Qdrant

Second

Pathway

Third

Retrieval + LLM

BM25 / Vector / Hybrid / Agentic SearchGemini · Claude · GPT via OpenRouter

Benchmark dashboard

100 questions · Accuracy · Citations · Latency · Cost

## 2. Step 1 — Build 100 verified questions first

Do not generate questions randomly without checking the original document. Every question must have a verified answer and supporting evidence.

Here is the distribution I recommend:

| Difficulty   | Questions | Type                                              |
| ------------ | --------- | ------------------------------------------------- |
| Easy         | 20        | Direct factual lookup                             |
| Medium       | 20        | Locate information in long filings                |
| Hard         | 25        | Financial calculations and comparisons            |
| Very hard    | 25        | Multi-document / multi-step reasoning             |
| Extreme      | 5         | Conflicting evidence, complex numerical reasoning |
| Unanswerable | 5         | Test whether LLM hallucinates                     |
| Total        | 100       |                                                   |

For each question store:

```
{
  "id": "Q001",
  "difficulty": "hard",
  "question": "...",
  "ground_truth": "...",
  "filing_accessions": ["..."],
  "evidence": [
    {
      "accession": "...",
      "section": "...",
      "exact_quote": "..."
    }
  ],
  "answer_type": "numerical",
  "verification_status": "verified"
}
```

Use DuckDB to select diverse filings, then an LLM to propose questions and answers from those filings. Independently validate all 100 against the actual text, especially calculations and citations.

Important: The gold evidence must be hidden from the answering system. It is only for evaluation.

## 3. Step 2 — Build LanceDB ingestion

Your first implementation should:

1. Read Parquet in batches without exhausting RAM.
2. Extract company identifiers, SEC form type and sections.
3. Split filings into meaningful chunks, preserving tables.
4. Generate embeddings using one fixed model.
5. Store text, vectors, section metadata, and original document references in LanceDB.
6. Build BM25 and vector indexes.

LanceDB supports full-text BM25, vector search, and hybrid retrieval.&#x20;

[image](https://www.google.com/s2/favicons?domain=https://github.com\&sz=32)

GitHub

+1



Start with 1,000 filings to validate the pipeline, then 10,000, 100,000, and finally all 638,067. The full dataset could produce millions of chunks, requiring much more disk space than the original 6.44 GB.

## 4. Step 3 — Run multiple retrieval methods

| Method         | What happens                                               |
| -------------- | ---------------------------------------------------------- |
| BM25           | Keyword retrieval                                          |
| Vector RAG     | Semantic similarity search                                 |
| Hybrid RAG     | Combine BM25 and vector results                            |
| Agentic Search | LLM performs multiple searches, filters and reads evidence |

Each method sends retrieved evidence to the selected Gemini, Claude, or GPT model, which produces an answer with filing accession and section/chunk citations.

SFD-v1 contains reconstructed Markdown, so do not invent page-number citations. Use stable document and section references unless genuine page boundaries are available.

## 5. Step 4 — Evaluate

Measure these dimensions separately:

| Metric               | Purpose                                  |
| -------------------- | ---------------------------------------- |
| Answer correctness   | Is the answer factually correct?         |
| Recall\@K            | Was the required evidence retrieved?     |
| Citation correctness | Do cited documents support the answer?   |
| Hallucination rate   | Does it invent unsupported claims?       |
| Latency              | How long did retrieval + answering take? |
| Cost                 | Embedding, indexing and LLM expenses     |
| Resource usage       | RAM, CPU and disk                        |

Use deterministic checks for numerical answers where possible, and an LLM judge with explicit criteria for open-ended answers. Audit a sample of judge decisions manually.

## 6. Step 5 — Replace LanceDB with Qdrant and Pathway

Use the same preprocessed chunks, IDs, embedding model, 100 questions, LLM settings and retrieval limits.

Qdrant supports dense and sparse search with hybrid retrieval. Pathway provides vector, BM25 and hybrid retrievers, but it is also a data-processing framework, so benchmark its incremental indexing capability separately.&#x20;

[image](https://www.google.com/s2/favicons?domain=https://qdrant.tech\&sz=32)

Qdrant

+1



## My suggested implementation sequence

Phase 1 — Generate and verify 100 Q&A

First

Phase 2 — LanceDB + 1,000 filings + 3 retrieval methods

Phase 3 — Scale LanceDB to the full dataset

Phase 4 — Gemini/Claude/GPT evaluations + dashboard

Phase 5 — Repeat with Qdrant

Phase 6 — Repeat with Pathway

One essential consideration: 100 questions cannot represent all 638,067 filings individually. Instead, sample across companies, filing types and reasoning requirements, and ensure the questions remain answerable as you scale the corpus.

I would also reserve 20 of the 100 questions as a final held-out test set, using the other 80 to debug retrieval. Otherwise, repeated tuning on the same questions will make your final comparison less credible.

The most important deliverable to create first is a verified `questions.jsonl` with 100 questions, answers, source accession IDs, and supporting evidence. Everything else can be built around it.