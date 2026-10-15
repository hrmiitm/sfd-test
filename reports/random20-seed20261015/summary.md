# RAG benchmark

Cells: correctness / 100 · reported answer + judge USD · completed/expected.
Scores exclude errors; inspect completion counts. Unknown costs are not zero.

| Model | lancedb/bm25 | lancedb/vector | lancedb/hybrid | lancedb/agentic | qdrant/bm25 | qdrant/vector | qdrant/hybrid | qdrant/agentic |
|---|---|---|---|---|---|---|---|---|
| google/gemini-3.8-flash | 71.2 · $0.292073 · 20/20 | 80.6 · $0.318304 · 20/20 | 86.4 · $0.304028 · 20/20 | 81.6 · $0.644439 · 20/20 | 71.0 · $0.301751 · 20/20 | 72.0 · $0.279674 · 20/20 | 87.2 · $0.306802 · 20/20 | 85.0 · $1.018314 · 20/20 |
| openai/gpt-6-luna | 69.0 · $0.078076 · 20/20 | 82.0 · $0.085898 · 20/20 | 88.0 · $0.081977 · 20/20 | 81.3 · $0.097821 · 20/20 | 73.0 · $0.083785 · 20/20 | 70.3 · $0.070645 · 20/20 | 83.8 · $0.081241 · 20/20 | 87.8 · $0.100966 · 20/20 |

Shared query embedding usage is in embedding-requests.jsonl and is not duplicated per cell.
Agentic columns use expanded original-source Parquet plus database search; their corpus differs from indexed-only methods.
Agentic chat costs include every planning and answering turn. Tool time is reported as retrieval latency.
Agentic retrieval recall counts index chunks and parsed filing passages; SQL-only evidence is retained in the judge trace.
requests.jsonl is the billing audit, including calls whose parsing/validation failed.
Corpus state and change detection: retrieval.json. Exact-quote recall requires a whole quote in one chunk.
Concurrent requests are client-side batching, not a provider Batch API or discounted batch pricing.
