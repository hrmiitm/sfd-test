# SEC EDGAR 100 Verified Questions Benchmark

This directory contains the **100 Verified Financial QA Benchmark Questions** constructed from the SEC Filing Dataset (2025 filings, `data/parsed/`).

This benchmark is specifically designed to rigorously evaluate Retrieval-Augmented Generation (RAG) engines (e.g., LanceDB, Qdrant, Pathway) and Large Language Models on complex, real-world financial documents.

---

## 1. Benchmark Overview & Distribution

The benchmark consists of **exactly 100 verified questions** matching the difficulty breakdown specified in [`idea.md`](file:///home/kit/Documents/sfd-2025/idea.md#L45-L68):

| Difficulty Level | Question IDs | Count | Description & Cognitive Load |
| :--- | :--- | :---: | :--- |
| **Easy** | `Q001` – `Q020` | **20** | **Direct Factual Lookup**: Single retrieval step for explicitly stated metadata (auditor firm, state of incorporation, fiscal year-end, shares outstanding, ticker symbols, CIK). |
| **Medium** | `Q021` – `Q040` | **20** | **Long-Context Information Retrieval**: Locating buried disclosures inside 100+ page filings (segment revenues, geographic breakdown, restructuring plans, lease commitments, stock repurchase authorizations). |
| **Hard** | `Q041` – `Q065` | **25** | **Financial Calculations & Metric Synthesis**: Multi-hop math within a filing (operating margins, YoY growth rates, effective tax rates, CapEx/Cash Flow ratios, working capital, gross margin expansion). |
| **Very Hard** | `Q066` – `Q090` | **25** | **Multi-Document Reasoning & Cross-Filing Synthesis**: Comparative analysis between competing corporate peers (e.g., Alphabet vs Amazon, NVIDIA vs Intel, Citigroup vs Goldman Sachs, ConocoPhillips vs Marathon) and cross-form temporal synthesis (Form 10-K + Form 10-Q + Form 8-K + Form 4). |
| **Extreme** | `Q091` – `Q095` | **5** | **Subtle Adjustments & Non-GAAP Metrics**: Reconciliations between conflicting disclosures (Constant Currency vs GAAP growth, Goodwill & intangibles impairments, Non-GAAP Adjusted Gross Profits, discounted vs undiscounted lease liabilities, Stock-Based Compensation cash flow dynamics). |
| **Unanswerable** | `Q096` – `Q100` | **5** | **Hallucination Detection Tests**: Queries concerning undisclosed or counterfactual metrics (e.g., granular monthly sales, future 2027 guidance, terminated acquisitions like Adobe/Figma, suspended dividends, undisclosed chip unit shipments). |
| **TOTAL** | `Q001` – `Q100` | **100** | **100% Verifiable with Verbatim Evidence Quotes** |

---

## 2. Directory Structure

```text
questions/
├── questions.jsonl         # Primary benchmark file (1 JSON object per line)
├── questions.json          # Formatted JSON array for inspection & web visualization
├── verify_questions.py     # Automated end-to-end verification test suite
└── README.md               # Dataset documentation and evaluation guide
```

---

## 3. Schema Specification

Each question adheres strictly to the benchmark schema:

```json
{
  "id": "Q001",
  "difficulty": "easy",
  "question": "Who was the independent registered public accounting firm that audited Alphabet Inc.'s consolidated financial statements for the fiscal year ended December 31, 2024?",
  "ground_truth": "Ernst & Young LLP",
  "filing_accessions": [
    "0001652044-25-000014"
  ],
  "evidence": [
    {
      "accession": "0001652044-25-000014",
      "section": "Item 8. Consolidated Financial Statements - Report of Independent Registered Public Accounting Firm",
      "exact_quote": "We have audited the accompanying consolidated balance sheets of Alphabet Inc. (the Company) as of December 31, 2024 and 2023, the related consolidated statements of income, comprehensive income, stockholders' equity, and cash flows for each of the three years in the period ended December 31, 2024"
    }
  ],
  "answer_type": "text",
  "verification_status": "verified"
}
```

### Field Definitions

- **`id`**: Unique identifier from `Q001` through `Q100`.
- **`difficulty`**: One of `easy`, `medium`, `hard`, `very_hard`, `extreme`, `unanswerable`.
- **`question`**: The natural-language financial query.
- **`ground_truth`**: Comprehensive, unambiguous factual answer, including exact calculation steps where applicable.
- **`filing_accessions`**: List of SEC EDGAR unique accession numbers (`file_stem`) containing the source evidence.
- **`evidence`**: List of evidence objects containing:
  - `accession`: The specific SEC filing identifier.
  - `section`: The exact Item or Note heading in the filing.
  - `exact_quote`: An exact, verbatim substring from the parsed markdown in `data/parsed/`.
- **`answer_type`**: Type of the answer (`text`, `percentage`, `currency`, `numerical`, `unanswerable`).
- **`verification_status`**: `"verified"`.

---

## 4. Companies and Filings Covered

The benchmark spans 27 distinct SEC filings across diverse sectors and form types (10-K, 10-Q, 8-K, Form 4):

| Company / Entity | Ticker / CIK | Form Type | Accession (`file_stem`) |
| :--- | :--- | :---: | :--- |
| **Alphabet Inc.** | GOOGL | 10-K | `0001652044-25-000014` |
| **Amazon.com, Inc.** | AMZN | 10-K | `0001018724-25-000004` |
| **NVIDIA Corporation** | NVDA | 10-K | `0001045810-25-000023` |
| **Intel Corporation** | INTC | 10-K | `0000050863-25-000009` |
| **Adobe Inc.** | ADBE | 10-K | `0000796343-25-000004` |
| **Levi Strauss & Co.** | LEVI | 10-K | `0000094845-25-000005` |
| **Comcast Corporation** | CMCSA | 10-K | `0001166691-25-000011` |
| **Citigroup Inc.** | C | 10-K | `0000831001-25-000067` |
| **Goldman Sachs Group, Inc.** | GS | 10-K | `0000886982-25-000005` |
| **ConocoPhillips** | COP | 10-K | `0001163165-25-000012` |
| **Marathon Petroleum Corporation** | MPC | 10-K | `0001510295-25-000012` |
| **Whirlpool Corporation** | WHR | 10-K | `0000106640-25-000011` |
| **Morningstar, Inc.** | MORN | 10-K | `0001289419-25-000041` |
| **Exelon Corporation** | EXC | 10-K | `0001109357-25-000043` |
| **C.H. Robinson Worldwide, Inc.** | CHRW | 10-K | `0001043277-25-000012` |
| **Utz Brands, Inc.** | UTZ | 10-K | `0001739566-25-000053` |
| **Sabre Corporation** | SABR | 10-K | `0001597033-25-000027` |
| **Grayscale Ethereum Mini Trust ETF** | ETH | 10-K | `0000950170-25-029407` |
| **Innovative Industrial Properties, Inc.** | IIPR | 10-K | `0001558370-25-001322` |
| **Microsoft Corporation** | MSFT | 10-Q | `0000950170-25-010491` |
| **Nike, Inc.** | NKE | 10-Q | `0000320187-25-000004` |
| **Qualcomm Incorporated** | QCOM | 10-Q | `0000804328-25-000011` |
| **Starbucks Corporation** | SBUX | 8-K | `0000829224-25-000006` |
| **Pfizer Inc.** | PFE | 8-K | `0000078003-25-000017` |
| **PepsiCo, Inc.** | PEP | 8-K | `0000077476-25-000008` |
| **Ford Motor Credit Company LLC** | F | 8-K | `0001104659-25-001594` |
| **Shriram K. (Alphabet Insider)** | GOOGL | Form 4 | `0000950170-25-011901` |

---

## 5. Automated Verification

To run the full automated verification test suite against the DuckDB parquet files:

```bash
# Using the workspace virtual environment
.venv/bin/python questions/verify_questions.py
```

### Verification Checks Performed:
1. Validates that exactly **100 questions** exist.
2. Checks sequential indexing `Q001` – `Q100`.
3. Validates that every mandatory field (`id`, `difficulty`, `question`, `ground_truth`, `filing_accessions`, `evidence`, `answer_type`, `verification_status`) is present and non-empty.
4. Confirms exact distribution:
   - 20 easy
   - 20 medium
   - 25 hard
   - 25 very hard
   - 5 extreme
   - 5 unanswerable
5. Validates that every evidence quote (113 quotes total) exists as an exact, byte-for-byte substring in `parsed_md` within `data/parsed/`.

---

## 6. How to Evaluate RAG Systems with this Benchmark

### Recommended Evaluation Metrics:

1. **Retrieval Performance (Hit Rate & MRR @ k)**:
   - Evaluates whether the retrieved chunks contain the required `filing_accessions` and cover the `exact_quote` text.
2. **Context Precision & Recall**:
   - Compares the retrieved context against the benchmark `evidence` array.
3. **Answer Correctness & Factual Accuracy**:
   - Assesses the model's generated answer against `ground_truth` using an LLM-as-a-judge (e.g., Claude 3.5 Sonnet or GPT-4o) with strict financial tolerance (numerical tolerances within ±0.1% or exact match for factual lookup).
4. **Hallucination Resistance (Unanswerable Questions Q096–Q100)**:
   - Assesses whether the model recognizes that the question cannot be answered from the document, awarding full credit when it explicitly states the information is undisclosed or unavailable.
