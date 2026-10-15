#!/usr/bin/env python3
"""
Automated Verification Suite for SEC EDGAR 100 Benchmark Questions.

Validates:
1. Exactly 100 questions present.
2. Sequential IDs from Q001 to Q100.
3. Precise difficulty distribution (20 easy, 20 medium, 25 hard, 25 very hard, 5 extreme, 5 unanswerable).
4. Full schema integrity (all required fields present and non-empty).
5. Ground truth evidence verification: queries the parquet dataset via DuckDB and
   strictly asserts that every evidence quote is an exact, verbatim substring of parsed_md.
"""

import sys
import os
import json
from pathlib import Path

try:
    import duckdb
except ImportError:
    print("Error: duckdb not installed. Install with `pip install duckdb`.")
    sys.exit(1)

BASE_DIR = Path(__file__).resolve().parent.parent
QUESTIONS_JSONL = BASE_DIR / "questions" / "questions.jsonl"
QUESTIONS_JSON = BASE_DIR / "questions" / "questions.json"
DATA_DIR = BASE_DIR / "data" / "parsed"

REQUIRED_FIELDS = [
    "id",
    "difficulty",
    "question",
    "ground_truth",
    "filing_accessions",
    "evidence",
    "answer_type",
    "verification_status",
]

EXPECTED_DISTRIBUTION = {
    "easy": 20,
    "medium": 20,
    "hard": 25,
    "very_hard": 25,
    "extreme": 5,
    "unanswerable": 5,
}


def main():
    print("=" * 70)
    print("SEC EDGAR 100 BENCHMARK VERIFICATION SUITE")
    print("=" * 70)

    # 1. Load questions.jsonl
    if not QUESTIONS_JSONL.exists():
        print(f"FAILED: File not found: {QUESTIONS_JSONL}")
        sys.exit(1)

    questions = []
    with open(QUESTIONS_JSONL, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
                questions.append(data)
            except json.JSONDecodeError as e:
                print(f"FAILED: JSON parse error on line {line_num}: {e}")
                sys.exit(1)

    print(f"✓ Loaded {len(questions)} questions from {QUESTIONS_JSONL.name}")

    # Check exactly 100 questions
    if len(questions) != 100:
        print(f"FAILED: Expected 100 questions, but found {len(questions)}")
        sys.exit(1)
    print("✓ Count check passed: Exactly 100 questions present")

    # 2. Check JSON formatting parity
    if QUESTIONS_JSON.exists():
        with open(QUESTIONS_JSON, "r", encoding="utf-8") as f:
            json_array = json.load(f)
            if len(json_array) != 100:
                print(f"FAILED: questions.json contains {len(json_array)} questions, expected 100")
                sys.exit(1)
        print(f"✓ Formatted questions.json parity verified (100 questions)")

    # 3. Validate sequential IDs & schema
    difficulty_counts = {}
    answer_type_counts = {}
    unique_accessions = set()
    errors = []

    for idx, q in enumerate(questions, 1):
        expected_id = f"Q{idx:03d}"
        actual_id = q.get("id")

        if actual_id != expected_id:
            errors.append(f"Question index {idx} has ID '{actual_id}', expected '{expected_id}'")

        # Check required fields
        for field in REQUIRED_FIELDS:
            if field not in q or q[field] is None or (isinstance(q[field], str) and not q[field].strip()):
                errors.append(f"{actual_id}: Missing or empty required field '{field}'")

        diff = q.get("difficulty")
        difficulty_counts[diff] = difficulty_counts.get(diff, 0) + 1

        atype = q.get("answer_type")
        answer_type_counts[atype] = answer_type_counts.get(atype, 0) + 1

        acc_list = q.get("filing_accessions", [])
        if not isinstance(acc_list, list) or len(acc_list) == 0:
            errors.append(f"{actual_id}: 'filing_accessions' must be a non-empty list")
        for a in acc_list:
            unique_accessions.add(a)

        ev_list = q.get("evidence", [])
        if not isinstance(ev_list, list) or len(ev_list) == 0:
            errors.append(f"{actual_id}: 'evidence' must be a non-empty list")
        else:
            for ev_idx, ev in enumerate(ev_list):
                if not ev.get("accession"):
                    errors.append(f"{actual_id}: evidence[{ev_idx}] missing 'accession'")
                if not ev.get("exact_quote"):
                    errors.append(f"{actual_id}: evidence[{ev_idx}] missing 'exact_quote'")
                if not ev.get("section"):
                    errors.append(f"{actual_id}: evidence[{ev_idx}] missing 'section'")

    if errors:
        print(f"FAILED: Schema errors found ({len(errors)}):")
        for err in errors[:10]:
            print(f"  - {err}")
        sys.exit(1)

    print("✓ Schema validation passed: All fields valid across 100 questions")

    # 4. Validate difficulty distribution
    print("\nDifficulty Distribution:")
    for diff, count in EXPECTED_DISTRIBUTION.items():
        actual_count = difficulty_counts.get(diff, 0)
        status = "✓" if actual_count == count else "✗"
        print(f"  {status} {diff.capitalize():<15}: {actual_count:>2} / {count}")
        if actual_count != count:
            errors.append(f"Difficulty '{diff}' count mismatch: expected {count}, got {actual_count}")

    if errors:
        print("FAILED: Difficulty distribution mismatch.")
        sys.exit(1)

    # 5. Connect to DuckDB and verify exact quotes in parquet files
    parquet_files = sorted(list(DATA_DIR.glob("sfd-v1-*.parquet")))
    if not parquet_files:
        print(f"FAILED: No parquet files found in {DATA_DIR}")
        sys.exit(1)

    print(f"\nConnecting to DuckDB using {len(parquet_files)} parquet chunks...")
    conn = duckdb.connect()

    filing_cache = {}

    def fetch_filing(accession):
        if accession in filing_cache:
            return filing_cache[accession]
        for pq in parquet_files:
            res = conn.execute(
                f"SELECT parsed_md FROM '{pq}' WHERE file_stem = '{accession}'"
            ).fetchone()
            if res:
                filing_cache[accession] = res[0]
                return res[0]
        return None

    print(f"Pre-loading filings for {len(unique_accessions)} unique accessions...")
    missing_accessions = []
    for acc in sorted(unique_accessions):
        md = fetch_filing(acc)
        if md is None:
            missing_accessions.append(acc)

    if missing_accessions:
        print(f"FAILED: Could not find {len(missing_accessions)} accessions in parquet files:")
        for acc in missing_accessions:
            print(f"  - {acc}")
        sys.exit(1)

    print(f"✓ All {len(unique_accessions)} unique filing accessions successfully found")

    print("\nVerifying exact verbatim quotes against parsed markdown...")
    quote_errors = []
    total_quotes = 0

    for q in questions:
        qid = q["id"]
        for ev in q["evidence"]:
            total_quotes += 1
            acc = ev["accession"]
            quote = ev["exact_quote"]
            md = filing_cache[acc]
            if quote not in md:
                quote_errors.append({
                    "id": qid,
                    "accession": acc,
                    "quote_snippet": quote[:80] + ("..." if len(quote) > 80 else ""),
                })

    if quote_errors:
        print(f"FAILED: {len(quote_errors)} / {total_quotes} evidence quotes could not be matched verbatim:")
        for qe in quote_errors[:5]:
            print(f"  - {qe['id']} on {qe['accession']}: {qe['quote_snippet']}")
        sys.exit(1)

    print(f"✓ 100% Quote Verification PASSED: All {total_quotes} quotes match verbatim in source filings!")

    # 6. Summary Statistics
    print("\n" + "=" * 70)
    print("BENCHMARK SUMMARY STATISTICS")
    print("=" * 70)
    print(f"Total Questions            : {len(questions)}")
    print(f"Total Verbatim Quotes      : {total_quotes}")
    print(f"Unique Filings Referenced  : {len(unique_accessions)}")
    print("\nAnswer Types Breakdown:")
    for atype, cnt in sorted(answer_type_counts.items(), key=lambda x: -x[1]):
        print(f"  - {atype:<20}: {cnt:>2} questions")

    print("\n" + "=" * 70)
    print("ALL VERIFICATION CHECKS PASSED (100 / 100 VERIFIED)")
    print("=" * 70)
    sys.exit(0)


if __name__ == "__main__":
    main()
