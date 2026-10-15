#!/usr/bin/env python3
"""Cross-check local source-corpus counts using DuckDB and PyArrow metadata columns."""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path

import duckdb
import pyarrow.parquet as pq

PROJECT = Path(__file__).resolve().parents[1]


def inspect(source):
    paths = sorted(source.rglob('*.parquet'))
    if not paths:
        raise ValueError('No source Parquet files found')
    connection = duckdb.connect(config={'threads': '1', 'memory_limit': '256MB'})
    sql = '''SELECT count(*) AS rows, count(DISTINCT file_stem) AS unique_document_ids,
        count(DISTINCT accession) AS unique_company_ids, count(DISTINCT md5) AS unique_stored_hashes,
        count(*) FILTER (WHERE char_count > 0) AS rows_with_positive_char_count
        FROM read_parquet(?)'''
    def query(files):
        result = connection.execute(sql, [[str(p) for p in files]])
        return dict(zip([d[0] for d in result.description], result.fetchone()))
    totals = query(paths)
    document_ids, company_ids, hashes = set(), set(), set()
    files = []
    for path in paths:
        item = query([path])
        ids, companies, content_hashes = set(), set(), set()
        rows, positive, null_markdown = 0, 0, 0
        parquet = pq.ParquetFile(path, pre_buffer=False)
        for batch in parquet.iter_batches(batch_size=1024,
                columns=['file_stem', 'accession', 'md5', 'char_count'], use_threads=False):
            for row in batch.to_pylist():
                rows += 1
                positive += bool(row['char_count'] and row['char_count'] > 0)
                if row['file_stem'] is not None: ids.add(row['file_stem'])
                if row['accession'] is not None: companies.add(row['accession'])
                if row['md5'] is not None: content_hashes.add(row['md5'])
        for group in range(parquet.num_row_groups):
            metadata = parquet.metadata.row_group(group)
            text_column = next(metadata.column(i) for i in range(metadata.num_columns)
                               if metadata.column(i).path_in_schema == 'parsed_md')
            stats = text_column.statistics
            if stats is None or stats.null_count is None:
                null_markdown = None
                break
            null_markdown += stats.null_count
        arrow = dict(rows=rows, unique_document_ids=len(ids), unique_company_ids=len(companies),
                     unique_stored_hashes=len(content_hashes), rows_with_positive_char_count=positive)
        if item != arrow or rows != parquet.metadata.num_rows:
            raise ValueError(f'DuckDB/PyArrow count mismatch: {path.name}')
        document_ids.update(ids); company_ids.update(companies); hashes.update(content_hashes)
        files.append(dict(filename=str(path.relative_to(source)), bytes=path.stat().st_size,
                          **item, markdown_nulls_from_footer=null_markdown, independent_counts_match=True))
    arrow_totals = dict(rows=sum(f['rows'] for f in files), unique_document_ids=len(document_ids),
                        unique_company_ids=len(company_ids), unique_stored_hashes=len(hashes),
                        rows_with_positive_char_count=sum(f['rows_with_positive_char_count'] for f in files))
    if totals != arrow_totals:
        raise ValueError('Global DuckDB/PyArrow totals disagree')
    return dict(checked_utc=datetime.now(timezone.utc).isoformat(), source_scope='data/parsed only; no external SFD corpus or pipeline outputs',
                file_count=len(files), bytes=sum(f['bytes'] for f in files), **totals,
                extra_rows_beyond_unique_document_ids=totals['rows']-totals['unique_document_ids'],
                independent_counts_match=True, files=files, sql=sql,
                definitions=dict(unique_document_ids='COUNT(DISTINCT file_stem); unique filing accession IDs, not physical files or guaranteed distinct content.',
                                 unique_company_ids='COUNT(DISTINCT accession); company CIK identifiers.',
                                 unique_stored_hashes='COUNT(DISTINCT md5); stored hashes, not recomputed checksums.',
                                 rows='All stored records, including repeated IDs.',
                                 physical_files='Only .parquet containers recursively under the selected source directory.'),
                resource_notes='One DuckDB thread, 256 MB query memory limit; PyArrow batches of 1,024 metadata-only rows. Markdown is not decoded.')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, default=PROJECT.parent/'data/parsed')
    parser.add_argument('--output', type=Path, default=PROJECT/'published/corpus-verification.json')
    args = parser.parse_args()
    result = inspect(args.source)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2)+'\n')
    encoded = json.dumps(result, ensure_ascii=False).replace('<', '\\u003c')
    args.output.with_suffix('.js').write_text('window.BENCHMARK_CORPUS_VERIFICATION='+encoded+';\n')
    print(json.dumps({k:v for k,v in result.items() if k not in {'files','sql','definitions'}}))


if __name__ == '__main__':
    main()
