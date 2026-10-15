#!/usr/bin/env python3
"""Prepare cited filing text plus 25 unrelated filings; never copy QA content."""
import json, random, time
from pathlib import Path
import lancedb, duckdb
import pyarrow as pa
import pyarrow.parquet as pq

base=Path('data/pipeline/benchmark-additions');out=base/'sources';out.mkdir(parents=True,exist_ok=True)
manifest=base/'source-manifest.json'
questions=json.loads(Path('questions/questions.json').read_text())
required=sorted({a for q in questions for a in q.get('filing_accessions',[])}|{e['accession'] for q in questions for e in q.get('evidence',[]) if e.get('accession')})
if manifest.exists():
    assert json.loads(manifest.read_text())['cited_accessions']==required,'Question references changed; use a new workspace'
    if (out/'documents.parquet').exists():
        print('Reusing document-only additions');raise SystemExit
started=time.perf_counter()
target=json.loads(Path('data/pipeline/sample-1000/ingested-both.json').read_text())
table=lancedb.connect('data/pipeline/sample-1000/lancedb').open_table(target['collection'])
existing={r['accession'] for r in table.search().select(['accession']).limit(None).to_arrow().to_pylist()}
cited=pq.read_table('data/pipeline/benchmark-fast/sources/documents.parquet').to_pylist()
assert {r['file_stem'] for r in cited}==set(required)
assert not (existing&set(required)), 'Some cited documents are already loaded; inspect before preparing additions'
paths=[str(p) for p in sorted(Path('data/pipeline/financial-profile').glob('*.parquet')) if not p.name.endswith('-sample.parquet') and ('2025-01.' in p.name or '2025-02.' in p.name)]
c=duckdb.connect();catalog=c.execute('SELECT source_file,source_row,accession FROM read_parquet(?) ORDER BY source_file,source_row',[paths]).fetchall()
unique={}
for filename,offset,accession in catalog:
    if accession not in existing and accession not in required:unique.setdefault(accession,(filename,offset,accession))
chosen=random.Random(20251015).sample(sorted(unique.values()),25)
extras=[]
for filename in sorted({r[0] for r in chosen}):
    positions={r[1] for r in chosen if r[0]==filename};offset=0
    for batch in pq.ParquetFile(Path('data/parsed')/filename).iter_batches(batch_size=16):
        for index in sorted(x-offset for x in positions if offset<=x<offset+len(batch)):
            row=batch.slice(index,1).to_pylist()[0]
            row.update(original_source_file=filename,original_source_row=offset+index)
            extras.append(row)
        offset+=len(batch)
        if offset>max(positions):break
    print('Extracted extras from',filename,flush=True)
rows=cited+extras
assert len(rows)==52 and len({r['file_stem'] for r in rows})==52
pq.write_table(pa.Table.from_pylist(rows),out/'documents.tmp',compression='zstd');(out/'documents.tmp').replace(out/'documents.parquet')
report=dict(cited_accessions=required,extra_accessions=sorted(r['file_stem'] for r in extras),
    documents=len(rows),characters=sum(len(r['parsed_md'] or '') for r in rows),seed=20251015,
    extra_sampling_scope='25 distinct accession IDs sampled from January/February, excluding existing and cited accessions',
    seconds=time.perf_counter()-started)
manifest.write_text(json.dumps(report,indent=2));print(json.dumps(report,indent=2))
