#!/usr/bin/env python3
"""Extract complete benchmark source documents using the existing row inventory."""
import hashlib
import json
from pathlib import Path
import time
import collections
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

started=time.perf_counter()
base=Path('data/pipeline/benchmark-fast');source=base/'sources';source.mkdir(parents=True,exist_ok=True)
questions=json.loads(Path('questions/questions.json').read_text())
ids=sorted({a for q in questions for a in q.get('filing_accessions',[])} |
           {e['accession'] for q in questions for e in q.get('evidence',[]) if e.get('accession')})
files=sorted(Path('data/parsed').glob('*.parquet'))
config=dict(accessions=ids,files=[dict(path=str(p.resolve()),size=p.stat().st_size,mtime=p.stat().st_mtime_ns) for p in files])
manifest=base/'source-manifest.json'
if manifest.exists():
    old=json.loads(manifest.read_text())
    if old['config']!=config:raise ValueError('Benchmark/input changed; use a fresh benchmark-fast workspace')
    if (source/'documents.parquet').exists():
        print('Reusing extracted complete benchmark sources',flush=True)
        raise SystemExit
catalog=[str(p) for p in Path('data/pipeline/financial-profile').glob('*.parquet') if not p.name.endswith('-sample.parquet')]
c=duckdb.connect()
locations=c.execute('SELECT source_file,source_row,accession FROM read_parquet(?) WHERE accession IN (SELECT unnest(?))',[catalog,ids]).fetchall()
found={r[2] for r in locations}
if found!=set(ids):raise ValueError(f'Missing benchmark accessions: {set(ids)-found}')
selected=collections.defaultdict(set)
for filename,offset,accession in locations:selected[filename].add(offset)
unique={}; aliases=collections.defaultdict(list)
for filename,offsets in sorted(selected.items()):
    offset=0
    for batch in pq.ParquetFile(Path('data/parsed')/filename).iter_batches(batch_size=16):
        for index in sorted(p-offset for p in offsets if offset<=p<offset+len(batch)):
            row=batch.slice(index,1).to_pylist()[0]
            sha=hashlib.sha256((row['parsed_md'] or '').encode()).hexdigest()
            key=(row['file_stem'],sha)
            aliases[key].append(dict(source_file=filename,source_row=offset+index,dataset_identifier=row['accession']))
            if key not in unique:
                row.update(original_source_file=filename,original_source_row=offset+index)
                unique[key]=row
        offset+=len(batch)
        if offset>max(offsets):break
    print(filename,'extracted',len(offsets),'source rows',f'{time.perf_counter()-started:.1f}s',flush=True)
rows=list(unique.values())
tmp=source/'documents.tmp';pq.write_table(pa.Table.from_pylist(rows),tmp,compression='zstd');tmp.replace(source/'documents.parquet')
report=dict(config=config,accessions=len(ids),source_rows=len(locations),distinct_documents=len(rows),
            source_characters=sum(len(r['parsed_md'] or '') for r in rows),seconds=time.perf_counter()-started,
            documents=[dict(accession=k[0],sha256=k[1],sources=v) for k,v in aliases.items()])
manifest.write_text(json.dumps(report,indent=2));print(json.dumps({k:v for k,v in report.items() if k not in ('config','documents')},indent=2))
