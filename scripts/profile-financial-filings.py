#!/usr/bin/env python3
"""Streaming form inventory and deterministic sample for subset sizing."""
import collections
import json
from pathlib import Path
import random
import time
import pyarrow as pa
import pyarrow.parquet as pq
from sfd_2025.pipeline.text import field

out=Path('data/pipeline/financial-profile');out.mkdir(parents=True,exist_ok=True)
started=time.perf_counter()
for file in sorted(Path('data/parsed').glob('*.parquet')):
    target=out/f'{file.stem}.parquet'
    if target.exists():continue
    rows=[]; samples=collections.defaultdict(list); seen=collections.Counter()
    rng=random.Random(file.name)
    offset=0
    for batch in pq.ParquetFile(file).iter_batches(batch_size=16):
        for row in batch.to_pylist():
            text=row.get('parsed_md') or ''
            form=field(text,'CONFORMED SUBMISSION TYPE').strip().upper()
            rows.append(dict(source_file=file.name,source_row=offset,
                form_type=form,characters=len(text),accession=row['file_stem']))
            if form in ('10-K','10-Q'):
                seen[form]+=1
                candidate=dict(row,original_source_file=file.name,original_source_row=offset)
                if len(samples[form])<4:samples[form].append(candidate)
                else:
                    pos=rng.randrange(seen[form])
                    if pos<4:samples[form][pos]=candidate
            offset+=1
    pq.write_table(pa.Table.from_pylist(rows),target,compression='zstd')
    selected=[r for group in samples.values() for r in group]
    if selected:pq.write_table(pa.Table.from_pylist(selected),out/f'{file.stem}-sample.parquet',compression='zstd')
    print(file.name,offset,'filings',dict(seen),f'{time.perf_counter()-started:.1f}s',flush=True)

import duckdb
c=duckdb.connect()
paths=[str(p) for p in sorted(out.glob('*.parquet')) if not p.name.endswith('-sample.parquet')]
data=c.execute('SELECT form_type,count(*) AS filings,CAST(sum(characters) AS BIGINT) AS characters FROM read_parquet(?) GROUP BY form_type ORDER BY filings DESC',[paths]).to_arrow_table().to_pylist()
timing=out/'scan-timing.json'
if not timing.exists():timing.write_text(json.dumps({'seconds':time.perf_counter()-started}))
report=dict(forms=data,selected=[r for r in data if r['form_type'] in ('10-K','10-Q')],
            amendments=[r for r in data if r['form_type'] in ('10-K/A','10-Q/A')],
            elapsed_seconds=json.loads(timing.read_text())['seconds'])
Path('reports/financial-subset-profile.json').write_text(json.dumps(report,indent=2))
print(json.dumps({k:v for k,v in report.items() if k!='forms'},indent=2))
