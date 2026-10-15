#!/usr/bin/env python3
"""Estimate the financial subset using exact source sizes and stratified samples."""
import json
from pathlib import Path
import time
import pyarrow.parquet as pq
from sfd_2025.pipeline.text import chunks

base=Path('data/pipeline/financial-profile')
cache=base/'sample-measurements.json'
records=json.loads(cache.read_text()) if cache.exists() else []
known={(r['source_file'],r['source_row']) for r in records}
for file in sorted(base.glob('*-sample.parquet')):
    for row in pq.read_table(file).to_pylist():
        source,offset=row['original_source_file'],row['original_source_row']
        if (source,offset) in known:continue
        started=time.perf_counter();n=t=0
        for chunk in chunks(row,source,offset,700):
            n+=1;t+=chunk['token_count']
        from sfd_2025.pipeline.text import field
        records.append(dict(source_file=source,source_row=offset,form_type=field(row['parsed_md'],'CONFORMED SUBMISSION TYPE').strip().upper(),
            accession=row['file_stem'],characters=len(row['parsed_md']),chunks=n,tokens=t,seconds=time.perf_counter()-started))
        cache.write_text(json.dumps(records,indent=2))
        print(source,offset,n,t,flush=True)
profile_path=Path('reports/financial-subset-profile.json')
if not profile_path.exists():
    print('Samples available so far measured; run again when the corpus scan completes.')
    raise SystemExit

import duckdb
c=duckdb.connect()
paths=[str(p) for p in base.glob('*.parquet') if not p.name.endswith('-sample.parquet')]
strata=c.execute("SELECT source_file,form_type,count(*) AS filings,CAST(sum(characters) AS BIGINT) AS characters FROM read_parquet(?) WHERE form_type IN ('10-K','10-Q') GROUP BY source_file,form_type ORDER BY source_file,form_type",[paths]).to_arrow_table().to_pylist()
for group in strata:
    sample=[r for r in records if r['source_file']==group['source_file'] and r['form_type']==group['form_type']]
    if not sample:raise ValueError(f'Missing sample: {group}')
    ratio=group['characters']/sum(r['characters'] for r in sample)
    group.update(sample_filings=len(sample),estimated_tokens=round(sum(r['tokens'] for r in sample)*ratio),
        estimated_chunks=round(sum(r['chunks'] for r in sample)*ratio),
        estimated_chunking_seconds=sum(r['seconds'] for r in sample)*ratio)
benchmark=json.loads(Path('reports/ingestion-benchmark.json').read_text())
measured=benchmark['measured']
total=dict(filings=sum(r['filings'] for r in strata),characters=sum(r['characters'] for r in strata),
    tokens=sum(r['estimated_tokens'] for r in strata),chunks=sum(r['estimated_chunks'] for r in strata))
profile=json.loads(profile_path.read_text())
tr=total['tokens']/measured['tokens'];cr=total['chunks']/measured['chunks']
seconds=dict(prepare=sum(r['estimated_chunking_seconds'] for r in strata)+profile['elapsed_seconds'],
             embed=measured['timings']['embed']*tr,ingest=measured['timings']['ingest']*cr)
result=dict(selection=['10-K','10-Q'],exact_counts=profile['selected'],amendments_excluded=profile['amendments'],
    sample_filings=len(records),strata=strata,totals=total,
    estimated_embedding_cost_usd=total['tokens']*benchmark['usd_per_million_tokens']/1e6,
    estimated_stage_seconds=seconds,estimated_total_hours=sum(seconds.values())/3600,
    estimated_storage_gb=sum(measured['storage_bytes'].values())*cr/1e9,
    current_model='openai/text-embedding-3-small',dimensions=1536,
    caveats=['Counts and source characters are exact local-label measurements, not SEC-validated classifications.',
        'Tokens, chunks, cost, time and storage are estimates from four randomly sampled filings per form per month.',
        'Runtime scales from the earlier live 1,000-filing run, including recovery time; allow rate-limit and indexing variability.',
        'Cost assumes the previously observed $0.02 per million input tokens; any separate Foundry markup is unknown.',
        'Storage includes shared chunks/embeddings and both indexed databases; temporary files need additional headroom.',
        'No additional embedding requests are made by this profiler.'])
Path('reports/financial-subset-estimate.json').write_text(json.dumps(result,indent=2))
print(json.dumps({k:v for k,v in result.items() if k!='strata'},indent=2))
