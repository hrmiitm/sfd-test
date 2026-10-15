#!/usr/bin/env python3
"""Produce a measured ingestion report and clearly labeled scale projections."""
import argparse
import json
import os
import platform
from importlib.metadata import version
from pathlib import Path
import shutil
import statistics
from datetime import datetime

import duckdb

p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--work',type=Path,default=Path('data/pipeline/sample-1000'))
p.add_argument('--larger-work',type=Path,default=Path('data/pipeline/sample-10000'))
p.add_argument('--output',type=Path,default=Path('reports/ingestion-benchmark.json'))
a=p.parse_args()
c=duckdb.connect(); c.execute('SET threads=2')


def chunk_stats(work):
    result=c.execute('SELECT count(*), sum(token_count), sum(length(text)) FROM read_parquet(?)',
                     [str(work/'chunks'/'*.parquet')]).fetchone()
    return dict(chunks=result[0],tokens=result[1],text_characters=result[2])


def timings(work):
    path=work/'stage-timings.jsonl'
    result={}
    if path.exists():
        for line in path.read_text().splitlines():
            r=json.loads(line); result[r['stage']]=result.get(r['stage'],0)+r['seconds']
    return result


def disk(path):
    return sum(p.stat().st_blocks*512 for p in path.rglob('*') if p.is_file()) if path.exists() else 0


corpus=json.loads(Path('reports/corpus-stats.json').read_text())
run=json.loads((a.work/'ingested-both.json').read_text())
requests=[json.loads(line) for line in (a.work/'embedding-requests.jsonl').read_text().splitlines()]
measured=chunk_stats(a.work)
measured.update(filings=json.loads((a.work/'prepared.json').read_text())['filings'],
               timings=timings(a.work),database_timings=run.get('timings'),
               api_requests=len(requests),api_input_tokens=sum(r['usage'].get('prompt_tokens',0) for r in requests),
               api_reported_cost_usd=sum(r['usage'].get('cost',0) for r in requests),
               retried_requests=sum(r['attempts']>1 for r in requests),
               request_latency_median_seconds=statistics.median(r['seconds'] for r in requests),
               request_latency_p95_seconds=sorted(r['seconds'] for r in requests)[int(.95*(len(requests)-1))],
               storage_bytes={name:disk(a.work/name) for name in ['chunks','embeddings','lancedb']})
measured['storage_bytes']['qdrant_server']=disk(Path('data/qdrant-server'))
# Include the initial request-limit failure and the resumed interval in observed
# wall time, rather than presenting only the final successful invocation.
end_records=[json.loads(line) for line in (a.work/'stage-timings.jsonl').read_text().splitlines()]
embed_end=max(datetime.fromisoformat(x['timestamp']).timestamp() for x in end_records if x['stage']=='embed')
embed_start=min(datetime.fromisoformat(x['timestamp']).timestamp()-x['seconds'] for x in requests)
measured['timings']['embed']=embed_end-embed_start
measured['embedding_wall_time_includes_recovery_and_pause']=True
rate=measured['api_reported_cost_usd']/measured['api_input_tokens']
source_chars=corpus['prefix_characters'][str(measured['filings'])]
scenarios=[]
for n in [1000,10000,corpus['total_filings']]:
    chars=corpus['prefix_characters'].get(str(n),corpus['total_characters'])
    ratio=chars/source_chars
    exact=n==measured['filings']
    larger_complete=n==10000 and (a.larger_work/'prepared.json').exists()
    stats=measured if exact else chunk_stats(a.larger_work) if larger_complete else {
        'tokens':round(measured['tokens']*ratio),'chunks':round(measured['chunks']*ratio)}
    tr=stats['tokens']/measured['tokens']; cr=stats['chunks']/measured['chunks']
    prep=measured['timings']['prepare']*ratio
    if larger_complete: prep=timings(a.larger_work).get('prepare',prep)
    if n==corpus['total_filings'] and (a.larger_work/'prepared.json').exists():
        bigger=chunk_stats(a.larger_work); biggerchars=corpus['prefix_characters']['10000']
        stats={'tokens':round(bigger['tokens']*chars/biggerchars),'chunks':round(bigger['chunks']*chars/biggerchars)}
        tr=stats['tokens']/measured['tokens']; cr=stats['chunks']/measured['chunks']
        prep=timings(a.larger_work).get('prepare',prep)*chars/biggerchars
    stage=dict(prepare=prep,embed=measured['timings']['embed']*tr,ingest=measured['timings']['ingest']*cr)
    total=sum(stage.values())
    scenarios.append(dict(filings=n,chunks=stats['chunks'],embedding_tokens=stats['tokens'],
        text_counts='measured' if exact or larger_complete else 'estimated from characters using 10k sample when available',
        embedding_cost_usd=measured['api_reported_cost_usd'] if exact else stats['tokens']*rate,cost_kind='API-reported' if exact else 'projected at observed API rate',
        stage_seconds=stage,total_seconds=total,
        planning_range_seconds=[total,total] if exact else [total*.75,total*(2 if n==10000 else 3)],
        storage_bytes_projected=sum(measured['storage_bytes'].values())*cr,
        raw_float32_vector_bytes_per_copy=stats['chunks']*1536*4))
report=dict(measured=measured,scenarios=scenarios,usd_per_million_tokens=rate*1e6,
    machine=dict(logical_cpus=os.cpu_count(),ram_bytes=os.sysconf('SC_PHYS_PAGES')*os.sysconf('SC_PAGE_SIZE'),
        python=platform.python_version(),packages={name:version(name) for name in ['lancedb','qdrant-client','duckdb','pyarrow']}),
    local_free_disk_bytes=shutil.disk_usage('.').free,collection=run['collection'],
    lancedb_path=str((a.work/'lancedb').resolve()),qdrant_url=run['qdrant_target'],
    caveats=['API usage cost is upstream/provider-reported; any separate Foundry billing markup is unknown.',
             'Larger runs are projections, not measured end-to-end runs; full corpus has not been embedded.',
             'Time ranges are planning allowances, not statistical confidence intervals. Rate limits and index growth can increase time.',
             'Storage scales from one observed run, includes indexes; compaction and backups need additional space.',
             'Two connection probes (15 tokens total) cost $0.00000030 in addition to the recorded corpus run.',
             'Embedding uses four concurrent workers, batches up to 128 (automatically split), 700-token chunks, 1536 dimensions. Observed wall time includes debugging/recovery pauses; future base64 transfers avoid float-JSON overhead.'])
a.output.parent.mkdir(exist_ok=True,parents=True)
a.output.write_text(json.dumps(report,indent=2))
print(json.dumps(report,indent=2))
