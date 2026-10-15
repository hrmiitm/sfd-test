#!/usr/bin/env python3
"""Count source filings and characters without decoding Markdown columns."""
import json
from pathlib import Path
import duckdb

c=duckdb.connect();c.execute('SET threads=2')
files=sorted(Path('data/parsed').glob('*.parquet'))
rows=[]
for p in files:
    n,chars=c.execute('SELECT count(*),sum(char_count) FROM read_parquet(?)',[str(p)]).fetchone()
    rows.append(dict(file=p.name,filings=n,characters=chars,compressed_bytes=p.stat().st_size))
prefix={}
for limit in [1000,10000]:
    remaining=limit; chars=0
    for p,info in zip(files,rows):
        take=min(remaining,info['filings'])
        chars+=c.execute(f'SELECT sum(char_count) FROM (SELECT char_count FROM read_parquet(?) LIMIT {take})',[str(p)]).fetchone()[0] or 0
        remaining-=take
        if not remaining: break
    prefix[str(limit)]=chars
result=dict(files=rows,total_filings=sum(r['filings'] for r in rows),
            total_characters=sum(r['characters'] for r in rows),prefix_characters=prefix)
Path('reports').mkdir(exist_ok=True)
Path('reports/corpus-stats.json').write_text(json.dumps(result,indent=2))
print(json.dumps(result,indent=2))
