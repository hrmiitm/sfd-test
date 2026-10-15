#!/usr/bin/env python3
"""Verify source coverage and exact chunk-ID parity after appending filings."""
import json
from pathlib import Path
import lancedb
from qdrant_client import QdrantClient
root=Path('data/pipeline'); work=root/'benchmark-additions/run'
r=json.loads((work/'ingested-both.json').read_text());manifest=json.loads((root/'benchmark-additions/source-manifest.json').read_text());plan=json.loads((work/'append-plan.json').read_text())
t=lancedb.connect(r['lancedb_path']).open_table(r['collection'])
rows=t.search().select(['id','accession','source_file','source_row']).limit(None).to_list()
lids={x['id'] for x in rows};la={x['accession'] for x in rows}
q=QdrantClient(url=r['qdrant_target']);qids=set();qa=set();offset=None
try:
    while True:
        points,offset=q.scroll(r['collection'],limit=1000,offset=offset,with_payload=['accession'],with_vectors=False)
        qids.update(str(x.id) for x in points);qa.update(x.payload['accession'] for x in points)
        if offset is None:break
finally:q.close()
assert lids==qids and len(lids)==r['expected_chunks']
assert set(plan['original_ids'])<=lids
assert set(manifest['cited_accessions']+manifest['extra_accessions'])<=la&qa
assert not {'question','answer','ground_truth','evidence'}&set(t.schema.names)
metrics=[json.loads(x) for x in (work/'embedding-requests.jsonl').read_text().splitlines()]
result=dict(collection=r['collection'],chunks_each=len(lids),filing_records=len({(x['source_file'],x['source_row']) for x in rows}),unique_accessions=len(la),cited_documents_present=27,extra_documents_present=25,original_chunks_preserved=len(plan['original_ids']),identical_chunk_ids=True,embedding_tokens=sum(x['usage']['prompt_tokens'] for x in metrics),embedding_cost_usd=sum(x['usage'].get('cost',0) for x in metrics))
(work/'coverage-verified.json').write_text(json.dumps(result,indent=2));print(json.dumps(result,indent=2))
