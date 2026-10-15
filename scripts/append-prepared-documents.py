#!/usr/bin/env python3
"""Append embedded source documents to both existing databases, with crash-safe checkpoints."""
import argparse
import json
import logging
from pathlib import Path
from types import SimpleNamespace
import lancedb
import pyarrow.parquet as pq
from qdrant_client import QdrantClient
from sfd_2025.pipeline.run import workspace, atomic_json
from sfd_2025.pipeline.stores import ingest_stores

p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--work',type=Path,default=Path('data/pipeline/benchmark-additions/run'))
p.add_argument('--target',type=Path,default=Path('data/pipeline/sample-1000'))
a=p.parse_args()
logging.basicConfig(level=logging.INFO)
with workspace(a.work), workspace(a.target):
    target=json.loads((a.target/'ingested-both.json').read_text())
    config=json.loads((a.work/'embedded.json').read_text())
    for key in ['model','dimensions','url']:
        if config[key]!=target['embedding'][key]:raise ValueError('Incompatible embedding '+key)
    table=lancedb.connect(str(a.target/'lancedb')).open_table(target['collection'])
    q=QdrantClient(url=target['qdrant_target'])
    try:
        if not q.collection_exists(target['collection']):raise ValueError('Destination collection missing')
        incoming=[]
        for shard in sorted((a.work/'embeddings').glob('*.parquet')):
            incoming.extend(pq.read_table(shard,columns=['id']).column('id').to_pylist())
        added=json.loads((a.work/'prepared.json').read_text())['chunks']
        assert len(incoming)==len(set(incoming))==added
        identity=dict(target=str(a.target.resolve()),collection=target['collection'],qdrant=target['qdrant_target'],chunks=added,prepare=json.loads((a.work/'prepare.json').read_text()),embedding=config)
        plan_path=a.work/'append-plan.json'
        if plan_path.exists():
            plan=json.loads(plan_path.read_text());assert plan['identity']==identity,'Append destination or inputs changed'
        else:
            original=table.search().select(['id']).limit(None).to_arrow().column('id').to_pylist()
            assert not set(original)&set(incoming),'Incoming IDs already exist'
            assert len(original)==q.count(target['collection'],exact=True).count==target['expected_chunks']
            plan=dict(identity=identity,baseline=len(original),original_ids=original)
            atomic_json(plan_path,plan)
    finally:q.close()
    ingest_stores(SimpleNamespace(work=a.work,storage_work=a.target,target_collection=target['collection'],expected_total=plan['baseline']+added,backend='both',qdrant_url=target['qdrant_target']))
    table=lancedb.connect(str(a.target/'lancedb')).open_table(target['collection'])
    assert set(plan['original_ids']).issubset(set(table.search().select(['id']).limit(None).to_arrow().column('id').to_pylist()))
    ext_path=a.target/'extensions.json'
    extensions=json.loads(ext_path.read_text()) if ext_path.exists() else {}
    extensions[str(a.work.resolve())]=dict(chunks=added,filings=json.loads((a.work/'prepared.json').read_text())['filings'])
    atomic_json(ext_path,extensions)
    result=json.loads((a.work/'ingested-both.json').read_text())
    target.update(expected_chunks=result['expected_chunks'],counts=result['counts'],extensions=extensions)
    atomic_json(a.target/'ingested-both.json',target)
    print(json.dumps(dict(collection=target['collection'],added_chunks=added,counts=result['counts']),indent=2))
