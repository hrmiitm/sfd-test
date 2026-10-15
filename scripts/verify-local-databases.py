#!/usr/bin/env python3
"""Storage/index readiness checks; does not call an LLM or an embedding API."""
import argparse
import json
import time
from pathlib import Path

import lancedb
import pyarrow.parquet as pq
from fastembed import SparseTextEmbedding
from qdrant_client import QdrantClient, models

p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--work',type=Path,default=Path('data/pipeline/sample-1000'))
a=p.parse_args()
r=json.loads((a.work/'ingested-both.json').read_text())
table=lancedb.connect(str(a.work/'lancedb')).open_table(r['collection'])
client=QdrantClient(url=r['qdrant_target'])
try:
    expected=r['expected_chunks']
    assert table.count_rows()==client.count(r['collection'],exact=True).count==expected
    sample=next(pq.ParquetFile(next(iter(sorted((a.work/'embeddings').glob('*.parquet'))))).iter_batches(batch_size=1)).to_pylist()[0]
    lance_dense=table.search(sample['vector']).distance_type('cosine').limit(1).to_list()
    q_dense=client.query_points(r['collection'],query=sample['vector'],using='dense',limit=1).points
    assert lance_dense and lance_dense[0]['_distance']<0.001
    assert q_dense and q_dense[0].score>0.999
    lance_text=table.search('company',query_type='fts').limit(1).to_list()
    model=SparseTextEmbedding('Qdrant/bm25')
    query=next(iter(model.query_embed('company')))
    q_text=client.query_points(r['collection'],using='bm25',
        query=models.SparseVector(indices=query.indices.tolist(),values=query.values.tolist()),limit=1).points
    assert lance_text and q_text
    indices=[dict(name=x.name,index_type=x.index_type,columns=x.columns) for x in table.list_indices()]
    assert {'text','vector'}.issubset({c for x in indices for c in x['columns']})
    deadline=time.monotonic()+300
    info=client.get_collection(r['collection'])
    while str(info.status) != 'green' and time.monotonic()<deadline:
        time.sleep(2)
        info=client.get_collection(r['collection'])
    assert str(info.status)=='green', f'Qdrant indexing not ready: {info.status}'
    result=dict(expected_chunks=expected,lancedb_count=table.count_rows(),
        qdrant_count=info.points_count,lancedb_indices=indices,
        qdrant_status=str(info.status),qdrant_indexed_vectors=info.indexed_vectors_count,
        lance_self_match_distance=lance_dense[0]['_distance'],qdrant_self_match_score=q_dense[0].score,
        lance_keyword_match=True,qdrant_bm25_match=True,
        sample_citation={k:sample[k] for k in ['id','accession','cik','section','source_file','source_row','char_start','char_end']})
    (a.work/'verified.json').write_text(json.dumps(result,indent=2))
    print(json.dumps(result,indent=2))
finally: client.close()
