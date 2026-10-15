"""Database adapters consume the same immutable embedding shards."""
import hashlib
import json
import logging
import math
import os
import sqlite3
import time

import lancedb
from lancedb.index import FTS, BTree, IvfFlat
import pyarrow.parquet as pq
from qdrant_client import QdrantClient, models

LOG = logging.getLogger(__name__)


def ingest_stores(args):
    config = json.loads((args.work / 'embedded.json').read_text())
    dimensions = config['dimensions']
    # Dedicated collection per corpus/config prevents accidental mixing.
    identity = (args.work / 'prepare.json').read_text() + json.dumps(config, sort_keys=True)
    name = 'sfd_' + hashlib.sha256(identity.encode()).hexdigest()[:16]
    name = getattr(args, 'target_collection', name)
    storage_work = getattr(args, 'storage_work', args.work)
    ledger = sqlite3.connect(args.work / 'ingestion.sqlite')
    ledger.execute('CREATE TABLE IF NOT EXISTS done (target TEXT, shard TEXT, PRIMARY KEY(target, shard))')
    timings = {}
    lance = qdrant = sparse = table = None
    target_q = args.qdrant_url or str((args.work / 'qdrant').resolve())
    try:
        if args.backend in ('both', 'lancedb'):
            lance = lancedb.connect(str(storage_work / 'lancedb'))
            if name in lance.list_tables().tables: table = lance.open_table(name)
            else: ledger.execute('DELETE FROM done WHERE target=?', ('lancedb',)); ledger.commit()
        if args.backend in ('both', 'qdrant'):
            qdrant = QdrantClient(url=args.qdrant_url, api_key=os.getenv('QDRANT_API_KEY'), timeout=120) if args.qdrant_url else QdrantClient(path=str(args.work / 'qdrant'))
            if not qdrant.collection_exists(name):
                qdrant.create_collection(name,
                    vectors_config={'dense': models.VectorParams(size=dimensions, distance=models.Distance.COSINE, on_disk=True)},
                    sparse_vectors_config={'bm25': models.SparseVectorParams(
                        modifier=models.Modifier.IDF, index=models.SparseIndexParams(on_disk=True))},
                    hnsw_config=models.HnswConfigDiff(on_disk=True),
                    on_disk_payload=True)
                ledger.execute('DELETE FROM done WHERE target=?', (target_q,)); ledger.commit()
            if args.qdrant_url:
                for field in ['accession', 'cik', 'form_type', 'company', 'filed_date']:
                    qdrant.create_payload_index(name, field, models.PayloadSchemaType.KEYWORD, wait=True)
            from fastembed import SparseTextEmbedding
            sparse = SparseTextEmbedding(model_name='Qdrant/bm25')
        shards = sorted((args.work / 'embeddings').glob('*.parquet'))
        # Group API checkpoints into larger DB writes to avoid a commit per
        # embedding request, without loading the corpus into memory.
        import pyarrow as pa
        for backend, target in [('lancedb', 'lancedb'), ('qdrant', target_q)]:
            if (backend == 'lancedb' and lance is None) or (backend == 'qdrant' and qdrant is None): continue
            started = time.perf_counter()
            done = {r[0] for r in ledger.execute('SELECT shard FROM done WHERE target=?', (target,))}
            pending = [s for s in shards if s.name not in done]
            for start in range(0, len(pending), 16):
                group = pending[start:start+16]
                data = pa.concat_tables([pq.read_table(s) for s in group])
                if backend == 'lancedb':
                    if table is None:
                        table = lance.create_table(name, data=data)
                        table.create_index('id', config=BTree())
                    else: table.merge_insert('id').when_matched_update_all().when_not_matched_insert_all().execute(data)
                else:
                    rows = data.to_pylist()
                    sparse_vectors = list(sparse.embed([r['text'] for r in rows]))
                    points = []
                    for row, sv in zip(rows, sparse_vectors):
                        vector = row.pop('vector')
                        points.append(models.PointStruct(id=row['id'], payload=row,
                            vector={'dense': vector, 'bm25': models.SparseVector(indices=sv.indices.tolist(), values=sv.values.tolist())}))
                    # Keep Qdrant requests below common request-size limits.
                    for offset in range(0, len(points), 64):
                        qdrant.upsert(name, points[offset:offset+64], wait=True)
                ledger.executemany('INSERT OR IGNORE INTO done VALUES (?, ?)', [(target, s.name) for s in group]); ledger.commit()
                LOG.info('Ingested %s shards (%s chunks) into %s', len(group), len(data), backend)
            timings[backend + '_write_seconds'] = time.perf_counter() - started
        expected = getattr(args, 'expected_total', None)
        if expected is None:
            expected = json.loads((args.work / 'prepared.json').read_text())['chunks']
            extensions = args.work / 'extensions.json'
            if extensions.exists():
                expected += sum(x['chunks'] for x in json.loads(extensions.read_text()).values())
        counts = {}
        if table is not None:
            started = time.perf_counter()
            counts['lancedb'] = table.count_rows()
            table.create_index('text', config=FTS(), replace=True)
            # Small samples use exact vector search; IVF needs enough training rows.
            if counts['lancedb'] >= 256:
                table.create_index('vector', config=IvfFlat(distance_type='cosine',
                                   num_partitions=max(1, int(math.sqrt(counts['lancedb'])))))
            timings['lancedb_index_seconds'] = time.perf_counter() - started
        if qdrant is not None: counts['qdrant'] = qdrant.count(name, exact=True).count
        if any(n != expected for n in counts.values()):
            raise ValueError(f'Ingestion count mismatch: expected {expected}, found {counts}')
        from .run import atomic_json
        atomic_json(args.work / f'ingested-{args.backend}.json', dict(collection=name, expected_chunks=expected, counts=counts, timings=timings,
            qdrant_target=target_q if qdrant else None, embedding=config, lancedb_path=str((storage_work / 'lancedb').resolve()),
            sparse_model='Qdrant/bm25', sparse_parameters={'k':1.2, 'b':0.75, 'avg_len':256.0}))
        LOG.info('Complete: %s %s', name, counts)
    finally:
        if qdrant: qdrant.close()
        ledger.close()
