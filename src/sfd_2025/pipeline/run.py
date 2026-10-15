"""Streaming, restartable pipeline. Completed Parquet shards are checkpoints."""
import argparse
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
import fcntl
import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb
from dotenv import load_dotenv
import pyarrow as pa
import pyarrow.parquet as pq

from .text import chunks, field
from .embedding import Embedder

LOG = logging.getLogger(__name__)


def atomic_json(path, value):
    tmp = path.with_suffix('.tmp')
    tmp.write_text(json.dumps(value, indent=2))
    tmp.replace(path)


def write_rows(path, rows, dimensions=None):
    table = pa.Table.from_pylist(rows)
    if dimensions is not None:
        col = pa.array([r['vector'] for r in rows], type=pa.list_(pa.float32(), dimensions))
        table = table.set_column(table.schema.get_field_index('vector'), 'vector', col)
    tmp = path.with_suffix('.tmp')
    pq.write_table(table, tmp, compression='zstd')
    tmp.replace(path)


@contextmanager
def workspace(path):
    path.mkdir(parents=True, exist_ok=True)
    with (path / '.lock').open('w') as lock:
        try: fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError: raise ValueError('Another pipeline is using this work directory')
        yield


def prepare(args):
    files = sorted(args.input.resolve().rglob('*.parquet'))
    if not files: raise ValueError(f'No Parquet files under {args.input}')
    config = dict(version=1, files=[dict(path=str(f), size=f.stat().st_size,
                  mtime=f.stat().st_mtime_ns) for f in files],
                  limit=args.limit, chunk_tokens=args.chunk_tokens, read_batch=args.read_batch)
    if getattr(args, 'chunk_chars', None):
        config['chunk_chars'] = args.chunk_chars
    forms = sorted(set(getattr(args, 'forms', None) or []))
    if forms:
        config['forms'] = forms
    include_accessions = sorted(set(getattr(args, 'include_accessions', None) or []))
    if include_accessions:
        config['include_accessions'] = include_accessions
    manifest = args.work / 'prepare.json'
    if manifest.exists() and json.loads(manifest.read_text()) != config:
        raise ValueError('Input or preprocessing configuration changed; use a new --work directory')
    atomic_json(manifest, config)
    out = args.work / 'chunks'; out.mkdir(exist_ok=True)
    conn = duckdb.connect()
    conn.execute("SET memory_limit='1GB'")
    conn.execute('SET threads=2')
    seen = total_chunks = 0
    try:
        for fi, file in enumerate(files):
            remaining = args.limit - seen if args.limit else None
            if remaining is not None and remaining <= 0: break
            # DuckDB's ordered LIMIT can buffer large Markdown results. Feed
            # bounded Parquet batches into DuckDB instead, retaining physical
            # source row order and a bounded memory footprint at every scale.
            reader = pq.ParquetFile(file).iter_batches(batch_size=args.read_batch)
            offset = 0
            for bi, raw_batch in enumerate(reader):
                if args.limit and seen >= args.limit: break
                conn.register('source_batch', raw_batch)
                batch = conn.execute('SELECT * FROM source_batch').to_arrow_table()
                source_offset = offset
                offset += len(raw_batch)
                selected = [(j, row) for j, row in enumerate(batch.to_pylist())
                    if not forms or field(row.get('parsed_md') or '', 'CONFORMED SUBMISSION TYPE').strip().upper() in forms
                    or row['file_stem'] in include_accessions]
                if args.limit:
                    selected = selected[:args.limit-seen]
                if not selected:
                    continue
                target = out / f'{fi:02d}-{bi:08d}.parquet'
                marker = target.with_suffix('.json')
                if not marker.exists():
                    records = []
                    empty = 0
                    for j, row in selected:
                        parts = list(chunks(row, row.get('original_source_file', file.name),
                            row.get('original_source_row', source_offset+j), args.chunk_tokens,
                            getattr(args, 'chunk_chars', None)))
                        empty += not parts
                        records.extend(parts)
                    if records: write_rows(target, records)
                    atomic_json(marker, dict(filings=len(selected), chunks=len(records), empty_filings=empty))
                stats = json.loads(marker.read_text())
                total_chunks += stats['chunks']; seen += stats['filings']
                LOG.info('Preprocessed %s filings / %s chunks', seen, total_chunks)
    finally: conn.close()
    atomic_json(args.work / 'prepared.json', dict(filings=seen, chunks=total_chunks))


def embed(args):
    if not (args.work / 'prepared.json').exists():
        raise ValueError('Run prepare to completion first')
    if not json.loads((args.work / 'prepared.json').read_text())['chunks']:
        raise ValueError('No nonempty chunks to embed; inspect input and preprocessing counts')
    url = os.getenv('LLMFOUNDRY_EMBEDDINGS_URL') or os.getenv(
        'LLMFOUNDRY_BASE_URL', 'https://llmfoundry.straivedemo.com').rstrip('/') + '/openrouter/v1/embeddings'
    config = dict(model=args.model, dimensions=args.dimensions, url=url, batch=args.embed_batch)
    manifest = args.work / 'embedding.json'
    if manifest.exists() and json.loads(manifest.read_text()) != config:
        raise ValueError('Embedding configuration changed; use a new --work directory')
    atomic_json(manifest, config)
    out = args.work / 'embeddings'; out.mkdir(exist_ok=True)
    client = Embedder(url, os.getenv('LLMFOUNDRY_API_KEY') or os.getenv('LLMFOUNDRY_TOKEN'), args.model, args.dimensions)
    client.metrics_path = args.work / 'embedding-requests.jsonl'

    def process(target, rows):
        vectors = client.embed([r['text'] for r in rows])
        for row, vector in zip(rows, vectors):
            row.update(vector=vector, embedding_model=args.model)
        write_rows(target, rows, args.dimensions)
        LOG.info('Embedded %s (%s chunks)', target.name, len(rows))

    try:
        with ThreadPoolExecutor(max_workers=getattr(args, 'embed_workers', 4)) as pool:
            pending = set()
            for shard in sorted((args.work / 'chunks').glob('*.parquet')):
                for i, batch in enumerate(pq.ParquetFile(shard).iter_batches(batch_size=args.embed_batch)):
                    target = out / f'{shard.stem}-{i:06d}.parquet'
                    if target.exists(): continue
                    pending.add(pool.submit(process, target, batch.to_pylist()))
                    if len(pending) >= getattr(args, 'embed_workers', 4):
                        finished, pending = wait(pending, return_when=FIRST_COMPLETED)
                        for future in finished: future.result()
            for future in pending: future.result()
    finally:
        client.close()
    atomic_json(args.work / 'embedded.json', config)


def ingest(args):
    if not (args.work / 'embedded.json').exists():
        raise ValueError('Run embed to completion first')
    from .stores import ingest_stores
    ingest_stores(args)


def main():
    load_dotenv()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('stage', choices=['prepare', 'embed', 'ingest', 'all', 'status'])
    parser.add_argument('--input', type=Path, default=Path('data/parsed'))
    parser.add_argument('--work', type=Path, default=Path('data/pipeline/sample-1000'))
    parser.add_argument('--limit', type=int, default=1000, help='Matching filing limit; 0 means all matching filings')
    parser.add_argument('--forms', nargs='+', type=str.upper, help='Exact submission types, e.g. 10-K 10-Q; amendments must be named explicitly')
    parser.add_argument('--include-accessions', nargs='+', help='Additional filing accession IDs to include alongside --forms')
    parser.add_argument('--chunk-tokens', type=int, default=700)
    parser.add_argument('--chunk-chars', type=int, help='Optional character cap for proxy request safety')
    parser.add_argument('--read-batch', type=int, default=8)
    parser.add_argument('--embed-batch', type=int, default=128)
    parser.add_argument('--embed-workers', type=int, default=4)
    parser.add_argument('--model', default=os.getenv('EMBEDDING_MODEL', 'openai/text-embedding-3-small'))
    parser.add_argument('--dimensions', type=int, default=int(os.getenv('EMBEDDING_DIMENSIONS', '1536')))
    parser.add_argument('--backend', choices=['both', 'lancedb', 'qdrant'], default='both')
    parser.add_argument('--qdrant-url', default=os.getenv('QDRANT_URL'))
    args = parser.parse_args()
    if (args.chunk_chars is not None and args.chunk_chars < 32) or args.limit < 0 or min(args.read_batch, args.embed_batch, args.embed_workers, args.dimensions) < 1 or args.chunk_tokens < 32:
        parser.error('Limits must be nonnegative, batch sizes/dimensions positive, chunk tokens >=32')
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    try:
        with workspace(args.work):
            if args.stage == 'status':
                for f in sorted(args.work.glob('*.json')): print(f.name, f.read_text())
            else:
                for name, fn in [('prepare', prepare), ('embed', embed), ('ingest', ingest)]:
                    if args.stage in [name, 'all']:
                        started = time.perf_counter()
                        fn(args)
                        with (args.work / 'stage-timings.jsonl').open('a') as stream:
                            stream.write(json.dumps(dict(stage=name, seconds=time.perf_counter()-started,
                                timestamp=datetime.now(timezone.utc).isoformat())) + '\n')
    except (ValueError, FileNotFoundError) as exc:
        parser.exit(1, f'Error: {exc}\n')


if __name__ == '__main__': main()
