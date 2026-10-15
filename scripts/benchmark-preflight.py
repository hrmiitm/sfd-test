#!/usr/bin/env python3
"""Select a reproducible random sample and validate stores/models before a paid run."""
import argparse
import asyncio
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import random
import time

from dotenv import load_dotenv
import pyarrow.parquet as pq
from sfd_2025.benchmark.client import ChatClient
from sfd_2025.benchmark.retrieval import Retriever
from sfd_2025.benchmark.run import save, parse_judgment, JUDGE_SYSTEM
from sfd_2025.pipeline.embedding import Embedder


async def main(args):
    load_dotenv()
    out = args.output
    out.mkdir(parents=True, exist_ok=True)
    raw = Path('questions/questions.jsonl').read_bytes()
    questions = [json.loads(line) for line in raw.splitlines() if line]
    assert len(questions) == 100 and len({q['id'] for q in questions}) == 100
    selected = sorted(random.Random(args.seed).sample(questions, 20), key=lambda q:q['id'])
    selection = dict(seed=args.seed, algorithm='Python random.Random(seed).sample without replacement; sort by ID',
        population=100, sample_size=20, source_sha256=hashlib.sha256(raw).hexdigest(),
        ids=[q['id'] for q in selected], difficulty_counts=dict(Counter(q['difficulty'] for q in selected)))
    if (out/'selection.json').exists() and json.loads((out/'selection.json').read_text()) != selection:
        raise ValueError('Existing sample differs; use a new output directory')
    save(out/'selection.json', selection)
    (out/'questions.jsonl').write_text(''.join(json.dumps(q)+'\n' for q in selected))
    print('Selected:', ', '.join(selection['ids']), flush=True)
    print('Difficulty:', selection['difficulty_counts'], flush=True)
    work = Path('data/pipeline/sample-1000')
    ingestion = json.loads((work/'ingested-both.json').read_text())
    extensions = json.loads((work/'extensions.json').read_text()) if (work/'extensions.json').exists() else {}
    expected = json.loads((work/'prepared.json').read_text())['chunks'] + sum(v['chunks'] for v in extensions.values())
    assert expected == ingestion['expected_chunks'], 'Ingestion manifest is not synchronized with extensions'
    embed = json.loads((work/'embedding.json').read_text())
    retriever = Retriever(work, ingestion['collection'], ['lancedb','qdrant'], os.getenv('QDRANT_URL'))
    result = dict(started_utc=datetime.now(timezone.utc).isoformat(), expected_chunks=expected,
                  collection=ingestion['collection'], extensions=extensions)
    key=os.getenv('LLMFOUNDRY_API_KEY') or os.getenv('LLMFOUNDRY_TOKEN')
    models=['google/gemini-3.8-flash','openai/gpt-6-luna','anthropic/claude-haiku-5.5']
    base=os.getenv('LLMFOUNDRY_BASE_URL','https://llmfoundry.straivedemo.com').rstrip('/')
    client=ChatClient(os.getenv('LLMFOUNDRY_CHAT_URL',base+'/openrouter/v1/chat/completions'),key,3,out/'preflight-requests.jsonl')
    try:
        result['stores_before']=retriever.state()
        assert all(s['count']==expected for s in result['stores_before'].values()), 'Store counts disagree with ingestion manifests'
        info=retriever.qdrant.get_collection(ingestion['collection'])
        result['qdrant_status']=str(info.status)
        assert str(info.status)=='green', 'Qdrant indexing not ready'
        result['indexes']=[dict(name=i.name,type=i.index_type,columns=i.columns) for i in retriever.table.list_indices()]
        assert {'text','vector'} <= {c for i in result['indexes'] for c in i['columns']}
        result['coverage']=retriever.coverage(a for q in selected for a in q['filing_accessions'])
        result['questions_fully_covered']={b:sum(all(result['coverage'][a][b]>0 for a in q['filing_accessions']) for q in selected) for b in ['lancedb','qdrant']}
        sample=next(pq.ParquetFile(next(iter(sorted((work/'embeddings').glob('*.parquet'))))).iter_batches(batch_size=1)).to_pylist()[0]
        result['self_match']={}
        for b in ['lancedb','qdrant']:
            hits=retriever.search(b,'vector','',sample['vector'],3)
            assert sample['id'] in {r['id'] for r in hits}, f'{b} dense self-match failed'
            result['self_match'][b]=True
        emb=Embedder(embed['url'],key,embed['model'],embed['dimensions'])
        emb.metrics_path=out/'preflight-embeddings.jsonl'
        try: vector=(await asyncio.to_thread(emb.embed,[selected[0]['question']]))[0]
        finally: emb.close()
        result['retrieval_checks']={}
        for b in ['lancedb','qdrant']:
            for method in ['bm25','vector','hybrid']:
                started=time.monotonic()
                hits=retriever.search(b,method,selected[0]['question'],vector,3)
                assert hits and all(r.get('text') and r.get('id') for r in hits)
                result['retrieval_checks'][f'{b}/{method}']=dict(hits=len(hits),seconds=time.monotonic()-started)
        await client.preflight(models)
        save(out/'preflight-models.json',{m:client.catalog[m] for m in models})
        replies=await asyncio.gather(*(client.chat(m,[dict(role='user',content='Reply with only READY.')],f'health:{m}',max_tokens=1000) for m in models))
        assert all('READY' in r['text'].upper() for r in replies), 'Model liveness response unexpected'
        judge=await client.chat(models[2],[dict(role='system',content=JUDGE_SYSTEM),dict(role='user',content=json.dumps(dict(question='What is 2 + 2?', reference='4', candidate='4', retrieved_excerpts=[])))],'health:judge-json',max_tokens=1500)
        result['judge_schema']=parse_judgment(judge['text'])
        result['model_checks']=[dict(requested=r['requested_model'],actual=r['actual_model'],text=r['text'],seconds=r['seconds']) for r in replies]
        result['stores_after']=retriever.state()
        assert result['stores_before']==result['stores_after'], 'Corpus changed during preflight'
        result['passed']=True
        print('Preflight PASSED:', result['stores_after'], 'coverage:',result['questions_fully_covered'],flush=True)
    except Exception as error:
        result.update(passed=False,error=str(error).replace(key or '', '[REDACTED]') if key else str(error))
        raise
    finally:
        result['finished_utc']=datetime.now(timezone.utc).isoformat()
        save(out/'preflight.json',result)
        retriever.close()
        await client.close()


p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--output',type=Path,required=True)
p.add_argument('--seed',type=int,default=20261015)
asyncio.run(main(p.parse_args()))
