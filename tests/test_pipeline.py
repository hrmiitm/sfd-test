import json
from types import SimpleNamespace

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from sfd_2025.pipeline.text import chunks
from sfd_2025.pipeline.embedding import Embedder
from sfd_2025.pipeline.run import prepare, embed
from sfd_2025.pipeline.stores import ingest_stores


def row(text=None):
    return dict(accession='0001234567', file_stem='0001234567-25-000001', year=2025, month=1,
        parsed_md=text or '## Filing Summary\n**ACCESSION NUMBER**: 0001234567-25-000001\n\n- **CENTRAL INDEX KEY:** 0001234567\n\n**CONFORMED SUBMISSION TYPE**: 10-K\n\n# Results\nRevenue increased to $100 million.\n\n| Year | Sales |\n|---|---|\n| 2025 | 100 |',
        md5='original', has_ocr=False, source_format='html')


def test_chunks_preserve_sources():
    r = row(); parts = list(chunks(r, 'a.parquet', 0, 64))
    assert parts == list(chunks(r, 'a.parquet', 0, 64))
    assert all(p['accession'] == r['file_stem'] and p['cik'] == r['accession'] for p in parts)
    assert all(p['text'] == r['parsed_md'][p['char_start']:p['char_end']] for p in parts)
    assert any('| 2025 | 100 |' in p['text'] for p in parts)
    assert all(p['token_count'] <= 64 for p in parts)


def test_long_unicode_no_loss():
    text = '漢字🙂' * 300
    parts = list(chunks(row(text), 'a', 0, 32))
    assert ''.join(p['text'] for p in parts) == text
    assert all(p['token_count'] <= 32 for p in parts)


def test_response_validation():
    def handler(request):
        assert request.headers['authorization'] == 'Bearer test'
        return httpx.Response(200, json={'data':[{'index':1,'embedding':[0.,1.]},{'index':0,'embedding':[1.,0.]}]})
    client = Embedder('https://example.test/embeddings', 'test', 'model', 2,
                      httpx.Client(transport=httpx.MockTransport(handler)))
    assert client.embed(['a','b']) == [[1.,0.],[0.,1.]]
    with pytest.raises(ValueError): client.embed(['a'])
    client.close()


def test_pipeline_resume(tmp_path, monkeypatch):
    source=tmp_path/'input'; source.mkdir()
    pq.write_table(pa.Table.from_pylist([row(),row(''),row('   ')]), source/'a.parquet')
    work=tmp_path/'work'; work.mkdir()
    args=SimpleNamespace(input=source,work=work,limit=3,chunk_tokens=64,read_batch=1,
        model='test-only',dimensions=4,embed_batch=2,backend='both',qdrant_url=None)
    prepare(args)
    assert json.loads((work/'prepared.json').read_text())['filings'] == 3
    calls=[]
    def fake(self,texts):
        calls.append(texts)
        return [[1.,0.,0.,0.] for _ in texts]
    monkeypatch.setenv('LLMFOUNDRY_API_KEY','test')
    monkeypatch.setattr(Embedder,'embed',fake)
    embed(args); before=len(calls)
    prepare(args); embed(args)
    assert len(calls) == before
    # Exercise both real database adapters; sparse tokenization is mocked to
    # keep tests independent of model downloads/network access.
    import fastembed
    import numpy as np
    class Sparse:
        list_supported_models = fastembed.SparseTextEmbedding.list_supported_models
        def __init__(self, **kwargs): pass
        def embed(self,texts):
            return [SimpleNamespace(indices=np.array([1]),values=np.array([1.])) for t in texts]
    monkeypatch.setattr(fastembed,'SparseTextEmbedding',Sparse)
    ingest_stores(args)
    # Replay after checkpoint loss simulates a crash after DB write, before commit.
    (work/'ingestion.sqlite').unlink()
    ingest_stores(args)
    result=json.loads((work/'ingested-both.json').read_text())
    assert result['counts']['lancedb'] == result['counts']['qdrant'] == result['expected_chunks']
    args.chunk_tokens=128
    with pytest.raises(ValueError, match='configuration changed'): prepare(args)


def test_vector_index_and_keyword_search(tmp_path):
    import uuid
    import lancedb
    from sfd_2025.pipeline.run import write_rows, atomic_json
    work = tmp_path
    (work/'embeddings').mkdir()
    atomic_json(work/'prepare.json', {'test':'index'})
    atomic_json(work/'prepared.json', {'chunks':300})
    atomic_json(work/'embedded.json', {'model':'test-only','dimensions':4})
    rows=[{'id':str(uuid.uuid4()), 'text':f'Revenue earnings report {i}',
           'vector':[float(i+1),1.,float(i%7),0.]} for i in range(300)]
    write_rows(work/'embeddings'/'test.parquet', rows, 4)
    args=SimpleNamespace(work=work,backend='lancedb',qdrant_url=None)
    ingest_stores(args)
    result=json.loads((work/'ingested-lancedb.json').read_text())
    table=lancedb.connect(str(work/'lancedb')).open_table(result['collection'])
    assert len(table.list_indices()) == 3
    assert table.search('earnings',query_type='fts').limit(1).to_list()
    assert table.search([1.,1.,0.,0.]).limit(1).to_list()


def test_embedding_token_limit_splits_and_records_usage(tmp_path):
    def handler(request):
        texts=json.loads(request.content)['input']
        if len(texts)>1:
            return httpx.Response(400,json={'error':{'code':'max_tokens_per_request'}})
        return httpx.Response(200,json={'data':[{'index':0,'embedding':[1.,0.]}],
            'usage':{'prompt_tokens':1,'cost':0.00000002}})
    client=Embedder('https://example.test/embeddings','test','model',2,
        httpx.Client(transport=httpx.MockTransport(handler)))
    client.metrics_path=tmp_path/'requests.jsonl'
    assert client.embed(['a','b']) == [[1.,0.],[1.,0.]]
    records=[json.loads(line) for line in client.metrics_path.read_text().splitlines()]
    assert sum(r['usage']['prompt_tokens'] for r in records)==2
    assert len(records)==2
    client.close()


def test_base64_embedding_response():
    import base64,struct
    def handler(request):
        assert json.loads(request.content)['encoding_format']=='base64'
        encoded=base64.b64encode(struct.pack('<2f',1.,0.)).decode()
        return httpx.Response(200,json={'data':[{'index':0,'embedding':encoded}]})
    client=Embedder('https://example.test/embeddings','test','model',2,
        httpx.Client(transport=httpx.MockTransport(handler)))
    assert client.embed(['a'])==[[1.,0.]]
    client.close()


def test_form_filter_preserves_original_rows_and_limits(tmp_path):
    source=tmp_path/'input';source.mkdir()
    rows=[]
    for form in ['8-K','10-K','10-K/A','10-Q','10-K']:
        r=row();r['parsed_md']=r['parsed_md'].replace('10-K',form);rows.append(r)
    pq.write_table(pa.Table.from_pylist(rows),source/'forms.parquet')
    work=tmp_path/'work';work.mkdir()
    args=SimpleNamespace(input=source,work=work,limit=2,chunk_tokens=64,read_batch=2,forms=['10-K','10-Q'])
    prepare(args);prepare(args)
    data=[r for f in (work/'chunks').glob('*.parquet') for r in pq.read_table(f).to_pylist()]
    assert {r['source_row'] for r in data}=={1,3}
    assert {r['form_type'] for r in data}=={'10-K','10-Q'}
    assert json.loads((work/'prepared.json').read_text())['filings']==2
    args.forms=['10-K']
    with pytest.raises(ValueError,match='configuration changed'):prepare(args)


def test_form_filter_explicit_accession_exception(tmp_path):
    source=tmp_path/'input';source.mkdir()
    rows=[]
    for i,form in enumerate(['8-K','10-Q','4']):
        r=row();r['file_stem']=f'filing-{i}';r['parsed_md']=r['parsed_md'].replace('10-K',form);rows.append(r)
    pq.write_table(pa.Table.from_pylist(rows),source/'forms.parquet')
    work=tmp_path/'work';work.mkdir()
    args=SimpleNamespace(input=source,work=work,limit=0,chunk_tokens=64,read_batch=2,
        forms=['10-K','10-Q'],include_accessions=['filing-0'])
    prepare(args)
    data=[r for f in (work/'chunks').glob('*.parquet') for r in pq.read_table(f).to_pylist()]
    assert {r['source_row'] for r in data}=={0,1}
    assert {r['form_type'] for r in data}=={'8-K','10-Q'}


def test_character_cap_keeps_complete_text():
    text='word ' * 3000
    parts=list(chunks(row(text),'source.parquet',7,2000,max_chars=400))
    assert ''.join(p['text'] for p in parts)==text
    assert all(len(p['text'])<=400 and p['token_count']<=2000 for p in parts)


def test_extracted_source_provenance(tmp_path):
    source=tmp_path/'input';source.mkdir()
    r=row();r.update(original_source_file='original.parquet',original_source_row=912)
    pq.write_table(pa.Table.from_pylist([r]),source/'selected.parquet')
    work=tmp_path/'work';work.mkdir()
    args=SimpleNamespace(input=source,work=work,limit=0,chunk_tokens=64,read_batch=1)
    prepare(args)
    records=pq.read_table(next((work/'chunks').glob('*.parquet'))).to_pylist()
    assert all(x['source_file']=='original.parquet' and x['source_row']==912 for x in records)
