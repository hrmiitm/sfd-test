import asyncio
import json
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from sfd_2025.benchmark.agentic import ParquetTools, answer_agent


@pytest.fixture
def corpus(tmp_path):
    path = tmp_path / 'filings.parquet'
    pq.write_table(pa.Table.from_pylist([dict(file_stem='0001', accession='CIK1', year=2025,
        month=1, char_count=30, parsed_md='Header\n| Revenue | 42 |\nTail')]), path)
    return [dict(path=str(path))]


def test_sql_scope_and_mutation_blocking(corpus, tmp_path):
    db = ParquetTools(corpus)
    try:
        assert db.query("SELECT file_stem FROM filings WHERE year=2025")['rows'] == [{'file_stem': '0001'}]
        for sql in ['DELETE FROM filings', 'SELECT 1; SELECT 2', "SET enable_external_access = true", "COPY filings TO '/tmp/agent-test.csv'", 'INSTALL httpfs']:
            with pytest.raises(ValueError): db.query(sql)
        secret = tmp_path / 'secret.txt'; secret.write_text('do not read')
        assert 'error' in db.query(f"SELECT * FROM read_text('{secret}')")
        assert 'error' in db.query("SELECT * FROM read_parquet('https://example.com/test.parquet')")
        assert db.query('SELECT 1 AS still_works')['rows'] == [{'still_works': 1}]
    finally: db.close()


def test_parquet_offsets_and_paths(corpus):
    db = ParquetTools(corpus)
    try:
        result = db.read(corpus[0]['path'], '0001', needle='revenue')
        assert result['text'][result['text'].index('|'):] == '| Revenue | 42 |\nTail'
        assert result['char_end'] == result['total_chars']
        assert db.read(corpus[0]['path'], '0001', needle='missing')['found'] is False
        with pytest.raises(ValueError): db.read('/etc/passwd', '0001')
        with pytest.raises(ValueError): db.read(corpus[0]['path'], '0001', start=-1)
    finally: db.close()


def test_sql_result_bounds(corpus):
    db = ParquetTools(corpus)
    try:
        result = db.query('SELECT range FROM range(100)')
        assert len(result['rows']) == 20 and result['truncated']
        result = db.query("SELECT repeat('a', 10000) AS text")
        assert len(result['rows'][0]['text']) == 4000 and result['truncated']
    finally: db.close()


def test_agent_tool_flow_accounting_and_resume(corpus, tmp_path):
    class Client:
        def __init__(self): self.count = 0
        async def chat(self, model, messages, tag, **kwargs):
            self.count += 1
            if self.count == 1:
                call = dict(id='call1', type='function', function=dict(name='read_parquet_filing',
                    arguments=json.dumps(dict(source_path=corpus[0]['path'], file_stem='0001'))))
                calls = [call]
                return dict(text='', message=dict(role='assistant', content=None, tool_calls=calls),
                    tool_calls=calls, usage={'cost': .01, 'prompt_tokens': 10}, seconds=1)
            assert any(m.get('tool_calls') for m in messages)
            assert any(m['role'] == 'tool' and 'Revenue' in m['content'] for m in messages)
            return dict(text='Revenue is 42 [0001].', message=dict(role='assistant', content='Revenue is 42 [0001].'),
                tool_calls=[], usage={'cost': .02, 'prompt_tokens': 20}, seconds=2)
    async def scenario():
        client = Client()
        args = (client, 'model', 'What is revenue?', 'qdrant', None, corpus, tmp_path/'state.json', 'test', 2, asyncio.Semaphore(1))
        result = await answer_agent(*args)
        assert result['usage']['cost'] == pytest.approx(.03)
        assert result['usage']['prompt_tokens'] == 30
        assert result['seconds'] == 3 and result['tool_calls'] == 1
        assert len(result['chunks']) == 1
        assert (await answer_agent(*args))['text'] == result['text']
        assert client.count == 2
    asyncio.run(scenario())


def test_resource_limits_and_timeout(corpus):
    db = ParquetTools(corpus)
    assert 'error' in db.query("SELECT repeat('x', 2000000000) AS oversized")
    result = db.query('SELECT 1 AS alive')
    assert result['rows'] == [{'alive': 1}]
    assert result['resources']['address_space_limit_mib'] == 1536
    assert result['resources']['cpu_cores'] == 1
    result = ParquetTools(corpus, seconds=.001).query('SELECT 1')
    assert result['resources']['terminated'] == 'wall_limit'


def test_streamed_company_search(tmp_path):
    path = tmp_path / 'source.parquet'
    pq.write_table(pa.Table.from_pylist([
        dict(file_stem='a', parsed_md='**COMPANY CONFORMED NAME**: Other\n**CONFORMED SUBMISSION TYPE**: 10-K'),
        dict(file_stem='b', parsed_md='**COMPANY CONFORMED NAME**: Alphabet Inc.\n**CONFORMED SUBMISSION TYPE**: 10-K'),
    ]), path)
    db = ParquetTools([dict(path=str(path))])
    result = db.search(source_path=str(path), company='alphabet', form_type='10-K')
    assert result['rows'][0]['file_stem'] == 'b'
    assert result['next_row'] == 2
    assert result['scanned'] == 2
    assert db.search(source_path=str(path), company='alphabet', start_row=2)['complete']


def test_final_answer_omits_tool_definitions(corpus, tmp_path):
    class Client:
        def __init__(self): self.count=0
        async def chat(self, model, messages, tag, **kwargs):
            self.count+=1
            if self.count==1:
                call=dict(id='tool1',type='function',function=dict(name='read_parquet_filing',
                    arguments=json.dumps(dict(source_path=corpus[0]['path'],file_stem='0001'))))
                return dict(text='',message=dict(role='assistant',content=None,tool_calls=[call]),
                    tool_calls=[call],usage={'cost':.01},seconds=1)
            assert kwargs['tools'] is None
            assert messages[-1]['role']=='user' and 'budget is exhausted' in messages[-1]['content']
            return dict(text='42',message=dict(role='assistant',content='42'),tool_calls=[],usage={'cost':.01},seconds=1)
    async def scenario():
        result=await answer_agent(Client(),'model','Revenue?','qdrant',None,corpus,tmp_path/'state.json','test',1,asyncio.Semaphore(1))
        assert result['text']=='42' and result['tool_calls']==1
    asyncio.run(scenario())
