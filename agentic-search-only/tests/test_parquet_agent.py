import asyncio
import json
from pathlib import Path

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from agentic_search_only.parquet_agent import ParquetTools, answer_agent, snapshot, summarize
from agentic_search_only.pinned_client import PinnedChatClient


@pytest.fixture
def tools(tmp_path):
    source = tmp_path / 'parsed'
    source.mkdir()
    pq.write_table(pa.Table.from_pylist([
        dict(file_stem='filing1', accession='123', year=2025, month=1, char_count=150,
             parsed_md='**COMPANY CONFORMED NAME**: Acme Inc\n**CONFORMED SUBMISSION TYPE**: 10-K\n| Revenue | 42 |'),
        dict(file_stem='filing2', accession='456', year=2025, month=2, char_count=10, parsed_md='Other'),
    ]), source / 'test.parquet')
    paths, stats = snapshot(source)
    return ParquetTools(paths, tmp_path / 'cache', stats)


def test_schema_sql_streaming_and_bounds(tools):
    schema = tools.execute('describe_sources', {})['output']['files'][0]
    assert schema['rows'] == 2
    assert 'parsed_md' in {c['name'] for c in schema['schema']}
    sql = "SELECT file_stem, substring(parsed_md, 1, 120) AS text FROM filings WHERE accession='123'"
    result = tools.execute('duckdb_query', {'sql': sql})
    assert 'Revenue' in result['output']['rows'][0]['text']
    assert not result['cache_hit']
    assert tools.execute('duckdb_query', {'sql': sql})['cache_hit']
    metadata = tools.execute('duckdb_query', dict(sql='SELECT count(*) AS n FROM filings',
        file_ids=None, file_stem=None))['output']
    assert metadata['rows'] == [{'n': 2}]
    blank_scope = tools.execute('duckdb_query', dict(sql='SELECT count(*) AS n FROM filings',
        file_stem=''))['output']
    assert blank_scope['rows'] == [{'n': 2}]
    scoped = tools.execute('duckdb_query', dict(sql='SELECT substring(parsed_md, 1, 120) AS text FROM filings',
        file_ids=['test.parquet'], file_stem='filing1'))['output']
    assert 'error' not in scoped, scoped
    assert 'Revenue' in scoped['rows'][0]['text'] and scoped['scoped_file_stem'] == 'filing1'
    empty = tools.execute('duckdb_query', dict(sql='SELECT file_stem FROM filings',
        file_ids=['test.parquet'], file_stem='missing'))['output']
    assert 'error' not in empty, empty
    assert empty['rows'] == []
    found = tools.execute('search_filings', dict(file_id='test.parquet', company='acme', max_rows=1))['output']
    assert found['rows'][0]['file_stem'] == 'filing1' and not found['complete']
    read = tools.execute('read_filing', dict(file_id='test.parquet', file_stem='filing1', needle='Revenue'))['output']
    assert '42' in read['text'] and read['file_id'] == 'test.parquet'
    assert read['resources']['address_space_limit_mib'] == 1536
    bounded = tools.execute('duckdb_query', {'sql': 'SELECT range FROM range(100)'})['output']
    assert len(bounded['rows']) == 20 and bounded['truncated']
    text = tools.execute('duckdb_query', {'sql': "SELECT repeat('a', 10000) AS x"})['output']
    assert len(text['rows'][0]['x']) == 6000 and text['truncated']


def test_readonly_paths_limits_and_changed_sources(tools, tmp_path):
    for sql in ['DELETE FROM filings', 'SELECT 1; SELECT 2', 'INSTALL httpfs', "COPY filings TO '/tmp/nope'",
                "SELECT * FROM read_text('/etc/passwd')", "SELECT * FROM read_parquet('https://example.com/a.parquet')"]:
        assert 'error' in tools.execute('duckdb_query', {'sql': sql})['output']
    assert 'error' in tools.execute('read_filing', dict(file_id='/etc/passwd', file_stem='x'))['output']
    assert 'error' in tools.execute('duckdb_query', {'sql': 'SELECT 1', 'file_ids': ['bad']})['output']
    tools.timeout = .001
    result = tools.execute('duckdb_query', {'sql': 'SELECT 12345'})['output']
    assert result['resources']['terminated'] == 'wall_limit'
    Path(next(iter(tools.paths.values()))).touch()
    with pytest.raises(ValueError, match='Source changed'):
        tools.execute('describe_sources', {})


def test_symlink_escape(tmp_path):
    source = tmp_path / 'parsed'; source.mkdir()
    outside = tmp_path / 'secret.parquet'; outside.touch()
    (source / 'link.parquet').symlink_to(outside)
    with pytest.raises(ValueError, match='escapes'):
        snapshot(source)


def test_raw_attempts_pins_usage_and_secret_exclusion(tmp_path):
    async def scenario():
        seen = []
        def handler(request):
            seen.append(json.loads(request.content))
            if len(seen) == 1:
                return httpx.Response(500, json={'error': 'retry me', 'usage': {'cost': .005}})
            return httpx.Response(200, json=dict(id='gen1', model='model', provider='Provider', usage={'cost': .01,
                'prompt_tokens': 10, 'completion_tokens_details': {'reasoning_tokens': 2}},
                choices=[dict(finish_reason='stop', message=dict(role='assistant', content='answer'))]))
        configs = [dict(model='model', provider='pin', temperature=0, reasoning_effort='low')]
        c = PinnedChatClient('https://test/chat/completions', 'SECRET', tmp_path, configs)
        await c.client.aclose()
        c.client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        c.catalog = {'model': {}}
        c.endpoints = {'model': dict(provider_name='Provider', supported_parameters=['temperature'])}
        try:
            response = await c.chat('model', [dict(role='user', content='question')], 'tag')
        finally:
            await c.close()
        assert response['usage']['cost'] == .01
        assert all(p['provider']['only'] == ['pin'] and not p['provider']['allow_fallbacks'] for p in seen)
        assert seen[0]['temperature'] == 0 and seen[0]['reasoning'] == {'effort': 'low'}
        audit = [json.loads(line) for line in (tmp_path / 'requests.jsonl').read_text().splitlines()]
        assert len(audit) == 2 and audit[0]['status'] == 'error'
        assert sum(r['usage']['cost'] for r in audit) == pytest.approx(.015)
        assert len(list((tmp_path / 'raw').glob('*.json'))) == 4
        assert all('SECRET' not in p.read_text() for p in tmp_path.rglob('*.json*'))
    asyncio.run(scenario())


def test_provider_mismatch_and_transport_failure_are_audited(tmp_path):
    async def scenario():
        c = PinnedChatClient('https://test/chat/completions', 'secret', tmp_path,
            [dict(model='m', provider='p', temperature=0, reasoning_effort='low')], attempts=1)
        await c.client.aclose()
        def handler(request):
            return httpx.Response(200, json=dict(model='m', provider='Wrong', usage={'cost': .1},
                choices=[dict(finish_reason='stop', message=dict(content='answer'))]))
        c.client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        c.catalog = {'m': {}}
        c.endpoints = {'m': dict(provider_name='Right', supported_parameters=[])}
        with pytest.raises(ValueError, match='Provider pin mismatch'):
            await c.chat('m', [], 'case:agent:0')
        await c.close()
        record = json.loads((tmp_path / 'requests.jsonl').read_text())
        assert record['temperature_sent'] is None and record['usage']['cost'] == .1
        c.client = httpx.AsyncClient(transport=httpx.MockTransport(lambda r: (_ for _ in ()).throw(httpx.ReadTimeout('timeout'))))
        with pytest.raises(ValueError, match='ReadTimeout'):
            await c.chat('m', [], 'case:agent:0')
        await c.close()
        assert len((tmp_path / 'requests.jsonl').read_text().splitlines()) == 2
    asyncio.run(scenario())


def test_agent_gold_isolation_finalization_and_resume(tools, tmp_path):
    class Client:
        def __init__(self): self.count = 0
        async def chat(self, model, messages, tag, **kwargs):
            self.count += 1
            assert 'GOLD_ONLY' not in json.dumps(messages)
            if self.count == 1:
                call = dict(id='t1', type='function', function=dict(name='read_filing', arguments=json.dumps(
                    dict(file_id='test.parquet', file_stem='filing1'))))
                return dict(message=dict(role='assistant', content=None, tool_calls=[call]), text='', tool_calls=[call])
            assert kwargs['tools'] is None
            return dict(message=dict(role='assistant', content='42'), text='42', tool_calls=[])
    async def scenario():
        c = Client()
        args = (c, 'm', 'Revenue?', 'sql_streaming', tools, tmp_path/'checkpoint.json', 'tag', 1, asyncio.Semaphore(1), tmp_path)
        result = await answer_agent(*args)
        assert result['text'] == '42' and result['tool_calls'] == 1
        assert '42' in result['trace'][0]['output']['text']
        assert (await answer_agent(*args))['text'] == '42' and c.count == 2
    asyncio.run(scenario())


def test_summary_includes_failed_billed_attempts_and_unknown(tmp_path):
    (tmp_path/'requests.jsonl').write_text('\n'.join(json.dumps(dict(tag='case:agent:0', status='error', usage=usage))
        for usage in [{'cost': .01}, {'cost': .02}, {}]))
    summarize(tmp_path, [dict(task_id='case', model='m', method='sql', status='error')],
        [dict(model='m', provider='p')], ['sql'], 1)
    result = json.loads((tmp_path/'summary.json').read_text())[0]
    assert result['reported_cost_usd'] == pytest.approx(.03) and result['unknown_cost_attempts'] == 1
    assert result['mean_correctness'] is None and result['errors'] == 1
