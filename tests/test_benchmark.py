import asyncio
import json
import httpx
import pytest
from sfd_2025.benchmark.retrieval import rrf
from sfd_2025.benchmark.run import parse_judgment, retrieval_metrics, report
from sfd_2025.benchmark.client import ChatClient


def test_rrf_deduplicates_and_rewards_agreement():
    a, b, c = [{'id': x} for x in 'abc']
    assert [r['id'] for r in rrf([[a, b], [c, b]], 3)] == ['b', 'a', 'c']


@pytest.mark.parametrize('score', [-1, 101, True, float('nan'), '90'])
def test_invalid_judge_scores(score):
    with pytest.raises(ValueError):
        parse_judgment(json.dumps(dict(correctness=score, citation_support=100, rationale='x')))


def test_metrics_do_not_confuse_accession_with_quote():
    q = dict(filing_accessions=['a', 'b'], evidence=[dict(accession='a', exact_quote='correct quote')])
    result = retrieval_metrics(q, [dict(accession='a', text='unrelated')])
    assert result == dict(accession_recall=.5, exact_quote_recall=0)


def test_cost_unknown_and_errors_are_visible(tmp_path):
    report(tmp_path, [dict(model='m', column='b/vector', status='error')], ['m'], ['b/vector'], 2)
    summary = json.loads((tmp_path / 'summary.json').read_text())[0]
    assert summary['correctness'] is None
    assert summary['reported_chat_cost_usd'] is None
    assert summary['errors'] == 1 and summary['expected'] == 2


def test_chat_parameters_and_usage(tmp_path):
    async def scenario():
        seen = []
        def handler(request):
            body = json.loads(request.content); seen.append(body)
            return httpx.Response(200, json=dict(model=body['model'], usage={'cost': .01},
                choices=[dict(finish_reason='stop', message={'content': 'answer'})]))
        c = ChatClient('https://test/chat/completions', 'secret', 2, tmp_path / 'audit.jsonl')
        await c.client.aclose()
        c.client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        c.catalog = {'gemini': {'supported_parameters': ['temperature']}, 'gpt': {'supported_parameters': []}}
        try:
            a = await c.chat('gemini', [], 'a')
            await c.chat('gpt', [], 'b')
        finally: await c.close()
        assert a['usage']['cost'] == .01
        assert seen[0]['temperature'] == 0
        assert 'temperature' not in seen[1]
        assert seen[1]['reasoning'] == {'effort': 'low'}
        assert 'secret' not in (tmp_path / 'audit.jsonl').read_text()
    asyncio.run(scenario())


def test_summary_handles_nonagent_tool_list_and_billed_retries(tmp_path):
    import hashlib
    key=hashlib.sha256('Q1:m:qdrant/vector'.encode()).hexdigest()[:24]
    (tmp_path/'requests.jsonl').write_text('\n'.join(json.dumps(dict(tag=key+suffix,usage={'cost':cost})) for suffix,cost in [(':answer',.01),(':answer',.02),(':judge',.03)]))
    row=dict(question_id='Q1',model='m',column='qdrant/vector',status='ok',
             judgment={'correctness':100,'citation_support':100},retrieval={'accession_recall':1,'seconds':1},
             answer={'tool_calls':[],'seconds':1,'usage':{'cost':.02}},judge={'seconds':1,'usage':{'cost':.03}})
    report(tmp_path,[row],['m'],['qdrant/vector'],1)
    summary=json.loads((tmp_path/'summary.json').read_text())[0]
    assert summary['reported_chat_cost_usd']==pytest.approx(.06)
    assert summary['tool_calls']==0
