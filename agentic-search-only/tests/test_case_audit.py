import json
from pathlib import Path

from agentic_search_only.case_audit import build, export_case


def example(tmp_path):
    (tmp_path / 'raw').mkdir()
    (tmp_path / 'results').mkdir()
    trace = []
    tool_calls = []
    for index in range(3):
        call = dict(id=f't{index}', function=dict(name='read_filing', arguments='{"file_stem":"filing1"}'))
        tool_calls.append(call)
        output = {'text': f'evidence {index}', 'file_stem': 'filing1'} if index < 2 else {'error': 'Tool budget exhausted'}
        t = dict(tool_call=call, output=output, tag='task')
        if index < 2:
            t.update(execution_seconds=.1, cache_hit=index == 1)
        trace.append(t)
    messages = [dict(role='system', content='system'), dict(role='user', content='question')]
    messages += [dict(role='tool', tool_call_id=t['tool_call']['id'], content=json.dumps(t['output'])) for t in trace]
    calls = [dict(call_id='a', tag='task:agent:0', status='ok', usage={'cost': .01}, tool_calls=tool_calls,
                  request_file='raw/a.request.json', response_file='raw/a.response.json')]
    calls += [dict(call_id='b', tag='task:agent:1', status='ok', usage={'cost': .02}, tool_calls=[],
                   request_file='raw/b.request.json', response_file='raw/b.response.json')]
    judge = dict(call_id='j', tag='task:judge', status='ok', usage={'cost': .005}, requested_model='haiku',
                 actual_provider='Anthropic', text='judge json', request_file='raw/j.request.json', response_file='raw/j.response.json')
    for call in calls + [judge]:
        body = dict(messages=messages, model='model', temperature=0, reasoning={'effort':'low'})
        if call == judge:
            body['messages'] = [dict(role='user', content=json.dumps(dict(candidate='42', reference='GOLD', tool_evidence=trace)))]
        (tmp_path / call['request_file']).write_text(json.dumps(dict(body=body)))
        (tmp_path / call['response_file']).write_text(json.dumps(dict(body=dict(choices=[dict(message=dict(tool_calls=call.get('tool_calls', [])))]))))
    row = dict(task_id='task', question_id='Q1', model='model', provider='p', method='sql_streaming',
        question='question', reference='GOLD', status='ok', elapsed_seconds=1,
        answer=dict(text='42', trace=trace, calls=calls, tool_calls=2), judge=judge,
        judgment=dict(correctness=99, citation_support=90, rationale='because'))
    return row, calls + [judge]


def test_exact_context_counts_scores_cost_and_immutable_export(tmp_path):
    row, calls = example(tmp_path)
    # A failed but charged HTTP attempt is included in billing, not model rounds.
    calls.append(dict(tag='task:agent:0', status='error', usage={'cost': .004}))
    case = export_case(tmp_path, row, calls)
    assert case['tool_counts'] == dict(requested_by_model=3, processed=3, budgeted_attempts=2,
        dispatched=2, cache_hits=1, source_worker_runs=1, errors=1, budget_rejected=1,
        processed_by_tool={'read_filing':3})
    assert case['model_rounds'] == 2 and case['api_attempt_count'] == 4
    assert case['evidence_accounting']['output_ids_missing_from_final_request'] == []
    assert [json.loads(m['content']) for m in case['final_answer_request']['exact_tool_messages']] == [t['output'] for t in row['answer']['trace']]
    assert 'GOLD' not in json.dumps(case['final_answer_request']['body'])
    assert case['judge']['parsed_input']['reference'] == 'GOLD'
    assert case['final_answer'] == '42' and case['judge']['correctness'] == 99
    assert abs(case['billing']['total']['reported_cost_usd'] - .039) < 1e-8
    result = tmp_path / 'results/task.json'; result.write_text(json.dumps(row))
    audit = tmp_path / 'requests.jsonl'; audit.write_text('\n'.join(json.dumps(c) for c in calls))
    original = result.read_bytes(), audit.read_bytes()
    index = build(tmp_path)
    assert len(index) == 1 and index[0]['tool_calls_requested'] == 3
    assert json.loads((tmp_path/'cases/task.json').read_text())['final_answer'] == '42'
    assert original == (result.read_bytes(), audit.read_bytes())


def test_missing_raw_and_partial_failure_remain_explicit(tmp_path):
    row, calls = example(tmp_path)
    journal = row['answer']['trace']
    row.pop('answer'); row.update(status='error', error='network failure')
    case = export_case(tmp_path, row, calls, journal)
    assert case['trace_source'] == 'partial durable journals' and len(case['retrieval_steps']) == 3
    assert case['tool_counts']['requested_by_model'] == 3
    assert case['final_answer'] is None and not case['final_answer_request']['is_final_answer_request']
    (tmp_path/'raw/b.request.json').unlink()
    case = export_case(tmp_path, row, calls, journal)
    assert case['missing_raw_requests'] == ['raw/b.request.json']
    assert case['final_answer_request']['body'] is None
