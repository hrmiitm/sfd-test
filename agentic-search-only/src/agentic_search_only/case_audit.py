"""Export/filter a self-contained investigation record for each question/model/method.

This is offline postprocessing: no model requests, source scans or original-result
rewrites. All paths in exported records are relative to the run directory.
"""
import argparse
from collections import Counter
import csv
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path

from .pinned_client import save


def load_lines(path):
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()] if path.exists() else []


def request_body(output, call, missing):
    relative = call.get('request_file') if call else None
    if not relative:
        missing.append('request_file not recorded')
        return None
    path = (output / relative).resolve()
    if not path.is_relative_to(output.resolve()) or not path.is_file():
        missing.append(relative)
        return None
    return json.loads(path.read_text()).get('body')


def tool_messages(body):
    return [dict(tool_call_id=m.get('tool_call_id'), content=m.get('content'))
            for m in (body or {}).get('messages', []) if m.get('role') == 'tool']


def cost_summary(calls):
    known = [c['usage']['cost'] for c in calls if isinstance(c.get('usage', {}).get('cost'), (int, float))]
    return dict(reported_cost_usd=sum(known) if known else None,
                unknown_cost_attempts=len(calls) - len(known), attempts=len(calls))


def export_case(output, row, attempts, journal=None):
    answer = row.get('answer', {})
    traces = answer.get('trace', journal or [])
    calls = answer.get('calls', [])
    reconstructed = False
    if not calls:
        # Failed cases may have a durable tool/API journal but no completed answer.
        reconstructed = True
        for attempt in attempts:
            if ':agent:' not in attempt['tag'] or attempt.get('status') != 'ok':
                continue
            relative = attempt.get('response_file')
            path = (output / relative).resolve() if relative else None
            if path and path.is_relative_to(output.resolve()) and path.is_file():
                body = json.loads(path.read_text()).get('body', {})
                choices = body.get('choices') or [{}]
                message = choices[0].get('message') or {}
                calls.append(dict(attempt, tool_calls=message.get('tool_calls') or []))
    requested = [t for call in calls for t in call.get('tool_calls', [])]
    by_name = Counter(t.get('tool_call', {}).get('function', {}).get('name', 'unknown') for t in traces)
    missing = []
    final_call = calls[-1] if calls and answer.get('text') else next(
        (c for c in reversed(attempts) if ':agent:' in c['tag']), None)
    final_body = request_body(output, final_call, missing) if final_call else None
    judge = row.get('judge')
    judge_body = request_body(output, judge, missing) if judge else None
    judge_input = None
    if judge_body:
        users = [m for m in judge_body.get('messages', []) if m.get('role') == 'user']
        if users:
            try:
                judge_input = json.loads(users[-1]['content'])
            except (ValueError, TypeError):
                judge_input = users[-1]['content']
    # Successful model turns and HTTP attempts differ when requests are retried.
    counts = dict(requested_by_model=len(requested), processed=len(traces),
        budgeted_attempts=answer.get('tool_calls', sum(t.get('output', {}).get('error') != 'Tool budget exhausted' for t in traces)),
        dispatched=sum('execution_seconds' in t for t in traces),
        cache_hits=sum(t.get('cache_hit') is True for t in traces),
        source_worker_runs=sum('execution_seconds' in t and not t.get('cache_hit', False) for t in traces),
        errors=sum('error' in t.get('output', {}) for t in traces),
        budget_rejected=sum(t.get('output', {}).get('error') == 'Tool budget exhausted' for t in traces),
        processed_by_tool=dict(sorted(by_name.items())))
    retrieved = [dict(step=i + 1, tool_call=t['tool_call'], output=t['output'])
                 for i, t in enumerate(traces)]
    final_evidence = tool_messages(final_body)
    sent_ids = {t['tool_call_id'] for t in final_evidence}
    retrieved_ids = {t.get('tool_call', {}).get('id') for t in traces}
    answer_attempts = [c for c in attempts if ':agent:' in c['tag']]
    judge_attempts = [c for c in attempts if ':judge' in c['tag']]
    return dict(schema_version=1, task_id=row['task_id'], question_id=row['question_id'],
        model=row['model'], provider=row.get('provider'), method=row['method'],
        question=row['question'], reference=row.get('reference'), status=row.get('status'), error=row.get('error'),
        paths_relative_to='run directory', original_result_file=f'results/{row["task_id"]}.json',
        tool_counts=counts, model_rounds=len(calls), api_attempt_count=len(attempts),
        retrieval_steps=retrieved, tool_trace=traces,
        trace_source='partial durable journals' if reconstructed else 'completed answer checkpoint',
        final_answer=row.get('answer', {}).get('text'),
        final_answer_request=dict(request_file=final_call.get('request_file') if final_call else None,
            response_file=final_call.get('response_file') if final_call else None,
            is_final_answer_request=bool(answer.get('text')),
            note='Final answer-generation context' if answer.get('text') else 'Latest available context; no completed answer recorded',
            body=final_body, exact_tool_messages=final_evidence),
        evidence_accounting=dict(tool_outputs_produced=len(traces),
            tool_outputs_sent_in_final_request=len(final_evidence),
            output_ids_missing_from_final_request=sorted(retrieved_ids - sent_ids, key=str),
            note='Final-request tool messages are the exact content strings sent. Full messages include earlier planning/tool calls. No separate evidence-selection step is inferred.'),
        judge=dict(model=judge.get('requested_model') if judge else None,
            provider=judge.get('actual_provider') if judge else None,
            correctness=row.get('judgment', {}).get('correctness'),
            citation_support=row.get('judgment', {}).get('citation_support'),
            rationale=row.get('judgment', {}).get('rationale'),
            raw_text=judge.get('text') if judge else None,
            request_file=judge.get('request_file') if judge else None,
            response_file=judge.get('response_file') if judge else None,
            request_body=judge_body, parsed_input=judge_input),
        billing=dict(total=cost_summary(attempts), answer=cost_summary(answer_attempts), judge=cost_summary(judge_attempts)),
        timing=dict(case_wall_seconds=row.get('elapsed_seconds'),
            api_attempt_seconds=sum(c.get('seconds', 0) for c in attempts),
            tool_execution_seconds=sum(t.get('execution_seconds', 0) for t in traces),
            tool_queue_seconds=sum(t.get('queue_seconds', 0) for t in traces)),
        token_usage={key: sum(c.get('usage', {}).get(key, 0) or 0 for c in attempts)
                     for key in ('prompt_tokens', 'completion_tokens', 'total_tokens')},
        api_attempts=attempts, missing_raw_requests=missing)


def build(output):
    output = Path(output)
    cases = output / 'cases'
    cases.mkdir(exist_ok=True)
    attempts = load_lines(output / 'requests.jsonl')
    grouped = {}
    for call in attempts:
        grouped.setdefault(call['tag'].split(':')[0], []).append(call)
    tools = {}
    for trace in load_lines(output / 'tools.jsonl'):
        tools.setdefault(trace['tag'], []).append(trace)
    index = []
    source_hashes = {}
    for path in sorted((output / 'results').glob('*.json')):
        row = json.loads(path.read_text())
        source_hashes[str(path.relative_to(output))] = hashlib.sha256(path.read_bytes()).hexdigest()
        case = export_case(output, row, grouped.get(row['task_id'], []), tools.get(row['task_id'], []))
        filename = f'cases/{row["task_id"]}.json'
        save(output / filename, case)
        index.append(dict(question_id=case['question_id'], model=case['model'], provider=case['provider'],
            method=case['method'], status=case['status'], task_id=case['task_id'],
            tool_calls_requested=case['tool_counts']['requested_by_model'],
            tool_calls_processed=case['tool_counts']['processed'], tool_calls_dispatched=case['tool_counts']['dispatched'],
            cache_hits=case['tool_counts']['cache_hits'], tool_errors=case['tool_counts']['errors'],
            model_rounds=case['model_rounds'], api_attempts=case['api_attempt_count'],
            correctness=case['judge']['correctness'], citation_support=case['judge']['citation_support'],
            reported_cost_usd=case['billing']['total']['reported_cost_usd'],
            unknown_cost_attempts=case['billing']['total']['unknown_cost_attempts'],
            case_wall_seconds=case['timing']['case_wall_seconds'],
            final_answer=case['final_answer'], judge_rationale=case['judge']['rationale'],
            case_file=filename, original_result_file=case['original_result_file']))
    index.sort(key=lambda r: (r['question_id'], r['model'], r['method']))
    save(output / 'cases-index.json', index)
    if index:
        with (output / 'cases-index.csv').open('w') as stream:
            writer = csv.DictWriter(stream, fieldnames=list(index[0]), lineterminator='\n')
            writer.writeheader()
            writer.writerows(index)
    save(output / 'case-export-provenance.json', dict(exported_utc=datetime.now(timezone.utc).isoformat(),
        exporter_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        original_result_sha256=source_hashes,
        note='Offline derived audit. Original benchmark results, request logs, manifest and original code provenance are unchanged. No model calls were made.'))
    return index


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('output', type=Path)
    parser.add_argument('--question')
    parser.add_argument('--model')
    parser.add_argument('--method')
    parser.add_argument('--full', action='store_true', help='Print complete matching cases including exact sent context')
    args = parser.parse_args()
    rows = build(args.output)
    rows = [r for r in rows if (not args.question or r['question_id'] == args.question)
            and (not args.model or r['model'] == args.model) and (not args.method or r['method'] == args.method)]
    if args.full:
        rows = [json.loads((args.output / r['case_file']).read_text()) for r in rows]
    print(json.dumps(rows, indent=2, ensure_ascii=False))


if __name__ == '__main__':
    main()
