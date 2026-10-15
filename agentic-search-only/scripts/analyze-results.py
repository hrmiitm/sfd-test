#!/usr/bin/env python3
"""Read-only audit and dashboard export. Never retries, judges, or edits a saved run."""
import argparse
from collections import Counter, defaultdict
import csv
from datetime import datetime, timezone
import hashlib
import itertools
import json
import math
from pathlib import Path
import re
import statistics
import sys

from agentic_search_only.case_audit import export_case

PROJECT = Path(__file__).resolve().parents[1]


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + '.tmp')
    with temp.open('w') as stream:
        json.dump(value, stream, indent=2, ensure_ascii=False, allow_nan=False)
        stream.write('\n')
    temp.replace(path)


def save_text(path, text):
    temp=path.with_suffix(path.suffix+'.tmp')
    temp.write_text(text)
    temp.replace(path)


def lines(path):
    if path.exists():
        with path.open() as stream:
            for line in stream:
                if line.strip():
                    yield json.loads(line)


def sha(path):
    h = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def snapshot(root):
    return {str(p.relative_to(root)): dict(bytes=p.stat().st_size, sha256=sha(p))
            for p in sorted(root.rglob('*')) if p.is_file()}


def known_cost(calls):
    values = [c.get('usage', {}).get('cost') for c in calls]
    good = [v for v in values if isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v)]
    return dict(known_usd=sum(good) if good else None, unknown_attempts=len(values)-len(good))


def avg(values):
    values = [v for v in values if isinstance(v, (int, float)) and not isinstance(v, bool)]
    return statistics.mean(values) if values else None


def percentile(values, fraction):
    values = sorted(v for v in values if isinstance(v, (int, float)))
    if not values:
        return None
    index = (len(values)-1)*fraction
    lo, hi = math.floor(index), math.ceil(index)
    return values[lo] + (values[hi]-values[lo])*(index-lo)


def tool_error_category(error):
    value = error.lower()
    if 'duplicate filing' in value: return 'duplicate_filing_id'
    if any(v in value for v in ('memory', 'malloc', 'allocate')): return 'memory_limit_or_allocation'
    if 'budget' in value: return 'tool_budget'
    if 'binder error' in value or 'parser error' in value or 'catalog error' in value: return 'sql_schema_or_syntax'
    if 'time' in value: return 'timeout'
    if 'file_stem' in value or 'file_id' in value or 'unknown' in value: return 'invalid_tool_arguments'
    return 'other'


def excerpt_characters(trace):
    """Conservative indicator, not a proof of financial evidence or its absence."""
    total = 0
    metadata = {'source_path','filename','file_stem','accession','file_id','company','form_type','path_in_schema','md5'}
    for step in trace:
        out = step.get('output', {})
        if isinstance(out.get('text'), str): total += len(out['text'])
        for row in out.get('rows', []):
            if isinstance(row, dict):
                total += sum(len(v) for k,v in row.items() if k not in metadata and isinstance(v,str) and len(v)>=80)
    return total


def filing_hits(trace):
    found = set()
    for step in trace:
        output = step.get('output', {})
        entries = [output] + [r for r in output.get('rows', []) if isinstance(r, dict)]
        for row in entries:
            for key in ('file_stem', 'accession'):
                value = row.get(key)
                if isinstance(value, str) and re.fullmatch(r'\d{10}-\d{2}-\d{6}', value): found.add(value)
    return found


def summarize(cases, model, method=None):
    selected = [c for c in cases if c['model']==model and (method is None or c['method']==method)]
    scored = [c for c in selected if c['status']=='ok' and c['correctness'] is not None]
    costs = [c['known_cost_usd'] for c in selected if c['known_cost_usd'] is not None]
    return dict(model=model, provider=selected[0]['provider'] if selected else None, method=method or 'all',
        expected=len(selected), scored=len(scored), failed=len(selected)-len(scored),
        completion_percent=100*len(scored)/len(selected) if selected else 0,
        mean_correctness=avg([c['correctness'] for c in scored]),
        coverage_adjusted_correctness=sum(c['correctness'] for c in scored)/len(selected) if selected else None,
        mean_citation_support=avg([c['citation_support'] for c in scored]),
        perfect_scores=sum(c['correctness']==100 for c in scored),
        scores_at_least_80=sum(c['correctness']>=80 for c in scored),
        scores_below_50=sum(c['correctness']<50 for c in scored),
        known_cost_usd=sum(costs) if costs else None,
        answer_known_cost_usd=sum(c['answer_known_cost_usd'] or 0 for c in selected),
        judge_known_cost_usd=sum(c['judge_known_cost_usd'] or 0 for c in selected),
        unknown_cost_attempts=sum(c['unknown_cost_attempts'] for c in selected),
        mean_known_cost_usd=sum(costs)/len(selected) if costs and selected else None,
        mean_wall_seconds=avg([c['wall_seconds'] for c in selected]),
        p95_wall_seconds=percentile([c['wall_seconds'] for c in selected],.95),
        tool_requested=sum(c['tool_requested'] for c in selected),
        tool_processed=sum(c['tool_processed'] for c in selected),
        tool_dispatched=sum(c['tool_dispatched'] for c in selected),
        tool_cache_hits=sum(c['tool_cache_hits'] for c in selected),
        tool_errors=sum(c['tool_errors'] for c in selected),
        cases_with_tool_errors=sum(c['tool_errors']>0 for c in selected),
        budget_exhausted_cases=sum(c['budget_exhausted'] for c in selected),
        mean_tools=avg([c['tool_requested'] for c in selected]),
        api_attempts=sum(c['api_attempts'] for c in selected),
        api_failed_attempts=sum(c['api_failed_attempts'] for c in selected),
        prompt_tokens=sum(c['prompt_tokens'] for c in selected),
        completion_tokens=sum(c['completion_tokens'] for c in selected),
        reasoning_tokens=sum(c['reasoning_tokens'] for c in selected),
        cache_read_tokens=sum(c['cache_read_tokens'] for c in selected),
        expected_filing_recall=avg([c['expected_filing_recall'] for c in selected]))


def corpus_metadata(manifest):
    """Only identifier/checksum columns, streamed in small batches; no Markdown scan."""
    import pyarrow.parquet as pq
    global_ids = Counter()
    total = 0
    files = []
    duplicates = []
    expected = {a for q in manifest['questions'] for a in q.get('filing_accessions', [])}
    expected_found = set()
    allowed_root = PROJECT.parent/'data'
    for item in manifest['sources'].values():
        path = Path(item['path']).resolve()
        if not path.is_relative_to(allowed_root.resolve()) or not path.is_file():
            return dict(checked=False, reason='Original corpus path is unavailable or outside the approved data folder')
        st = path.stat()
        if st.st_size!=item['size_bytes'] or st.st_mtime_ns!=item['mtime_ns']:
            return dict(checked=False, reason='A source file changed since the benchmark; original recorded snapshot retained')
        ids, variants = Counter(), Counter()
        with pq.ParquetFile(path,pre_buffer=False) as parquet:
            columns = [name for name in ('file_stem','md5','char_count') if name in parquet.schema_arrow.names]
            for batch in parquet.iter_batches(batch_size=2048,columns=columns,use_threads=False):
                for row in batch.to_pylist():
                    stem=row['file_stem'];ids[stem]+=1;variants[(stem,row.get('md5'),row.get('char_count'))]+=1
                    if stem in expected: expected_found.add(stem)
            total += parquet.metadata.num_rows
        dup_ids = {key for key,n in ids.items() if n>1}
        descriptions = defaultdict(list)
        for (stem,md5,chars),n in variants.items():
            if stem in dup_ids: descriptions[stem].append(dict(md5=md5,char_count=chars,occurrences=n))
        for stem in sorted(dup_ids):
            descriptions_for_id=descriptions[stem]
            duplicates.append(dict(filename=path.name,file_stem=stem,rows=ids[stem],
                identical_recorded_md5_and_length=len(descriptions_for_id)==1,
                expected_by_questions=stem in expected, variants=descriptions_for_id))
        files.append(dict(filename=path.name,rows=sum(ids.values()),unique_filing_ids=len(ids),
            duplicated_id_groups=len(dup_ids),extra_duplicate_rows=sum(n-1 for n in ids.values()),bytes=st.st_size))
        global_ids.update(ids)
    return dict(checked=True,rows=total,unique_filing_ids=len(global_ids),files=files,
        within_file_duplicate_groups=len(duplicates),
        within_file_extra_duplicate_rows=sum(f['extra_duplicate_rows'] for f in files),
        identical_checksum_duplicate_groups=sum(d['identical_recorded_md5_and_length'] for d in duplicates),
        duplicate_expected_filing_ids=sorted({d['file_stem'] for d in duplicates if d['expected_by_questions']}),
        expected_filing_ids=len(expected),expected_filing_ids_present=len(expected_found),
        missing_expected_filing_ids=sorted(expected-expected_found),
        duplicate_examples=duplicates[:20],
        reference_filing_duplicates=[d for d in duplicates if d['expected_by_questions']],
        duplicate_listing='First 20 groups plus every reference-filing duplicate. Aggregate counts cover all groups.',
        note='Checksums are stored Parquet metadata, not newly recomputed content hashes. No full Markdown or gold-quote verification was performed.')


def js_json(value):
    return json.dumps(value,ensure_ascii=False,allow_nan=False).replace('<','\\u003c').replace('\u2028','\\u2028').replace('\u2029','\\u2029')


def analyze(run,out,check_corpus=True):
    run=run.resolve(strict=True);out=out.resolve()
    if out==run or out.is_relative_to(run) or run.is_relative_to(out):
        raise ValueError('Derived output must not overlap the original run directory')
    out.mkdir(parents=True,exist_ok=True);(out/'details').mkdir(exist_ok=True)
    generator_sha256=sha(Path(__file__))
    before=snapshot(run)
    save(out/'original-files.sha256.json',before)
    manifest=json.loads((run/'manifest.json').read_text())
    questions=manifest['questions'];qmap={q['id']:q for q in questions}
    answer_models=[c['model'] for c in manifest['configs']['answers']];methods=manifest['methods']
    expected=set(itertools.product(qmap,answer_models,methods))
    task_questions={hashlib.sha256(json.dumps([qid,model,method],sort_keys=True).encode()).hexdigest()[:24]:qmap[qid]
                    for qid,model,method in expected}
    pin_lookup={p['model']:p for p in json.loads((run/'model-pins.json').read_text())}
    calls=list(lines(run/'requests.jsonl'));by_tag=defaultdict(list)
    metadata_requests=list(lines(run/'metadata-requests.jsonl'))
    tools=list(lines(run/'tools.jsonl'));by_tools=defaultdict(list)
    for c in calls:by_tag[c['tag'].split(':')[0]].append(c)
    for i,c in enumerate(calls):
        if c.get('status')!='ok':
            c['retry_recovered']=any(later['tag']==c['tag'] and later.get('status')=='ok' for later in calls[i+1:])
    for t in tools:by_tools[t['tag']].append(t)
    resources=list(lines(run/'resources.jsonl'));executions=list(lines(run/'execution.jsonl'))
    raw_checks=Counter();raw_problems=[];api_failures=[];referenced_ids=set()
    seen_tags=set()
    for call in calls:
        request_path=run/call['request_file'];response_path=run/call['response_file']
        referenced_ids.add(call['call_id']);raw_checks['attempts_checked']+=1
        if not request_path.is_file() or not response_path.is_file():
            raw_problems.append(dict(tag=call['tag'],problem='Missing raw request/response'));continue
        req=json.loads(request_path.read_text());res=json.loads(response_path.read_text())
        payload=req.get('body',{});body=res.get('body',{})
        if not isinstance(body,dict):body={}
        tests=dict(usage_matches=body.get('usage',{})==call.get('usage',{}),
            requested_model_matches=payload.get('model')==call['requested_model'],
            low_reasoning=payload.get('reasoning',{}).get('effort')=='low',
            provider_fallback_disabled=payload.get('provider',{}).get('allow_fallbacks') is False,
            provider_only_matches=payload.get('provider',{}).get('only')==[call['requested_provider']])
        if 'temperature' in payload:tests['zero_temperature_when_sent']=payload['temperature']==0
        if call.get('status')=='ok':
            tests['reported_provider_matched']=call.get('provider_verification')=='reported_match'
            tests['actual_model_matches_raw']=body.get('model')==call.get('actual_model')
            tests['actual_provider_matches_raw']=body.get('provider')==call.get('actual_provider')
            tests['response_id_matches_raw']=body.get('id')==call.get('response_id')
        pin=pin_lookup.get(call['requested_model'],{})
        tests['temperature_capability_respected']=('temperature' in payload)==bool(pin.get('temperature_sent'))
        for name,passed in tests.items():
            raw_checks[name+'_passed']+=int(passed)
            if not passed:raw_problems.append(dict(tag=call['tag'],call_id=call['call_id'],problem=name))
        if ':agent:' in call['tag'] and call['tag'].split(':')[0] not in seen_tags:
            seen_tags.add(call['tag'].split(':')[0])
            # Ground truth is not passed as a separate answer-agent input.
            users=[m.get('content','') for m in payload.get('messages',[]) if m.get('role')=='user']
            raw_checks['agent_initial_inputs_checked']+=1
            question=task_questions.get(call['tag'].split(':')[0])
            inventory='\n'.join(manifest['sources'])
            expected_user=(f'{question["question"]}\nAvailable file IDs:\n{inventory}\nTool budget: {manifest["max_tools"]}.') if question else None
            if not users or users[0]!=expected_user:
                raw_problems.append(dict(tag=call['tag'],problem='Initial input is not exactly question + approved file list + tool budget'))
            else:raw_checks['question_only_initial_input_passed']+=1
            system=[m.get('content') for m in payload.get('messages',[]) if m.get('role')=='system']
            if system!=[manifest['agent_system']]:raw_problems.append(dict(tag=call['tag'],problem='Answer system prompt differs from manifest'))
        if call.get('status')!='ok':
            api_failures.append({k:call.get(k) for k in ('tag','call_id','attempt','status_code','error','usage','requested_model','requested_provider','request_file','response_file','retry_recovered')})
    missing_orphan=[]
    for f in (run/'raw').glob('*.request.json'):
        if f.name.split('.')[0] not in referenced_ids:
            value=json.loads(f.read_text())
            if value.get('method')=='POST':missing_orphan.append(str(f.relative_to(run)))
    current_qfile=PROJECT/'questions/questions.jsonl'
    current_q={q['id']:q for q in lines(current_qfile)}
    question_mismatches=[qid for qid,q in qmap.items() if current_q.get(qid)!=q]
    corpus=corpus_metadata(manifest) if check_corpus else dict(checked=False,reason='Skipped by CLI option')
    save(out/'corpus-audit.json',corpus)
    compact=[];detail_paths=[];tuples=[];judge_problems=[];tool_categories=Counter();tool_errors=Counter()
    structural_problems=[];heuristics=[]
    for result_path in sorted((run/'results').glob('*.json')):
        row=json.loads(result_path.read_text());key=(row['question_id'],row['model'],row['method']);tuples.append(key)
        question=qmap.get(row['question_id'])
        if question is None:structural_problems.append(dict(task_id=row['task_id'],problem='Unknown question'));continue
        if row.get('question')!=question['question'] or row.get('reference')!=question['ground_truth']:
            structural_problems.append(dict(task_id=row['task_id'],problem='Question/reference differs from frozen input'))
        attempts=by_tag[row['task_id']]
        case=export_case(run,row,attempts,by_tools[row['task_id']])
        case['question_metadata']={k:question.get(k) for k in ('difficulty','answer_type','filing_accessions','evidence','verification_status')}
        flags=[];trace=case['tool_trace'];judgment=row.get('judgment',{})
        if row.get('status')=='ok':
            for field in ('correctness','citation_support'):
                score=judgment.get(field)
                if isinstance(score,bool) or not isinstance(score,(int,float)) or not math.isfinite(score) or not 0<=score<=100:
                    judge_problems.append(dict(task_id=row['task_id'],problem='Invalid '+field))
            payload=case['judge']['parsed_input']
            if not isinstance(payload,dict):judge_problems.append(dict(task_id=row['task_id'],problem='Judge input is not JSON'))
            else:
                expected_input=dict(question=question['question'],reference=question['ground_truth'],gold_evidence=question.get('evidence',[]),
                    candidate=row['answer']['text'],tool_evidence=[dict(tool_call=t['tool_call'],output=t['output']) for t in row['answer']['trace']])
                if payload!=expected_input:judge_problems.append(dict(task_id=row['task_id'],problem='Judge input differs from saved answer/reference/evidence'))
            try:
                text=row['judge']['text'].strip()
                if text.startswith('```'):text=text.split('\n',1)[1].rsplit('```',1)[0]
                if json.loads(text)!=judgment:judge_problems.append(dict(task_id=row['task_id'],problem='Parsed score differs from raw judge text'))
            except (ValueError,KeyError):judge_problems.append(dict(task_id=row['task_id'],problem='Unparseable saved judge text'))
        else:flags.append('case_failed')
        for step in trace:
            error=step.get('output',{}).get('error')
            if error:
                category=tool_error_category(error);tool_categories[category]+=1;tool_errors[error.split('\n')[0][:200]]+=1
                if category not in flags:flags.append(category)
        budget=case['tool_counts']['budgeted_attempts']>=manifest['max_tools']
        if budget:flags.append('tool_budget_reached')
        chars=excerpt_characters(trace)
        if row.get('status')=='ok' and judgment.get('correctness',0)>=80 and chars==0:
            flags.append('high_score_without_long_excerpt')
            heuristics.append(dict(task_id=row['task_id'],question_id=row['question_id'],model=row['model'],method=row['method'],
                correctness=judgment.get('correctness'),note='Heuristic review only: short/structured SQL evidence or appropriate abstention can be valid. Not proof of leakage.'))
        if case['evidence_accounting']['output_ids_missing_from_final_request']:
            flags.append('some_tool_outputs_not_in_latest_request')
        if row.get('status')=='ok' and case['evidence_accounting']['output_ids_missing_from_final_request']:
            structural_problems.append(dict(task_id=row['task_id'],problem='Completed answer context misses a recorded tool output'))
        if case['missing_raw_requests']:structural_problems.append(dict(task_id=row['task_id'],problem='Missing final/judge raw request'))
        relevant={a for a in question.get('filing_accessions',[])};hits=filing_hits(trace)
        qrefs=question.get('evidence',[])
        strings=[json.dumps(t.get('output',{}),ensure_ascii=False) for t in trace]
        # Exact quote coverage uses decoded strings, not their JSON-escaped form.
        def text_values(value):
            if isinstance(value,str):yield value
            elif isinstance(value,dict):
                for v in value.values():yield from text_values(v)
            elif isinstance(value,list):
                for v in value:yield from text_values(v)
        evidence_strings=[s for t in trace for s in text_values(t.get('output',{}))]
        quote_hits=sum(any(e.get('exact_quote','') and e['exact_quote'] in s for s in evidence_strings) for e in qrefs)
        total_cost=known_cost(attempts)
        if total_cost['unknown_attempts']:flags.append('unknown_api_cost')
        successful_agent=[a for a in attempts if ':agent:' in a['tag'] and a.get('status')=='ok']
        checkpoint=run/'checkpoints'/f'{row["task_id"]}.json'
        terminal_tool_request=False
        if row.get('status')!='ok' and successful_agent:
            last=successful_agent[-1];body=json.loads((run/last['response_file']).read_text()).get('body',{})
            last_message=(body.get('choices') or [{}])[0].get('message',{})
            terminal_tool_request=bool(last_message.get('tool_calls'))
        case['audit_flags']=flags
        case['raw_finalization_requested_tools_without_answer']=terminal_tool_request
        case['original_run_relative_to_project']=str(run.relative_to(PROJECT))
        filename=f'details/{row["task_id"]}.json'
        save(out/filename,case)
        save_text(out/'details'/f'{row["task_id"]}.js','window.registerBenchmarkCase('+js_json(row['task_id'])+','+js_json(case)+');\n')
        detail_paths.append(out/filename)
        item=dict(task_id=row['task_id'],question_id=row['question_id'],difficulty=question['difficulty'],answer_type=question.get('answer_type'),
            model=row['model'],provider=row.get('provider'),method=row['method'],question=question['question'],reference=question['ground_truth'],
            status=row.get('status'),error=row.get('error'),answer=case['final_answer'],correctness=judgment.get('correctness'),
            citation_support=judgment.get('citation_support'),judge_model=case['judge']['model'],judge_provider=case['judge']['provider'],
            judge_rationale=judgment.get('rationale'),known_cost_usd=total_cost['known_usd'],unknown_cost_attempts=total_cost['unknown_attempts'],
            answer_known_cost_usd=case['billing']['answer']['reported_cost_usd'],judge_known_cost_usd=case['billing']['judge']['reported_cost_usd'],
            wall_seconds=row.get('elapsed_seconds'),api_seconds=case['timing']['api_attempt_seconds'],
            api_attempts=len(attempts),api_failed_attempts=sum(c.get('status')!='ok' for c in attempts),
            model_rounds=case['model_rounds'],tool_requested=case['tool_counts']['requested_by_model'],
            tool_processed=case['tool_counts']['processed'],tool_dispatched=case['tool_counts']['dispatched'],
            tool_cache_hits=case['tool_counts']['cache_hits'],tool_worker_runs=case['tool_counts']['source_worker_runs'],
            tool_errors=case['tool_counts']['errors'],tool_budget_rejected=case['tool_counts']['budget_rejected'],budget_exhausted=budget,
            prompt_tokens=sum(c.get('usage',{}).get('prompt_tokens',0) or 0 for c in attempts),
            completion_tokens=sum(c.get('usage',{}).get('completion_tokens',0) or 0 for c in attempts),
            reasoning_tokens=sum(c.get('usage',{}).get('completion_tokens_details',{}).get('reasoning_tokens',0) or 0 for c in attempts),
            cache_read_tokens=sum(c.get('usage',{}).get('prompt_tokens_details',{}).get('cached_tokens',0) or 0 for c in attempts),
            excerpt_characters=chars,expected_filing_ids=sorted(relevant),observed_filing_ids=sorted(hits),
            expected_filing_recall=len(relevant&hits)/len(relevant) if relevant else None,
            exact_gold_quotes_observed=quote_hits,gold_quote_count=len(qrefs),flags=flags,
            detail_json=filename,detail_js=f'details/{row["task_id"]}.js',
            result_file=str(result_path.relative_to(PROJECT)),
            final_request_file=str((run/case['final_answer_request']['request_file']).relative_to(PROJECT)) if case['final_answer_request']['request_file'] else None,
            judge_request_file=str((run/case['judge']['request_file']).relative_to(PROJECT)) if case['judge']['request_file'] else None)
        compact.append(item)
    compact.sort(key=lambda c:(c['question_id'],c['model'],c['method']))
    actual=set(tuples);missing=sorted(expected-actual);unexpected=sorted(actual-expected)
    duplicates=[list(k) for k,n in Counter(tuples).items() if n>1]
    model_summary=[summarize(compact,m) for m in answer_models]
    strategy_summary=[summarize(compact,m,s) for m in answer_models for s in methods]
    question_rows=[]
    for q in questions:
        members=[c for c in compact if c['question_id']==q['id']]
        question_rows.append(dict(id=q['id'],question=q['question'],reference=q['ground_truth'],difficulty=q['difficulty'],
            answer_type=q.get('answer_type'),filing_accessions=q.get('filing_accessions',[]),evidence=q.get('evidence',[]),
            case_ids=[c['task_id'] for c in members],scores={c['model']+'|'+c['method']:c['correctness'] for c in members},
            mean_correctness=avg([c['correctness'] for c in members]),
            total_correctness=sum(c['correctness'] for c in members if c['correctness'] is not None),
            scored=sum(c['status']=='ok' for c in members),
            expected=len(answer_models)*len(methods),known_cost_usd=sum(c['known_cost_usd'] or 0 for c in members),
            unknown_cost_attempts=sum(c['unknown_cost_attempts'] for c in members),tool_requested=sum(c['tool_requested'] for c in members),
            flags=sorted({flag for c in members for flag in c['flags']})))
    # Compare summary recomputation against the original summaries, without fixing them.
    saved_summary=json.loads((run/'summary.json').read_text());summary_differences=[]
    for old in saved_summary:
        new=next((s for s in strategy_summary if s['model']==old['model'] and s['method']==old['method']),None)
        if new:
            for old_key,new_key in [('completed','scored'),('errors','failed'),('mean_correctness','mean_correctness'),('reported_cost_usd','known_cost_usd')]:
                a,b=old.get(old_key),new.get(new_key)
                equal=a==b if not isinstance(a,(int,float)) or not isinstance(b,(int,float)) else math.isclose(a,b,rel_tol=1e-10,abs_tol=1e-10)
                if not equal:summary_differences.append(dict(model=old['model'],method=old['method'],field=old_key,saved=a,recomputed=b))
    billing=known_cost(calls);saved_bill=json.loads((run/'billing.json').read_text())
    billing_matches=math.isclose(billing['known_usd'] or 0,saved_bill.get('reported_total_usd') or 0,abs_tol=1e-10) and billing['unknown_attempts']==saved_bill.get('unknown_cost_attempts')
    events=Counter()
    for row in resources:
        for key,value in (line.split() for line in row.get('memory.events','').splitlines()):events[key]=max(events[key],int(value))
    post=json.loads((run/'postflight.json').read_text())
    failures=[{k:c[k] for k in ('task_id','question_id','model','method','error')} for c in compact if c['status']!='ok']
    audit=dict(expected_cases=len(expected),result_files=len(compact),scored_cases=sum(c['status']=='ok' for c in compact),
        failed_cases=failures,missing_cases=[list(k) for k in missing],unexpected_cases=[list(k) for k in unexpected],duplicate_cases=duplicates,
        api_attempts=len(calls),api_successful_attempts=sum(c.get('status')=='ok' for c in calls),failed_http_attempts=api_failures,
        metadata_http_attempts=len(metadata_requests),metadata_http_failures=sum(m.get('status')!='ok' for m in metadata_requests),
        inference_requests_without_audit=missing_orphan,raw_checks=dict(raw_checks),raw_integrity_problems=raw_problems,
        question_input_mismatches=question_mismatches,case_structure_problems=structural_problems,judge_input_or_score_problems=judge_problems,
        summary_recalculation_differences=summary_differences,billing_recalculation_matches=billing_matches,
        billing=dict(**billing,complete=billing['unknown_attempts']==0 and not missing_orphan,
            preflight_known_usd=known_cost([c for c in calls if c['tag'].startswith('preflight:')])['known_usd']),
        resource=dict(peak_memory_mib=max(int(r.get('memory.peak',0)) for r in resources)/2**20,
            max_swap_bytes=max(int(r.get('memory.swap.current',0)) for r in resources),memory_events=dict(events),
            wall_seconds=sum(r.get('wall_seconds',0) for r in executions if r.get('event')=='finished')),
        recorded_sources_unchanged=post.get('sources_unchanged'),recorded_code_unchanged=post.get('code_unchanged'),
        tool_calls_requested=sum(c['tool_requested'] for c in compact),tool_outputs=len(tools),
        tool_errors=sum(c['tool_errors'] for c in compact),tool_error_categories=dict(tool_categories),
        most_common_tool_errors=[dict(error=e,count=n) for e,n in tool_errors.most_common(20)],
        cases_with_tool_errors=sum(c['tool_errors']>0 for c in compact),
        cases_reaching_tool_budget=sum(c['budget_exhausted'] for c in compact),
        cache_hits=sum(c['tool_cache_hits'] for c in compact),heuristic_review_cases=heuristics)
    findings=[
        dict(severity='error' if failures else 'pass',code='case_completeness',title=f'{audit["scored_cases"]}/{len(expected)} cases have final answers and Haiku scores',
            detail='Q024 / Gemini Flash / sql_streaming returned a tool call during forced finalization and was not judged.' if failures else 'All expected cases are scored.'),
        dict(severity='warning' if api_failures else 'pass',code='http_retries',title=f'{len(api_failures)} failed HTTP attempts are retained',detail='Failed HTTP attempts were followed by later successful replies for their tags. This differs from the terminal case-level failure.'),
        dict(severity='warning' if not audit['billing']['complete'] else 'pass',code='billing',title='Billing contains unpriced failed attempts' if not audit['billing']['complete'] else 'Billing complete',detail='Known reported charges are a subtotal. Missing usage on HTTP errors is unknown, not confirmed zero.'),
        dict(severity='warning',code='tool_retrieval_errors',title=f'{audit["tool_errors"]} tool error responses',detail='SQL schema/syntax errors, duplicate filing-ID rejection and bounded allocation failures can consume the eight-tool budget and cause abstention. A scored case is not proof of successful retrieval.'),
        dict(severity='warning' if corpus.get('within_file_duplicate_groups',0) else 'info',code='duplicate_ids',title=f'{corpus.get("within_file_duplicate_groups","Unverified")} duplicated filing-ID groups inside source files',detail='Scoped SQL and filing-read tools reject duplicate IDs. Next experiments should distinguish identical stored-checksum copies from conflicting records and support an explicit row identity. No originals were deduplicated or altered.'),
        dict(severity='warning' if events.get('max',0) else 'pass',code='memory',title=f'Memory reached {audit["resource"]["peak_memory_mib"]:.0f} MiB; {events.get("oom_kill",0)} OOM kills',detail='The process-tree limit prevented machine-wide growth. Reaching memory.max still caused pressure and some worker allocation failures; no swap was used.'),
        dict(severity='info',code='score_denominator',title='Scores are judge estimates over scored cases',detail='The primary mean excludes the failed case. A separate coverage-adjusted mean treats missing scores as zero without changing the original score. Citation support and correctness are distinct; neither is a guarantee of objective truth.'),
        dict(severity='info',code='latency_fairness',title='Shared-cache, concurrent-run latency',detail='Four concurrent cases, one serial data tool and a shared cache mean queueing and cache order affect timing. This is not an independent cold-cache latency comparison. The same 100 questions repeat across four conditions, so do not treat 400 scores as independent questions.'),
        dict(severity='info',code='reference_limit',title='Reference and evidence limitations',detail='Judge inputs and scores were mechanically checked against raw payloads. Gold quotes were checked for visibility in retrieved outputs, not reverified against every full source filing. Expected-filing recall only counts explicit accession IDs in structured outputs. Long-excerpt flags are heuristics, not proof of leakage.'),
        dict(severity='warning',code='question_supplied_operands',title='Some questions already supply the financial operands',detail='Q053, Q059, Q076, Q083 and Q085 include numbers sufficient for substantial arithmetic answers. High correctness with little retrieved text can therefore reflect calculation from the question, rather than successful document retrieval. For example, Q059 / Flash / SQL used a constant arithmetic SELECT; Haiku awarded 98 correctness and 80 citation support. Report correctness and retrieval indicators separately.'),
    ]
    all_consistent=not(raw_problems or judge_problems or structural_problems or summary_differences or missing or unexpected or duplicates or question_mismatches) and billing_matches
    findings.append(dict(severity='pass' if all_consistent else 'error',code='consistency',title='Saved payloads, scores and summaries reconcile' if all_consistent else 'Some saved records need integrity review',detail='See structured audit checks and the original-preservation manifest. No requests were retried and no answers were rejudged.'))
    audit['findings']=findings;audit['mechanically_consistent']=all_consistent
    configs=[dict(c,role='answer') for c in manifest['configs']['answers']]+[dict(manifest['configs']['judge'],role='judge')]
    pins=json.loads((run/'model-pins.json').read_text())
    settings=[]
    for c in configs:
        pin=next((p for p in pins if p['model']==c['model']),{})
        settings.append(dict(**c,temperature_sent=pin.get('temperature_sent',False)))
    metadata=dict(schema_version=1,generated_utc=datetime.now(timezone.utc).isoformat(),
        generator_sha256=generator_sha256,dependency_lock_sha256=sha(PROJECT/'uv.lock'),
        source_run=str(run.relative_to(PROJECT)),output=str(out.relative_to(PROJECT)),
        source_questions='questions/questions.jsonl',question_count=len(questions),answer_models=answer_models,methods=methods,
        dataset=dict(files=len(manifest['sources']),rows=corpus.get('rows') if corpus.get('checked') else
            sum(f['rows'] for f in json.loads((run/'sources.json').read_text())['output']['files']),
            compressed_bytes=sum(v['size_bytes'] for v in manifest['sources'].values()),
            unique_filing_ids=corpus.get('unique_filing_ids'),
            within_file_duplicate_groups=corpus.get('within_file_duplicate_groups'),
            extra_duplicate_rows=corpus.get('within_file_extra_duplicate_rows'),
            identical_checksum_duplicate_groups=corpus.get('identical_checksum_duplicate_groups'),
            conflicting_checksum_duplicate_groups=corpus.get('within_file_duplicate_groups',0)-corpus.get('identical_checksum_duplicate_groups',0),
            reference_filing_ids=corpus.get('expected_filing_ids'),reference_filing_ids_present=corpus.get('expected_filing_ids_present'),
            months='January–June 2025'),
        settings=settings,run_parameters={k:manifest.get(k) for k in ('max_tools','concurrency','cache_policy')},
        scope='Full 100-question run; four model/strategy conditions. Raw originals remain immutable.',
        metric_notes=['Mean correctness excludes unscored cases. Coverage-adjusted score includes missing as zero.',
            'Known API cost includes failed/finalization attempts when usage was returned. Unpriced errors remain unknown.',
            'Case cost includes its answer and judge; preflight is counted only in the run total.',
            'Tool requested/processed/dispatched/cache counts are distinct. See audit definitions.',
            'Model summaries pool both strategies; strategy rows separate them.'])
    difficulty=[]
    for model in answer_models:
        for method in methods:
            for level in dict.fromkeys(q['difficulty'] for q in questions):
                group=[c for c in compact if c['model']==model and c['method']==method and c['difficulty']==level]
                difficulty.append(dict(model=model,method=method,difficulty=level,count=len(group),scored=sum(c['status']=='ok' for c in group),
                    mean_correctness=avg([c['correctness'] for c in group])))
    view=dict(metadata=metadata,audit=audit,model_summary=model_summary,strategy_summary=strategy_summary,
        difficulty_summary=difficulty,questions=question_rows,cases=compact)
    save(out/'dashboard.json',view)
    save_text(out/'dashboard-data.js','window.BENCHMARK_DATA='+js_json(view)+';\n')
    # Full formatted JSON is streamed case-by-case to keep memory bounded.
    full=out/'benchmark.json';temp=full.with_suffix('.json.tmp')
    with temp.open('w') as stream:
        header={k:v for k,v in view.items() if k!='cases'}
        serialized=json.dumps(header,ensure_ascii=False,indent=2,allow_nan=False)
        stream.write(serialized[:-2]+',\n  "cases": [\n')
        for i,path in enumerate(detail_paths):
            if i:stream.write(',\n')
            with path.open() as source:
                for line in source:stream.write('    '+line)
        stream.write('\n  ]\n}\n')
    temp.replace(full)
    save(out/'audit.json',audit)
    with (out/'cases.csv').open('w') as stream:
        fields=[k for k in compact[0] if k not in ('flags','expected_filing_ids','observed_filing_ids')]
        writer=csv.DictWriter(stream,fieldnames=fields,extrasaction='ignore',lineterminator='\n');writer.writeheader();writer.writerows(compact)
    report=['# Read-only audit: all 100 questions','',f'Cases scored: **{audit["scored_cases"]}/{len(expected)}**. Known reported API spend: **${billing["known_usd"]:.9f}**; {billing["unknown_attempts"]} unpriced attempts.',
        '',f'HTTP attempts: {len(calls)} total, {audit["api_successful_attempts"]} successful, {len(api_failures)} failed. No model calls were made by this audit.','']
    for finding in findings:report += [f'## {finding["severity"].upper()}: {finding["title"]}', '',finding['detail'],'']
    report += ['## Scores as recorded','', '| Model | Strategy | Scored | Mean correctness | Missing=0 mean | Known USD |','|---|---|---:|---:|---:|---:|']
    for s in strategy_summary:report.append(f'| {s["model"]} | {s["method"]} | {s["scored"]}/{s["expected"]} | {s["mean_correctness"]:.2f} | {s["coverage_adjusted_correctness"]:.2f} | {s["known_cost_usd"]:.6f} |')
    report += ['','## Next experiments','',
        '- Keep this run unchanged. Recover the missing answer/judgment only in a separately recorded recovery experiment, if desired.',
        '- Handle duplicated filing IDs with exact-row selection or a verified identical-content rule; do not blindly choose conflicting copies.',
        '- Strengthen final-answer enforcement and record exhausted-budget requests separately from valid answers.',
        '- Give models clearer schema repair feedback; account for worker allocation failures and consider streaming before text SQL.',
        '- Use uncached or independently warmed runs for latency comparisons. Validate a blinded sample of Haiku judgments and gold references manually.',
        '- Reconcile unpriced HTTP errors with gateway billing; do not replace unknown charges with zero.',
        '', 'Full details: `audit.json`, `corpus-audit.json`, `benchmark.json`; the HTML dashboard uses a compact export and lazy case files.']
    (out/'audit-report.md').write_text('\n'.join(report)+'\n')
    after=snapshot(run)
    changed=[p for p in before if after.get(p)!=before[p]];added=sorted(set(after)-set(before));removed=sorted(set(before)-set(after))
    preservation=dict(passed=not(changed or added or removed),files_checked=len(before),changed=changed,added=added,removed=removed,
        new_model_requests=0,run_directory=str(run),original_snapshot='original-files.sha256.json',
        generator_code_unchanged=sha(Path(__file__))==generator_sha256)
    save(out/'preservation.json',preservation)
    if not preservation['passed']:raise ValueError('Original run changed while auditing; see preservation.json')
    print(json.dumps(dict(scored=audit['scored_cases'],expected=len(expected),known_usd=billing['known_usd'],unknown_costs=billing['unknown_attempts'],
        tool_errors=audit['tool_errors'],duplicate_groups=corpus.get('within_file_duplicate_groups'),mechanically_consistent=all_consistent,
        original_files_preserved=len(before),output=str(out)),indent=2))
    return view


def main():
    from agentic_search_only.parquet_agent import require_resource_scope
    require_resource_scope()
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run',type=Path,default=PROJECT/'reports-whole-data/all100')
    parser.add_argument('--output',type=Path,default=PROJECT/'analysis/all100')
    parser.add_argument('--skip-corpus',action='store_true')
    args=parser.parse_args()
    # Shared read lock prevents analyzing a run actively being written by the runner.
    import fcntl
    with (args.run/'.lock').open('r') as lock:
        fcntl.flock(lock,fcntl.LOCK_SH|fcntl.LOCK_NB)
        analyze(args.run,args.output,not args.skip_corpus)


if __name__=='__main__':main()
