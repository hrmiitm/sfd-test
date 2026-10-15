#!/usr/bin/env python3
"""Build a self-contained offline HTML report from recorded benchmark artifacts."""
import argparse
from collections import defaultdict
from datetime import datetime, timezone
import hashlib
import html
import json
from pathlib import Path
import statistics


def read_lines(path):
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()] if path.exists() else []


def build(root):
    manifest=json.loads((root/'manifest.json').read_text())
    questions=manifest['config']['questions']
    selection=json.loads((root/'selection.json').read_text())
    results=[json.loads(p.read_text()) for p in sorted((root/'results').glob('*.json')) if not p.name.endswith('.agent.json')]
    retrieval=json.loads((root/'retrieval.json').read_text())
    calls=read_lines(root/'requests.jsonl')
    tagged=defaultdict(list)
    for c in calls:tagged[c['tag'].split(':')[0]].append(c)
    groups=defaultdict(list)
    details=[]
    for row in results:
        groups[(row['model'],row['column'])].append(row)
        key=hashlib.sha256(f"{row['question_id']}:{row['model']}:{row['column']}".encode()).hexdigest()[:24]
        history=tagged[key]
        details.append(dict(id=row['question_id'],model=row['model'],strategy=row['column'],status=row.get('status'),
            correctness=row.get('judgment',{}).get('correctness'),citation_support=row.get('judgment',{}).get('citation_support'),
            rationale=row.get('judgment',{}).get('rationale'),answer=row.get('answer',{}).get('text'),
            error=row.get('error'),retrieval=row.get('retrieval'),
            cost=sum(c.get('usage',{}).get('cost') or 0 for c in history),
            calls=len(history),tool_calls=row.get('answer',{}).get('tool_calls',0),
            evidence=[dict(id=c.get('id'),accession=c.get('accession'),section=c.get('section'),text=c.get('text'))
                      for c in row.get('answer',{}).get('chunks',[])],
            tools=[dict(name=t['tool_call']['function']['name'],arguments=t['tool_call']['function']['arguments'],
                        error=t['output'].get('error'),resources=t['output'].get('resources')) for t in row.get('answer',{}).get('trace',[])],
            file=f'results/{key}.json'))
    cells=[]
    for model in manifest['config']['models']:
        for backend in manifest['config']['backends']:
            for method in manifest['config']['methods']:
                column=f'{backend}/{method}'
                group=groups[(model,column)];good=[r for r in group if r.get('status')=='ok']
                history=[]
                for q in questions:
                    key=hashlib.sha256(f"{q['id']}:{model}:{column}".encode()).hexdigest()[:24]
                    history.extend(tagged[key])
                known=[c for c in history if isinstance(c.get('usage',{}).get('cost'),(int,float))]
                unknown=len(history)-len(known)
                total=sum(c['usage']['cost'] for c in known)
                answer_cost=sum(c['usage']['cost'] for c in known if not c['tag'].endswith(':judge'))
                judge_cost=sum(c['usage']['cost'] for c in known if c['tag'].endswith(':judge'))
                scores=[r['judgment']['correctness'] for r in good]
                cells.append(dict(model=model,backend=backend,method=method,strategy=column,
                    label=('Gemini' if 'gemini' in model else 'Luna')+' · '+('LanceDB' if backend=='lancedb' else 'Qdrant')+' / '+method,
                    correctness=statistics.mean(scores) if scores else None,
                    sd=statistics.stdev(scores) if len(scores)>1 else 0,completed=len(good),expected=len(questions),
                    errors=sum(r.get('status')=='error' for r in group),cost=total,answer_cost=answer_cost,judge_cost=judge_cost,
                    per_question_cost=total/len(questions) if history and not unknown else None,
                    unknown_cost_calls=unknown,call_count=len(history),
                    tokens=sum(c.get('usage',{}).get('total_tokens',0) for c in history),
                    mean_answer_s=statistics.mean(r['answer']['seconds'] for r in good) if good else None,
                    mean_retrieval_s=statistics.mean(r['retrieval']['seconds'] for r in good) if good else None,
                    recall=statistics.mean(r['retrieval']['accession_recall'] for r in good) if good else None,
                    citation=statistics.mean(r['judgment']['citation_support'] for r in good) if good else None))
    resources=read_lines(root/'resources.jsonl')
    preflight=json.loads((root/'preflight.json').read_text())
    postflight=json.loads((root/'postflight.json').read_text()) if (root/'postflight.json').exists() else None
    execution=json.loads((root/'execution.json').read_text()) if (root/'execution.json').exists() else {}
    history=read_lines(root/'execution-history.jsonl')
    if history:
        execution['first_started_utc']=history[0]['started_utc']
    recovery=json.loads((root/'recovery.json').read_text()) if (root/'recovery.json').exists() else None
    embeddings=read_lines(root/'embedding-requests.jsonl')
    health_calls=read_lines(root/'preflight-requests.jsonl')+read_lines(root/'preflight-embeddings.jsonl')
    data=dict(selection=selection,cells=cells,details=details,questions=questions,preflight=preflight,
        retrieval={k:v['chunks'] for k,v in retrieval['entries'].items()},
        corpus_state={k:v for k,v in retrieval.items() if k!='entries'},
        execution=execution,postflight=postflight,execution_history=history,recovery=recovery,config=manifest['config'],generated_utc=datetime.now(timezone.utc).isoformat(),
        audit=dict(calls=len(calls),known_chat_usd=sum(c.get('usage',{}).get('cost') or 0 for c in calls),
            unknown_chat_costs=sum(not isinstance(c.get('usage',{}).get('cost'),(int,float)) for c in calls),
            embedding_usd=sum(c.get('usage',{}).get('cost') or 0 for c in embeddings),
            unknown_embedding_costs=sum(not isinstance(c.get('usage',{}).get('cost'),(int,float)) for c in embeddings),
            preflight_usd=sum(c.get('usage',{}).get('cost') or 0 for c in health_calls),
            peak_tree_gib=max((int(r.get('memory.peak',0)) for r in resources),default=0)/1024**3))
    (root/'report-data.json').write_text(json.dumps(data,indent=2))
    template=Path(__file__).with_name('benchmark-report-template.html').read_text()
    shared=Path(__file__).resolve().parents[1]/'assets'
    template=template.replace('__DATA_SOURCE_CSS__',(shared/'data-source.css').read_text())
    template=template.replace('__DATA_SOURCE_JS__',(shared/'data-source.js').read_text())
    encoded=json.dumps(data).replace('<','\\u003c').replace('&','\\u0026').replace('\u2028','\\u2028').replace('\u2029','\\u2029')
    (root/'report.html').write_text(template.replace('__REPORT_DATA__',encoded))
    print(root/'report.html')


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('run',type=Path);a=p.parse_args();build(a.run)
