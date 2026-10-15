#!/usr/bin/env python3
"""Recalculate stage timings from the original 20-question results, without editing them."""
import argparse
from collections import defaultdict
import json
import math
from pathlib import Path
import statistics


def verify(root):
    report=json.loads((root/'report-data.json').read_text())
    cached=json.loads((root/'retrieval.json').read_text())['entries']
    grouped=defaultdict(list)
    for path in sorted((root/'results').glob('*.json')):
        if path.name.endswith('.agent.json'):continue
        row=json.loads(path.read_text())
        if row.get('status')!='ok':continue
        answer=row['answer']['seconds']; retrieval=row['retrieval']['seconds']
        if not all(isinstance(v,(int,float)) and math.isfinite(v) and v>=0 for v in (answer,retrieval)):
            raise ValueError(f'Missing or invalid timing: {path.name}')
        if row['column'].endswith('/agentic'):
            if not math.isclose(retrieval,row['answer']['tool_seconds'],abs_tol=1e-8):raise ValueError('Tool timing mismatch')
            if not math.isclose(answer,sum(call['seconds'] for call in row['answer']['calls']),abs_tol=1e-8):raise ValueError('Model-call timing mismatch')
        else:
            entry=cached[row['question_id']+':'+row['column']]
            if not math.isclose(retrieval,entry['seconds'],abs_tol=1e-8):raise ValueError('Cached retrieval timing mismatch')
        grouped[(row['model'],row['column'])].append((answer,retrieval))
    groups=[]
    for cell in report['cells']:
        rows=grouped[(cell['model'],cell['strategy'])]
        answer=statistics.mean(a for a,r in rows);retrieval=statistics.mean(r for a,r in rows)
        if len(rows)!=cell['completed'] or not math.isclose(answer,cell['mean_answer_s'],abs_tol=1e-8) or not math.isclose(retrieval,cell['mean_retrieval_s'],abs_tol=1e-8):
            raise ValueError('Recomputed timing differs from the report: '+cell['label'])
        groups.append(dict(model=cell['model'],backend=cell['backend'],method=cell['method'],scored_cases=len(rows),
                           mean_answer_seconds=answer,mean_retrieval_seconds=retrieval))
    return dict(passed=True,source_run='reports/random20-seed20261015',checked_cases=sum(len(v) for v in grouped.values()),groups=groups,
                definitions=dict(answer='Recorded answer.seconds; for agentic cases, sum of model-call durations.',
                                 retrieval='Recorded retrieval.seconds; cached search latency for BM25/Vector/Hybrid, cumulative tool duration for Agentic Search.',
                                 exclusions='Judge duration and embedding-preparation time are excluded from these stage timings.',
                                 shared_cache='Non-agentic retrieval was precomputed once per question/database/method and shared by both answer models.'))


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('run',type=Path,nargs='?',default=Path('reports/random20-seed20261015'))
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args();result=verify(args.run)
    args.output.write_text(json.dumps(result,indent=2)+'\n')
    print(f'Timing checks passed: {result["checked_cases"]} cases, {len(result["groups"])} conditions; saved {args.output}')


if __name__=='__main__':main()
