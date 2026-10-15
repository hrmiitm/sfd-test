"""Small offline HTML report for Parquet-agent runs (standard library only)."""
import html
import json
import math
from pathlib import Path
from .case_audit import build as build_cases


def render_case(case, filename):
    e = lambda value: html.escape(str(value), quote=True)
    pretty = lambda value: e(json.dumps(value, ensure_ascii=False, indent=2))
    counts, judge = case['tool_counts'], case['judge']
    content = (f'<details class="case" data-question="{e(case["question_id"])}" '
        f'data-model="{e(case["model"])}" data-method="{e(case["method"])}">'
        f'<summary>{e(case["question_id"])} · {e(case["model"])} · {e(case["method"])} · '
        f'{e(case["status"])} · {e(judge["correctness"])}/100 · {counts["requested_by_model"]} tool requests</summary>'
        f'<h3>Question</h3><p>{e(case["question"])}</p>'
        '<h3>Tool counts and timing</h3>'
        f'<pre>{pretty(dict(tool_counts=counts, model_rounds=case["model_rounds"], api_attempts=case["api_attempt_count"], timing=case["timing"], billing=case["billing"]))}</pre>'
        '<p>Requested = emitted by successful model turns; processed includes rejected calls. Dispatched includes '
        'cache lookups; source worker runs exclude cache hits. Errors and budget rejections are counted explicitly.</p>'
        f'<h3>Final answer</h3><pre>{e(case["final_answer"] or case.get("error") or "pending")}</pre>'
        f'<h3>Reference</h3><pre>{e(case["reference"])}</pre>'
        f'<h3>Haiku judgment</h3><p>Correctness: {e(judge["correctness"])}/100 · '
        f'Citation support: {e(judge["citation_support"])}/100 · {e(judge["model"])} / {e(judge["provider"])}</p>'
        f'<p>{e(judge["rationale"])}</p><h3>Retrieval timeline — exact arguments and returned data</h3>')
    for i, trace in enumerate(case['tool_trace'], 1):
        call = trace['tool_call']
        status = 'error' if 'error' in trace['output'] else 'cache hit' if trace.get('cache_hit') else 'completed'
        content += (f'<details><summary>Step {i}: {e(call["function"]["name"])} · {status}</summary>'
            f'<h3>Arguments / SQL</h3><pre>{e(call["function"]["arguments"])}</pre>'
            f'<h3>Retrieved output — exact tool response</h3><pre>{pretty(trace["output"])}</pre>'
            f'<h3>Timing and execution details</h3><pre>{pretty({k:v for k,v in trace.items() if k not in ("tool_call", "output")})}</pre></details>')
    content += ('<details><summary>Exact context sent for the final answer</summary>'
        '<p>This is the actual saved request body, including the system prompt, question, earlier model/tool '
        'messages, returned evidence, parameters and tool definitions where present. No evidence subset is inferred.</p>'
        f'<pre>{pretty(case["final_answer_request"]["body"])}</pre>'
        f'<h3>Evidence accounting</h3><pre>{pretty(case["evidence_accounting"])}</pre></details>'
        '<details><summary>Exact context sent to Haiku and its raw answer</summary>'
        f'<pre>{pretty(judge["request_body"])}</pre><h3>Raw judge answer</h3><pre>{e(judge["raw_text"])}</pre></details>'
        '<details><summary>All API attempts — parameters, usage, cost and raw files</summary>')
    for attempt in case['api_attempts']:
        metadata = {k: v for k, v in attempt.items() if k not in ('request_file', 'response_file')}
        content += f'<pre>{pretty(metadata)}</pre>'
        for key in ('request_file', 'response_file'):
            if attempt.get(key):
                content += f'<a href="{e(attempt[key])}">{e(key)}</a> '
    content += ('</details>'
        f'<p><a href="{e(filename)}">Complete per-case JSON</a> · '
        f'<a href="{e(case["original_result_file"])}">Original result JSON</a></p></details>')
    return content


def build(output):
    output = Path(output)
    case_index = build_cases(output)
    summary = json.loads((output / 'summary.json').read_text())
    billing = json.loads((output / 'billing.json').read_text())
    selection = json.loads((output / 'selection.json').read_text())
    e = lambda value: html.escape(str(value), quote=True)
    fmt = lambda value: 'unknown' if value is None else f'{value:.2f}'
    usd = lambda value: 'unknown' if value is None else f'${value:.6f}'
    points = [s for s in summary if s['completed'] and not s['unknown_cost_attempts']
              and s['reported_cost_usd'] is not None and s['reported_cost_usd'] > 0
              and s['mean_correctness'] is not None]
    costs = [s['reported_cost_usd'] / s['completed'] for s in points]
    lo = math.floor(math.log10(min(costs))) if costs else -4
    hi = math.ceil(math.log10(max(costs))) if costs else -1
    if hi == lo:
        hi += 1
    svg = ['<svg viewBox="0 0 940 440" role="img" aria-label="Correctness against log-scale cost per completed case">']
    for score in range(0, 101, 20):
        y = 345 - score * 2.9
        svg.append(f'<line x1="90" x2="880" y1="{y}" y2="{y}" class="grid"/><text x="72" y="{y+5}" text-anchor="end">{score}</text>')
    for power in range(lo, hi + 1):
        x = 90 + (power - lo) / (hi - lo) * 790
        svg.append(f'<line x1="{x}" x2="{x}" y1="55" y2="345" class="grid"/><text x="{x}" y="371" text-anchor="middle">${10**power:g}</text>')
    for s in points:
        cost = s['reported_cost_usd'] / s['completed']
        x = 90 + (math.log10(cost) - lo) / (hi - lo) * 790
        y = 345 - s['mean_correctness'] * 2.9
        color = '#67d5ed' if 'gemini' in s['model'] else '#ffbe7b'
        label = s['model'].split('/')[-1] + ' · ' + s['method']
        shape = (f'<circle cx="{x}" cy="{y}" r="7" fill="{color}"/>' if s['method'] == 'sql' else
                 f'<rect x="{x-6}" y="{y-6}" width="12" height="12" fill="{color}"/>')
        # Alternate label offsets to keep coincident 100-score points legible.
        dy = -15 if s['method'] == 'sql' else 23
        anchor = 'end' if x > 650 else 'start'
        offset = -10 if anchor == 'end' else 10
        svg.append(f'<g><title>{e(label)}: {s["mean_correctness"]:.1f}/100; {usd(cost)}/case</title>{shape}'
                   f'<text x="{x+offset}" y="{y+dy}" text-anchor="{anchor}" class="label">{e(label)}</text></g>')
    svg.extend(['<text x="485" y="418" text-anchor="middle">Answer + judge USD / completed case · logarithmic scale</text>',
                '<text transform="translate(22,200) rotate(-90)" text-anchor="middle">Correctness / 100</text></svg>'])
    table = []
    for s in summary:
        average = s['reported_cost_usd'] / s['completed'] if s['completed'] and s['reported_cost_usd'] is not None else None
        table.append('<tr>' + ''.join(f'<td>{e(v)}</td>' for v in [s['model'], s['provider'], s['method'],
            f'{s["completed"]}/{s["expected"]}', fmt(s['mean_correctness']), fmt(s['mean_citation_support']),
            usd(average), usd(s['reported_cost_usd']), fmt(s['mean_case_wall_seconds']),
            s['api_attempts'], s['tool_calls'], s['tool_cache_hits'], s['errors'], s['unknown_cost_attempts']]) + '</tr>')
    details = []
    for entry in case_index:
        case = json.loads((output / entry['case_file']).read_text())
        details.append(render_case(case, entry['case_file']))
    filters = '<div class="filters">'
    for key, label in [('question_id', 'Question'), ('model', 'Model'), ('method', 'Strategy')]:
        options = ''.join(f'<option value="{e(v)}">{e(v)}</option>' for v in sorted({r[key] for r in case_index}))
        filters += f'<label>{label} <select id="filter-{key}"><option value="">All</option>{options}</select></label>'
    filters += '</div><p id="case-count" aria-live="polite"></p>'
    document = '''<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Whole-corpus Parquet agent benchmark</title><style>
:root{color-scheme:dark;font-family:system-ui,sans-serif;background:#0b1220;color:#e5ecfa}body{max-width:1300px;margin:0 auto;padding:30px 20px 60px}h1{font-size:clamp(25px,4vw,39px);margin-bottom:8px}p{line-height:1.6;color:#c1cde0}a{color:#78d9f0}.cards{display:flex;gap:16px;flex-wrap:wrap;margin:24px 0}.card,section,details{background:#121e30;border:1px solid #293a50;border-radius:12px;padding:18px}.card{flex:1;min-width:160px}.card strong{display:block;font-size:25px;margin-top:8px}section{margin:20px 0}svg{width:100%;height:auto}.grid{stroke:#2a3a50}svg text{fill:#c1cde0;font-size:14px}.label{font-size:12px;fill:#eff6ff}.scroll{overflow-x:auto}table{border-collapse:collapse;min-width:1100px;width:100%;font-size:13px}th,td{text-align:left;border-bottom:1px solid #2a3a50;padding:12px 9px;vertical-align:top}th{color:#80d9ec}pre{white-space:pre-wrap;overflow-wrap:anywhere;font:14px/1.6 ui-monospace,monospace}summary{cursor:pointer;font-weight:600}details{margin:10px 0}h3{font-size:15px;color:#80d9ec}small{color:#a8b8cd}
</style><h1>Whole-corpus Parquet agent benchmark</h1>
<p>Direct DuckDB queries and bounded streaming reads. No embeddings or vector database.</p>'''
    document += (f'<div class="cards"><div class="card">Selected questions<strong>{len(selection["ids"])}</strong></div>'
        f'<div class="card">Completed cases<strong>{sum(s["completed"] for s in summary)} / {sum(s["expected"] for s in summary)}</strong></div>'
        f'<div class="card">Reported API spend<strong>{usd(billing["reported_total_usd"])}</strong></div></div>'
        f'<p>Selection: {e(selection["selection"])}; seed {e(selection["seed"])}. '
        'This run is a smoke test if only a small question set was selected; its scores are not a population benchmark.</p>'
        '<section><h2>Correctness and cost</h2>' + ''.join(svg) +
        '<p>Circles: SQL. Squares: SQL + streaming. Hover for values. Cost includes all answer/judge attempts for each group, '
        'divided by completed cases. Preflight spend appears only in total spend. Groups with unknown costs are omitted from the plot.</p></section>'
        '<section><h2>Model × strategy results</h2><div class="scroll"><table><thead><tr>' +
        ''.join(f'<th>{v}</th>' for v in ['Model', 'Provider', 'Strategy', 'Completed', 'Correctness /100', 'Support /100',
            'USD/case', 'Group USD', 'Mean case seconds', 'API attempts', 'Tools', 'Cache hits', 'Errors', 'Unknown costs']) +
        '</tr></thead><tbody>' + ''.join(table) + '</tbody></table></div></section>'
        '<h2>Inspect a question / model / strategy</h2>' + filters + ''.join(details) +
        '<p><a href="cases-index.json">Searchable case index JSON</a> · <a href="cases-index.csv">Per-case CSV with answers and scores</a></p>' +
        '<p><a href="requests.jsonl">API audit</a> · <a href="tools.jsonl">Tool audit</a> · '
        '<a href="summary.csv">Summary CSV</a> · <a href="billing.json">Billing completeness</a> · '
        '<a href="model-pins.json">Model/provider pins</a> · <a href="resources.jsonl">Resource measurements</a></p>'
        '<small>Four concurrent API calls; one data tool at a time. Shared tool-cache hits, if enabled, affect latency. '
        'Temperature zero is sent only where supported; all models request low reasoning. Costs come from API usage, '
        'including failures and retries. Missing cost or interrupted requests mean incomplete billing.</small>'
        '<style>.filters{display:flex;flex-wrap:wrap;gap:16px}.filters label{display:flex;flex-direction:column;gap:6px}'
        'select{max-width:100%;padding:9px;background:#0b1220;color:#e5ecfa;border:1px solid #53677e;border-radius:6px}'
        '[hidden]{display:none!important}</style>'
        '<script>const filterIds=["question_id","model","method"];'
        'function filterCases(){const values=filterIds.map(k=>document.getElementById("filter-"+k).value);let count=0;'
        'document.querySelectorAll("details.case").forEach(el=>{el.hidden=!([el.dataset.question,el.dataset.model,el.dataset.method]'
        '.every((v,i)=>!values[i]||values[i]===v));if(!el.hidden)count++;});'
        'document.getElementById("case-count").textContent=count+" matching cases";}'
        'filterIds.forEach(k=>document.getElementById("filter-"+k).addEventListener("change",filterCases));filterCases();</script></html>')
    (output / 'report.html').write_text(document)
    return output / 'report.html'


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('output', type=Path)
    print(build(parser.parse_args().output))
