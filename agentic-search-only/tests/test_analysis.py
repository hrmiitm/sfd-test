"""Critical audit semantics: missing scores, unknown bills and source protection."""
import importlib.util
from pathlib import Path

import pytest

path=Path(__file__).resolve().parents[1]/'scripts/analyze-results.py'
spec=importlib.util.spec_from_file_location('result_analysis',path)
analysis=importlib.util.module_from_spec(spec)
spec.loader.exec_module(analysis)


def test_unknown_billing_is_never_confirmed_zero():
    result=analysis.known_cost([{'usage':{'cost':.1}},{'usage':{}},{'usage':{'cost':0}}])
    assert result=={'known_usd':.1,'unknown_attempts':1}
    assert analysis.known_cost([{'usage':{}}])=={'known_usd':None,'unknown_attempts':1}


def case(status,score,cost):
    return dict(model='m',provider='p',method='sql',status=status,correctness=score,citation_support=90,
        known_cost_usd=cost,answer_known_cost_usd=cost,judge_known_cost_usd=0,unknown_cost_attempts=0,
        wall_seconds=1,tool_requested=2,tool_processed=2,tool_dispatched=2,tool_cache_hits=0,tool_errors=0,
        budget_exhausted=False,api_attempts=2,api_failed_attempts=0,prompt_tokens=10,completion_tokens=2,
        reasoning_tokens=0,cache_read_tokens=0,expected_filing_recall=1)


def test_failed_case_remains_in_coverage_and_cost_not_scored_mean():
    summary=analysis.summarize([case('ok',80,.2),case('error',None,.1)],'m','sql')
    assert summary['scored']==1 and summary['failed']==1 and summary['expected']==2
    assert summary['mean_correctness']==80
    assert summary['coverage_adjusted_correctness']==40
    assert abs(summary['known_cost_usd']-.3)<1e-12
    assert summary['mean_known_cost_usd']==pytest.approx(.15)


def test_analyzer_refuses_overlapping_output_before_writing(tmp_path):
    run=tmp_path/'raw';run.mkdir();marker=run/'original.json';marker.write_text('{"untouched":true}')
    for output in [run,run/'derived',tmp_path]:
        with pytest.raises(ValueError,match='overlap'):
            analysis.analyze(run,output)
    assert marker.read_text()=='{"untouched":true}' and len(list(run.iterdir()))==1


def test_memory_and_duplicate_errors_are_distinguished():
    assert analysis.tool_error_category('malloc of size 646993472 failed')=='memory_limit_or_allocation'
    assert analysis.tool_error_category('Duplicate filing ID in source')=='duplicate_filing_id'
    assert analysis.tool_error_category('Tool budget exhausted')=='tool_budget'


def test_arithmetic_sql_does_not_masquerade_as_long_filing_excerpt():
    trace=[{'output':{'rows':[{'rev_per_emp':205241.317,'filename':'x'*200,'file_stem':'0001'}]}}]
    assert analysis.excerpt_characters(trace)==0
    assert analysis.excerpt_characters([{'output':{'text':'Revenue'}}])==7
