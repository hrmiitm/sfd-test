"""Standalone defaults must not depend on the caller's working directory."""
import json
from pathlib import Path
import sys

import agentic_search_only.parquet_agent as runner


def test_independent_defaults_and_all100(monkeypatch, tmp_path):
    captured = {}
    dotenv_paths = []
    async def run(args):
        captured.update(vars(args))
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(runner, 'load_dotenv', lambda path: dotenv_paths.append(path))
    monkeypatch.setattr(runner, 'require_resource_scope', lambda: '/test-scope')
    monkeypatch.setattr(runner, 'run', run)
    monkeypatch.setattr(sys, 'argv', ['parquet_agent', '--output', str(tmp_path/'results')])
    runner.main()
    project = Path(runner.__file__).resolve().parents[2]
    assert project.name == 'agentic-search-only'
    assert captured['count'] == 100
    assert captured['source'] == project.parent/'data/parsed'
    assert captured['questions'] == project/'questions/questions.jsonl'
    assert captured['models_config'] == project/'config/parquet-models.json'
    assert len(captured['questions'].read_text().splitlines()) == 100
    assert dotenv_paths == [project/'.env', project.parent/'.env']
    assert all((Path(runner.__file__).parent/name).is_file() for name in runner.code_snapshot())
    assert not (project/'data').exists()
