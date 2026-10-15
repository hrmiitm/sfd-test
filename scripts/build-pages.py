#!/usr/bin/env python3
"""Stage only the completed benchmark report and its linked result artifacts."""
import argparse
from html.parser import HTMLParser
import importlib.util
import json
from pathlib import Path
import shutil
import hashlib
import tarfile
from urllib.parse import unquote, urlsplit

RUN = Path('reports/random20-seed20261015')
ARTIFACTS = (
    'summary.csv', 'summary.json', 'summary.md', 'selection.json', 'preflight.json',
    'postflight.json', 'requests.jsonl', 'resources.jsonl', 'manifest.json',
    'recovery.json', 'questions.jsonl', 'reconciliation.json', 'execution.json',
    'execution-history.jsonl', 'embedding-requests.jsonl', 'preflight-requests.jsonl',
    'preflight-embeddings.jsonl', 'models.json', 'preflight-models.json',
    'memory-events.json', 'retrieval.json', 'html-validation.json', 'presentation-timing-audit.json',
)


def stage_agentic(output):
    project = Path('agentic-search-only')
    target = output / project
    target.mkdir()
    html = (project / 'index.html').read_text().replace(
        '<script defer src="assets/dashboard.js">',
        '<script>window.BENCHMARK_PUBLISHED=true;</script><script defer src="assets/dashboard.js">')
    (target / 'index.html').write_text(html)
    shutil.copytree(project / 'assets', target / 'assets')
    shutil.copytree(project / 'docs', target / 'docs')
    (target / 'published').mkdir()
    for name in ('corpus-verification.json', 'corpus-verification.js'):
        shutil.copyfile(project / 'published' / name, target / 'published' / name)
    bundle = project / 'published/all100-report.tar.gz'
    manifest = json.loads((bundle.parent / 'manifest.json').read_text())
    with bundle.open('rb') as stream:
        if hashlib.file_digest(stream, 'sha256').hexdigest() != manifest['sha256']:
            raise ValueError('Agentic report archive checksum mismatch')
    analysis = target / 'analysis/all100'
    analysis.mkdir(parents=True)
    with tarfile.open(bundle, 'r:gz') as archive:
        members = archive.getmembers()
        if len(members) != manifest['files'] or sum(m.size for m in members) != manifest['expanded_bytes']:
            raise ValueError('Agentic report archive inventory mismatch')
        for member in members:
            relative = Path(member.name)
            if (not member.isfile() or relative.is_absolute() or '..' in relative.parts
                    or len(relative.parts) > 2 or relative.suffix not in {'.json', '.js', '.csv', '.md'}):
                raise ValueError(f'Unexpected report archive member: {member.name}')
            destination = analysis / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            with archive.extractfile(member) as source, destination.open('wb') as dest:
                shutil.copyfileobj(source, dest)
    dashboard = json.loads((analysis / 'dashboard.json').read_text())
    verified = json.loads((target / 'published/corpus-verification.json').read_text())
    dataset = dashboard['metadata']['dataset']
    if (not verified['independent_counts_match'] or
            (verified['rows'], verified['unique_document_ids'], verified['file_count'], verified['bytes']) !=
            (dataset['rows'], dataset['unique_filing_ids'], dataset['files'], dataset['compressed_bytes'])):
        raise ValueError('Verified local source counts differ from the recorded benchmark snapshot')
    if len(dashboard['cases']) != 400 or len(dashboard['questions']) != 100:
        raise ValueError('Agentic report must contain 100 questions and 400 planned cases')
    for case in dashboard['cases']:
        for key in ('detail_json', 'detail_js'):
            path = Path(case[key])
            if len(path.parts) != 2 or path.parts[0] != 'details' or not (analysis / path).is_file():
                raise ValueError(f'Missing agentic case export: {path}')


class Links(HTMLParser):
    def __init__(self):
        super().__init__()
        self.hrefs = []

    def handle_starttag(self, tag, attributes):
        if tag == 'a':
            self.hrefs.extend(value for key, value in attributes if key == 'href' and value)


def main(output):
    if output.exists() and any(output.iterdir()):
        raise ValueError('Choose an empty output directory; existing files are not deleted')
    output.mkdir(parents=True, exist_ok=True)
    # Standard-library-only rendering: no database connection or paid API calls.
    spec = importlib.util.spec_from_file_location('benchmark_report',
        Path(__file__).with_name('render-benchmark-report.py'))
    renderer = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(renderer)
    renderer.build(RUN)
    data = json.loads((RUN / 'report-data.json').read_text())
    if len(data['cells']) != 16 or any(c['completed'] != 20 or c['errors'] for c in data['cells']):
        raise ValueError('Refusing to publish incomplete benchmark results')
    shutil.copyfile(RUN / 'report.html', output / 'index.html')
    shutil.copyfile(RUN / 'report-data.json', output / 'report-data.json')
    for name in ARTIFACTS:
        shutil.copyfile(RUN / name, output / name)
    (output / 'results').mkdir()
    for item in data['details']:
        relative = Path(item['file'])
        if len(relative.parts) != 2 or relative.parts[0] != 'results' or relative.suffix != '.json':
            raise ValueError(f'Unexpected result path: {relative}')
        shutil.copyfile(RUN / relative, output / relative)
    stage_agentic(output)
    shutil.copytree(Path('assets'), output / 'assets')
    (output / '.nojekyll').touch()
    for page in (output / 'index.html', output / 'agentic-search-only/index.html'):
        parser = Links()
        parser.feed(page.read_text())
        for href in parser.hrefs:
            url = urlsplit(href)
            if url.scheme or url.netloc or not url.path:
                continue
            target = page.parent / unquote(url.path)
            if not target.is_file():
                raise ValueError(f'Broken report link in {page}: {href}')
    print(f'Pages ready: {output}/index.html; two benchmark tabs, 720 cases; all local links checked')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, default=Path('_site'))
    main(parser.parse_args().output)
