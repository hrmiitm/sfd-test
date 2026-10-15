#!/usr/bin/env python3
"""Stage only the completed benchmark report and its linked result artifacts."""
import argparse
from html.parser import HTMLParser
import importlib.util
import json
from pathlib import Path
import shutil
from urllib.parse import unquote, urlsplit

RUN = Path('reports/random20-seed20261015')
ARTIFACTS = (
    'summary.csv', 'summary.json', 'summary.md', 'selection.json', 'preflight.json',
    'postflight.json', 'requests.jsonl', 'resources.jsonl', 'manifest.json',
    'recovery.json', 'questions.jsonl', 'reconciliation.json', 'execution.json',
    'execution-history.jsonl', 'embedding-requests.jsonl', 'preflight-requests.jsonl',
    'preflight-embeddings.jsonl', 'models.json', 'preflight-models.json',
    'memory-events.json', 'retrieval.json', 'html-validation.json',
)


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
    (output / '.nojekyll').touch()
    parser = Links()
    parser.feed((output / 'index.html').read_text())
    for href in parser.hrefs:
        url = urlsplit(href)
        if url.scheme or url.netloc or not url.path:
            continue
        target = output / unquote(url.path)
        if not target.is_file():
            raise ValueError(f'Broken report link: {href}')
    print(f'Pages ready: {output}/index.html; 16 comparisons, 320 results; all local links checked')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, default=Path('_site'))
    main(parser.parse_args().output)
