#!/usr/bin/env python3
"""Package audited report exports for Pages; excludes original runs and source data."""
import gzip
import hashlib
import json
from pathlib import Path
import tarfile

PROJECT = Path(__file__).resolve().parents[1]
SOURCE = PROJECT / 'analysis/all100'
TARGET = PROJECT / 'published/all100-report.tar.gz'


def main():
    paths = sorted(p for p in SOURCE.rglob('*') if p.is_file()
                   and p.suffix in {'.json', '.js', '.csv', '.md'}
                   and 'qa-downloads' not in p.parts)
    required = {'benchmark.json', 'dashboard-data.js', 'dashboard.json', 'audit.json', 'cases.csv'}
    if not required.issubset({p.name for p in paths}):
        raise ValueError('Build the audited analysis/all100 exports first')
    TARGET.parent.mkdir(parents=True, exist_ok=True)
    with TARGET.open('wb') as dest, gzip.GzipFile(fileobj=dest, mode='wb', filename='', mtime=0) as zipped:
        with tarfile.open(fileobj=zipped, mode='w|') as archive:
            for path in paths:
                info = archive.gettarinfo(str(path), arcname=str(path.relative_to(SOURCE)))
                info.uid = info.gid = info.mtime = 0
                info.uname = info.gname = ''
                with path.open('rb') as stream:
                    archive.addfile(info, stream)
    manifest = dict(files=len(paths), expanded_bytes=sum(p.stat().st_size for p in paths),
                    archive_bytes=TARGET.stat().st_size,
                    sha256=hashlib.file_digest(TARGET.open('rb'), 'sha256').hexdigest(),
                    source='analysis/all100', excludes=['source corpus', 'original runs', 'smoke runs', 'screenshots', 'QA downloads'])
    TARGET.with_name('manifest.json').write_text(json.dumps(manifest, indent=2)+'\n')
    print(json.dumps(manifest))


if __name__ == '__main__':
    main()
