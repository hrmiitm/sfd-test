# Published 100-question report

`all100-report.tar.gz` contains the derived, audited exports for the completed
100-question / 400-case benchmark: summaries, question comparisons, per-case tool
and API traces, judge scores, CSV, full formatted JSON, and preservation checks.
The archive is compressed to avoid committing repeated JSON/JavaScript exports
as hundreds of large loose files. `manifest.json` records its SHA-256 and size.

The archive excludes the source corpus, original run directory, smoke runs,
local environments, screenshots and temporary browser downloads.

From the repository root, `python3 scripts/build-pages.py --output _site` builds
both linked benchmark tabs and expands this report into
`_site/agentic-search-only/analysis/all100/`. The build uses only the Python
standard library, reads no database, and makes no model/API requests. Original
request-file links are marked as local on the deployed site; complete sent
contexts, returned evidence and API details remain available in each case export.

To replace this archive after a new local analysis, run
`python3 agentic-search-only/scripts/package-report.py`. This only reads the
existing `analysis/all100` exports; it never edits original results.
