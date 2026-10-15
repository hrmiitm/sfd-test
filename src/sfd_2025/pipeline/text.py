"""Deterministic source-preserving Markdown segmentation."""
import hashlib
import re
import uuid

import tiktoken

ENC = tiktoken.get_encoding('cl100k_base')


def tokens(text):
    return len(ENC.encode(text, disallowed_special=()))


def field(text, label):
    match = re.search(r'\*\*' + re.escape(label) + r':?\*\*:?[ \t]*([^\n]*)', text)
    return match.group(1).strip() if match else ''


def chunks(row, source, source_row, budget=700, max_chars=None):
    text = row.get('parsed_md') or ''
    accession = field(text, 'ACCESSION NUMBER') or row['file_stem']
    cik = field(text, 'CENTRAL INDEX KEY') or row['accession']
    digest = hashlib.sha256(text.encode()).hexdigest()
    metadata = dict(accession=accession, cik=cik,
                    company=field(text, 'COMPANY CONFORMED NAME'),
                    form_type=field(text, 'CONFORMED SUBMISSION TYPE'),
                    filed_date=field(text, 'FILED AS OF DATE'),
                    year=row['year'], month=row['month'],
                    source_file=source, source_row=source_row,
                    source_md5=row.get('md5') or '', content_sha256=digest,
                    has_ocr=bool(row.get('has_ocr')), source_format=row.get('source_format') or '')
    # Sections retain exact character offsets into the original Markdown.
    headings = list(re.finditer(r'(?m)^#{1,6}\s+.+$|^(?:\*\*)?ITEM\s+\d+[A-Z]?\.[^\n]*', text, re.I))
    boundaries = sorted(set([0, len(text)] + [h.start() for h in headings]))
    titles = {h.start(): h.group().strip('# *') for h in headings}
    for section_no, (start, end) in enumerate(zip(boundaries, boundaries[1:])):
        title = titles.get(start, 'Preamble')
        # Paragraphs and contiguous Markdown table rows stay together when they fit.
        blocks = list(re.finditer(r'[^\n]+(?:\n(?!\s*\n)[^\n]+)*', text[start:end]))
        spans = []
        for block in blocks:
            a, b = start + block.start(), start + block.end()
            if tokens(text[a:b]) <= budget and (max_chars is None or b-a <= max_chars):
                spans.append((a, b)); continue
            # Oversized blocks split on lines; oversized individual rows split on
            # Unicode character boundaries, never dropping or corrupting text.
            for line in re.finditer(r'[^\n]+\n?', text[a:b]):
                lo, stop = a + line.start(), a + line.end()
                while lo < stop:
                    left, right = lo + 1, min(stop, lo+max_chars) if max_chars else stop
                    while left < right:
                        mid = (left + right + 1) // 2
                        if tokens(text[lo:mid]) <= budget: left = mid
                        else: right = mid - 1
                    spans.append((lo, left)); lo = left
        merged = []
        for a, b in spans:
            if merged and (max_chars is None or b-merged[-1][0] <= max_chars) and tokens(text[merged[-1][0]:b]) <= budget:
                merged[-1] = (merged[-1][0], b)
            else: merged.append((a, b))
        for index, (a, b) in enumerate(merged):
            body = text[a:b]
            if not body.strip(): continue
            key = f'{source}:{source_row}:{digest}:{section_no}:{a}:{b}'
            yield dict(metadata, id=str(uuid.uuid5(uuid.NAMESPACE_URL, key)),
                       section=title, section_index=section_no, chunk_index=index,
                       char_start=a, char_end=b, text=body, token_count=tokens(body),
                       contains_table=bool(re.search(r'(?m)^\s*\|', body)))
