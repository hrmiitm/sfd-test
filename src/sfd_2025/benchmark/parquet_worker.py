"""One-shot isolated worker. Resource limits MUST precede native library imports."""
import json
import os
import resource
import sys
import time

MEMORY_BYTES = 1536 * 1024 * 1024
CPU_SECONDS = 20


def constrain():
    resource.setrlimit(resource.RLIMIT_AS, (MEMORY_BYTES, MEMORY_BYTES))
    resource.setrlimit(resource.RLIMIT_CPU, (CPU_SECONDS, CPU_SECONDS))
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    os.sched_setaffinity(0, {min(os.sched_getaffinity(0))})
    os.nice(10)


def query(paths, sql, source_path=None):
    import duckdb
    import pyarrow.parquet as pq
    if not isinstance(sql, str) or len(sql) > 8000:
        raise ValueError('SQL must be a string of at most 8000 characters')
    if source_path is not None:
        if source_path not in paths:
            raise ValueError('source_path must match an approved file')
        paths = [source_path]
    conn = duckdb.connect(config={'memory_limit': '128MB', 'threads': 1,
        'autoinstall_known_extensions': False, 'autoload_known_extensions': False,
        'preserve_insertion_order': False, 'max_temp_directory_size': '0B'})
    try:
        statements = conn.extract_statements(sql)
        if len(statements) != 1 or statements[0].type != duckdb.StatementType.SELECT:
            raise ValueError('Only a single read-only SELECT is allowed')
        # SQL scans only compact metadata. Full Markdown is read by the bounded
        # streaming search/read tools, never vectorized into SQL scan batches.
        conn.read_parquet(paths, filename=True).project(
            'file_stem, accession, year, month, char_count, filename AS source_path').create_view('filings')
        conn.execute('SET allowed_paths = ?', [paths])
        conn.execute('SET enable_external_access=false')
        conn.execute('SET lock_configuration=true')
        cursor = conn.execute(f'SELECT * FROM ({sql.rstrip().rstrip(";")}) AS agent_result LIMIT 21')
        names = [c[0] for c in cursor.description]
        rows, used, truncated = [], 0, False
        for index in range(21):
            row = cursor.fetchone()
            if row is None: break
            if index == 20:
                truncated = True; break
            item = {key: value if value is None or isinstance(value, (bool, int, float))
                    else str(value)[:4000] for key, value in zip(names, row)}
            truncated |= any(isinstance(value, str) and len(value) > 4000 for value in row)
            size = len(json.dumps(item, default=str))
            if used + size > 12000:
                truncated = True; break
            rows.append(item); used += size
        return dict(rows=rows, truncated=truncated)
    finally:
        conn.close()


def read(paths, source_path, file_stem, start=0, needle=''):
    import re
    import pyarrow.parquet as pq
    if source_path not in paths:
        raise ValueError('source_path must exactly match an approved source file')
    if isinstance(start, bool) or not isinstance(start, int) or start < 0:
        raise ValueError('start must be a nonnegative character offset')
    if not isinstance(file_stem, str) or not isinstance(needle, str):
        raise ValueError('file_stem and needle must be strings')
    # Locate the row using ONLY the small identifier column first.
    target = None
    with pq.ParquetFile(source_path, pre_buffer=False) as parquet:
        for group in range(parquet.num_row_groups):
            offset = 0
            for batch in parquet.iter_batches(batch_size=1024, row_groups=[group],
                                               columns=['file_stem'], use_threads=False):
                for index, accession in enumerate(batch.column(0).to_pylist()):
                    if accession == file_stem:
                        if target is not None: raise ValueError('Duplicate filing accession in source')
                        target = (group, offset + index)
                offset += len(batch)
        if target is None: raise ValueError('Filing not found in the selected source')
        group, offset = target
        # A Parquet row group cannot be randomly decoded by row: stream it one
        # filing at a time and stop at the selected row, under the process limit.
        for index, batch in enumerate(parquet.iter_batches(batch_size=1, row_groups=[group],
                                        columns=['parsed_md'], use_threads=False)):
            if index == offset:
                text = batch.column(0)[0].as_py() or ''
                break
    if needle:
        match = re.search(re.escape(needle), text[start:], re.IGNORECASE)
        if match is None: return dict(found=False, accession=file_stem, total_chars=len(text), needle=needle)
        start = max(start, start + match.start() - 1000)
    start = min(start, len(text))
    end = min(len(text), start + 12000)
    return dict(found=True, id=f'{file_stem}:{start}:{end}', accession=file_stem,
                section='Original filing Markdown', source_path=source_path,
                char_start=start, char_end=end, text=text[start:end], total_chars=len(text),
                next_start=end if end < len(text) else None)


def search(paths, source_path, company, form_type='', start_row=0):
    import re
    import pyarrow.compute as pc
    import pyarrow.parquet as pq
    if source_path not in paths: raise ValueError('Choose one approved source_path')
    if not isinstance(company, str) or not company.strip(): raise ValueError('company is required')
    if not isinstance(start_row, int) or isinstance(start_row, bool) or start_row < 0:
        raise ValueError('start_row must be nonnegative')
    matches, scanned = [], 0
    def field(header, label):
        match = re.search(r'\*\*' + re.escape(label) + r':?\*\*:?[ \t]*([^\n]*)', header)
        return match.group(1).strip() if match else ''
    with pq.ParquetFile(source_path, pre_buffer=False) as parquet:
        base = 0
        for group in range(parquet.num_row_groups):
            size = parquet.metadata.row_group(group).num_rows
            if base + size <= start_row:
                base += size; continue
            for offset, batch in enumerate(parquet.iter_batches(batch_size=1, row_groups=[group],
                columns=['parsed_md'], use_threads=False)):
                row = base + offset
                if row < start_row: continue
                scanned += 1
                header = pc.utf8_slice_codeunits(batch.column(0), start=0, stop=16000)[0].as_py() or ''
                issuer = field(header, 'COMPANY CONFORMED NAME')
                form = field(header, 'CONFORMED SUBMISSION TYPE')
                if company.casefold() in issuer.casefold() and (not form_type or form.upper() == form_type.upper()):
                    id_offset = 0
                    for ids in parquet.iter_batches(batch_size=1024, row_groups=[group],
                                                     columns=['file_stem'], use_threads=False):
                        if id_offset + len(ids) > offset:
                            filing_id = ids.column(0)[offset - id_offset].as_py()
                            break
                        id_offset += len(ids)
                    matches.append(dict(source_path=source_path, file_stem=filing_id,
                                        company=issuer, form_type=form, source_row=row))
                if len(matches) >= 1 or scanned >= 50000:
                    return dict(rows=matches, scanned=scanned, next_row=row+1, complete=False)
            base += size
    return dict(rows=matches, scanned=scanned, next_row=None, complete=True)


def main():
    constrain()
    started = time.perf_counter()
    request = json.load(sys.stdin)
    try:
        paths = [s['path'] for s in request['sources']]
        operation = {'query': query, 'read': read, 'search': search}[request['operation']]
        result = operation(paths, **request['arguments'])
    except Exception as error:
        result = dict(error=str(error)[:1000], error_type=type(error).__name__)
    usage = resource.getrusage(resource.RUSAGE_SELF)
    result['resources'] = dict(peak_rss_mib=usage.ru_maxrss / 1024,
        cpu_seconds=usage.ru_utime + usage.ru_stime, wall_seconds=time.perf_counter()-started,
        address_space_limit_mib=1536, cpu_limit_seconds=CPU_SECONDS, cpu_cores=1)
    print(json.dumps(result, default=str))


if __name__ == '__main__': main()
