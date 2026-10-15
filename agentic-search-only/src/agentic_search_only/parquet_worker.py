"""Isolated, read-only whole-corpus Parquet tools. Limits precede native imports."""
import json
from pathlib import Path
import resource
import sys
import time

from .source_read import constrain, read


def inspect(paths):
    import pyarrow.parquet as pq
    result = []
    for file_id, path in paths.items():
        with pq.ParquetFile(path, pre_buffer=False) as f:
            st = Path(path).stat()
            result.append(dict(file_id=file_id, filename=Path(path).name, path=path,
                size_bytes=st.st_size, mtime_ns=st.st_mtime_ns, rows=f.metadata.num_rows,
                row_groups=f.num_row_groups,
                schema=[dict(name=field.name, type=str(field.type)) for field in f.schema_arrow]))
    return {'files': result}


def filing_table(path, file_stem):
    """Decode identifiers first, then stream to exactly one full filing row."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    pa.set_cpu_count(1)
    pa.set_io_thread_count(1)
    if not isinstance(file_stem, str) or not file_stem:
        raise ValueError('file_stem must be a nonempty string')
    with pq.ParquetFile(path, pre_buffer=False) as parquet:
        target = None
        for group in range(parquet.num_row_groups):
            ids = parquet.read_row_group(group, columns=['file_stem'], use_threads=False).column(0)
            for offset, item in enumerate(ids):
                if item.as_py() == file_stem:
                    if target is not None:
                        raise ValueError('Duplicate filing ID in source')
                    target = group, offset
        if target is None:
            table = pa.Table.from_batches([], schema=parquet.schema_arrow)
        else:
            group, offset = target
            for index, batch in enumerate(parquet.iter_batches(batch_size=1, row_groups=[group], use_threads=False)):
                if index == offset:
                    table = pa.Table.from_batches([batch])
                    break
        return table.append_column('filename', pa.array([path] * table.num_rows, type=pa.string()))


def query(paths, sql, file_ids=None, file_stem=None):
    import duckdb
    # Some tool clients serialize an omitted optional string as an empty string.
    if file_stem == '':
        file_stem = None
    if not isinstance(sql, str) or len(sql) > 8000:
        raise ValueError('SQL must be a string of at most 8000 characters')
    ids = file_ids if file_ids is not None else list(paths)
    if not isinstance(ids, list) or not ids or any(i not in paths for i in ids):
        raise ValueError('file_ids must select approved file IDs')
    conn = duckdb.connect(config={'memory_limit': '128MB', 'threads': 1,
        'autoinstall_known_extensions': False, 'autoload_known_extensions': False,
        'preserve_insertion_order': False, 'max_temp_directory_size': '0B'})
    try:
        statements = conn.extract_statements(sql)
        if len(statements) != 1 or statements[0].type != duckdb.StatementType.SELECT:
            raise ValueError('Only a single read-only SELECT is allowed')
        if file_stem is not None:
            if len(ids) != 1:
                raise ValueError('file_stem scoping requires exactly one file_id')
            # DuckDB's vectorized raw Markdown scan can decode thousands of large
            # cells before applying filters. Bind a single streamed source row
            # instead; the agent still runs its own SQL over the full row schema.
            conn.register('filings', filing_table(paths[ids[0]], file_stem))
        else:
            conn.read_parquet([paths[i] for i in ids], filename=True, union_by_name=True).create_view('filings')
        conn.execute('SET allowed_paths = ?', [[paths[i] for i in ids]])
        conn.execute('SET enable_external_access=false')
        conn.execute('SET lock_configuration=true')
        cur = conn.execute(f'SELECT * FROM ({sql.rstrip().rstrip(";")}) AS bounded_result LIMIT 21')
        names = [v[0] for v in cur.description]
        rows, used, truncated = [], 0, False
        for i in range(21):
            row = cur.fetchone()
            if row is None:
                break
            if i == 20:
                truncated = True
                break
            item = {}
            for key, value in zip(names, row):
                if value is not None and not isinstance(value, (bool, int, float)):
                    value = str(value)
                    truncated |= len(value) > 6000
                    value = value[:6000]
                item[key] = value
            length = len(json.dumps(item, default=str))
            if used + length > 16000:
                truncated = True
                break
            rows.append(item)
            used += length
        return dict(rows=rows, truncated=truncated, file_ids=ids, scoped_file_stem=file_stem,
                    note='Text may be truncated; use read_filing for exact character offsets.')
    finally:
        conn.close()


def search(paths, file_id, company, form_type='', start_row=0, max_rows=200):
    """Bounded streaming fallback; never buffers a batch of full Markdown filings."""
    import re
    import pyarrow.compute as pc
    import pyarrow.parquet as pq
    if file_id not in paths:
        raise ValueError('Unknown file_id')
    if not isinstance(company, str) or not company.strip():
        raise ValueError('company must be a nonempty substring')
    if isinstance(start_row, bool) or not isinstance(start_row, int) or start_row < 0:
        raise ValueError('start_row must be nonnegative')
    if isinstance(max_rows, bool) or not isinstance(max_rows, int) or not 1 <= max_rows <= 500:
        raise ValueError('max_rows must be between 1 and 500')
    if not isinstance(form_type, str):
        raise ValueError('form_type must be a string')
    def field(header, label):
        m = re.search(r'\*\*' + re.escape(label) + r':?\*\*:?[ \t]*([^\n]*)', header)
        return m.group(1).strip() if m else ''
    matches, scanned = [], 0
    with pq.ParquetFile(paths[file_id], pre_buffer=False) as parquet:
        base = 0
        for group in range(parquet.num_row_groups):
            count = parquet.metadata.row_group(group).num_rows
            if base + count <= start_row:
                base += count
                continue
            # Identifier columns are small; loading one row group does not load Markdown.
            ids = parquet.read_row_group(group, columns=['file_stem'], use_threads=False).column(0)
            for offset, batch in enumerate(parquet.iter_batches(batch_size=1, row_groups=[group],
                    columns=['parsed_md'], use_threads=False)):
                row = base + offset
                if row < start_row:
                    continue
                scanned += 1
                header = pc.utf8_slice_codeunits(batch.column(0), start=0, stop=16000)[0].as_py() or ''
                issuer, form = field(header, 'COMPANY CONFORMED NAME'), field(header, 'CONFORMED SUBMISSION TYPE')
                if company.casefold() in issuer.casefold() and (not form_type or form.upper() == form_type.upper()):
                    matches.append(dict(file_id=file_id, file_stem=ids[offset].as_py(),
                        company=issuer, form_type=form, source_row=row))
                if len(matches) >= 5 or scanned >= max_rows:
                    return dict(rows=matches, scanned=scanned, next_row=row + 1,
                                complete=row + 1 >= parquet.metadata.num_rows)
            base += count
    return dict(rows=matches, scanned=scanned, next_row=None, complete=True)


def dispatch(paths, operation, arguments):
    if operation == 'describe_sources':
        return inspect(paths)
    if operation == 'duckdb_query':
        return query(paths, **arguments)
    if operation == 'search_filings':
        return search(paths, **arguments)
    if operation == 'read_filing':
        arguments = dict(arguments)
        file_id = arguments.pop('file_id')
        if file_id not in paths:
            raise ValueError('Unknown file_id')
        result = read(list(paths.values()), paths[file_id], **arguments)
        result['file_id'] = file_id
        return result
    raise ValueError('Unknown tool')


def main():
    constrain()
    started = time.perf_counter()
    request = json.load(sys.stdin)
    try:
        result = dispatch(request['paths'], request['operation'], request['arguments'])
    except Exception as error:
        result = dict(error=str(error)[:1000], error_type=type(error).__name__)
    usage = resource.getrusage(resource.RUSAGE_SELF)
    result['resources'] = dict(peak_rss_mib=usage.ru_maxrss / 1024,
        cpu_seconds=usage.ru_utime + usage.ru_stime, wall_seconds=time.perf_counter() - started,
        address_space_limit_mib=1536, cpu_limit_seconds=20, cpu_cores=1,
        duckdb_memory_limit_mib=128)
    print(json.dumps(result, default=str))


if __name__ == '__main__':
    main()
