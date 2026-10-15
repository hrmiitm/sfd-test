"""OS resource limits and bounded filing reads; no vector-database dependencies."""
import os
import resource

MEMORY_BYTES = 1536 * 1024 * 1024
CPU_SECONDS = 20


def constrain():
    resource.setrlimit(resource.RLIMIT_AS, (MEMORY_BYTES, MEMORY_BYTES))
    resource.setrlimit(resource.RLIMIT_CPU, (CPU_SECONDS, CPU_SECONDS))
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    os.sched_setaffinity(0, {min(os.sched_getaffinity(0))})
    os.nice(10)


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
