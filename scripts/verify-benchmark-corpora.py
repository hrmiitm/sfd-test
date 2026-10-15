#!/usr/bin/env python3
"""Independently recount benchmark sources and reproduce the supplied filing-type scan."""
import argparse
from collections import Counter
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import time
import subprocess
import sys

import duckdb
import pyarrow.parquet as pq

USER_PATTERN=re.compile(r'CONFORMED SUBMISSION TYPE\*{0,2}\s*:\s*([A-Z0-9/-]+)',re.I)
FULL_PATTERN=re.compile(r'\*\*CONFORMED SUBMISSION TYPE:?\*\*:?[ \t]*([^\n]*)')
ACCESSION_PATTERN=re.compile(r'\*\*ACCESSION NUMBER:?\*\*:?[ \t]*([^\n]*)')


def page_prefixes(path,group):
    # Read one compressed page at a time. Decode only its string prefixes,
    # avoiding Arrow's large UTF-8 buffers for these unusually large groups.
    import struct
    import numpy as np
    import fastparquet
    from fastparquet import core, parquet_thrift
    original=core.read_plain
    def unpack(raw,count,utf=False):
        view=memoryview(raw).cast('B');offset=0;values=[]
        for _ in range(count):
            length=struct.unpack_from('<I',view,offset)[0];offset+=4
            if offset+length>len(view):raise ValueError('Invalid Parquet string length')
            prefix=bytes(view[offset:offset+min(length,20000)]).decode('utf-8',errors='ignore')[:5000]
            values.append(prefix);offset+=length
        return np.array(values,dtype=object)
    def read_plain(raw,type_,count,width=0,utf=False,stat=False):
        if type_==parquet_thrift.Type.BYTE_ARRAY and not stat:return unpack(raw,count,utf=utf)
        return original(raw,type_,count,width=width,utf=utf,stat=stat)
    core.unpack_byte_array=unpack;core.read_plain=read_plain
    parquet=fastparquet.ParquetFile(path)
    column=next(col for col in parquet.row_groups[group].columns if col.meta_data.path_in_schema==['parsed_md'])
    output=np.empty(parquet.row_groups[group].num_rows,dtype=object)
    with open(path,'rb') as stream:core.read_col(column,parquet.schema,stream,assign=output)
    return output


def scan_group(path,group):
    parquet=pq.ParquetFile(path,pre_buffer=False);local=Counter();full=Counter();ids=set();headers=set()
    prefixes=page_prefixes(path,group)
    stems=parquet.read_row_group(group,columns=['file_stem'],use_threads=False).column(0).to_pylist()
    rows=empty=mismatches=missing_header=0
    for text,stem in zip(prefixes,stems):
        prefix=text or ''
        match=USER_PATTERN.search(prefix);local[match.group(1).upper() if match else 'UNKNOWN']+=1
        match=FULL_PATTERN.search(prefix);full[match.group(1).strip().upper() if match else 'UNKNOWN']+=1
        match=ACCESSION_PATTERN.search(prefix);header=match.group(1).strip() if match else None
        if header and re.fullmatch(r'\d{10}-\d{2}-\d{6}',header):headers.add(header);mismatches+=header!=stem
        else:missing_header+=1
        if stem is not None:ids.add(stem)
        rows+=1;empty+=not bool(prefix.strip())
    if rows!=parquet.metadata.row_group(group).num_rows:raise ValueError('Group scan disagrees with footer')
    return dict(rows=rows,ids=sorted(ids),headers=sorted(headers),empty=empty,mismatches=mismatches,
                missing_header=missing_header,local=dict(local),full=dict(full))


def source_scan(source):
    started=time.monotonic();totals=Counter();forms=Counter();metadata_ids=set();header_ids=set();files=[]
    paths=sorted(source.glob('*.parquet'));cache=Path('/tmp/sfd-corpus-rowgroup-counts');cache.mkdir(exist_ok=True)
    for path in paths:
        print(f'Processing {path.name}...',flush=True)
        parquet=pq.ParquetFile(path,pre_buffer=False);local=Counter();full=Counter();ids=set();headers=set()
        rows=empty=mismatches=missing_header=0;last=time.monotonic();stat=path.stat()
        for group in range(parquet.num_row_groups):
            checkpoint=cache/f'{path.stem}.{stat.st_size}.{stat.st_mtime_ns}.{group}.json'
            if checkpoint.exists():value=json.loads(checkpoint.read_text())
            else:
                worker=subprocess.run([sys.executable,str(Path(__file__).resolve()),'--worker-file',str(path),'--row-group',str(group)],capture_output=True,text=True)
                if worker.returncode:raise RuntimeError(f'Group {path.name}/{group} failed ({worker.returncode}): {worker.stderr[-500:]}')
                value=json.loads(worker.stdout);checkpoint.write_text(worker.stdout)
            rows+=value['rows'];empty+=value['empty'];mismatches+=value['mismatches'];missing_header+=value['missing_header']
            ids.update(value['ids']);headers.update(value['headers']);local.update(value['local']);full.update(value['full'])
            if time.monotonic()-last>25:
                print(f'  {path.name}: {rows:,} / {parquet.metadata.num_rows:,} rows',flush=True);last=time.monotonic()
        if rows!=parquet.metadata.num_rows:raise ValueError('Physical row scan disagrees with footer')
        totals.update(local);forms.update(full);metadata_ids.update(ids);header_ids.update(headers)
        item=dict(filename=path.name,bytes=stat.st_size,footer_rows=parquet.metadata.num_rows,
                  text_rows_scanned=rows,unique_metadata_document_ids=len(ids),unique_valid_header_accessions=len(headers),
                  missing_or_invalid_header_accessions=missing_header,header_vs_metadata_id_mismatches=mismatches,
                  empty_prefix_rows=empty,supplied_regex_counts=dict(local.most_common()),full_label_counts=dict(full.most_common()))
        files.append(item);print(f'  {rows:,} documents processed; {len(ids):,} metadata IDs; {len(headers):,} header IDs',flush=True)
    return dict(scope='Only data/parsed/*.parquet in this checkout; first 5,000 characters of every text row, not a sampled set of rows.',
                files=files,file_count=len(files),compressed_bytes=sum(f['bytes'] for f in files),
                total_rows=sum(totals.values()),unique_metadata_document_ids=len(metadata_ids),unique_valid_header_accessions=len(header_ids),
                supplied_regex_counts=dict(totals.most_common()),full_label_counts=dict(forms.most_common()),
                regex_total_matches_footer_rows=sum(totals.values())==sum(f['footer_rows'] for f in files),
                header_scan_characters=5000,reader='fastparquet page decoder retaining only 5,000-character string prefixes; PyArrow metadata/ID cross-check',
                regex_notes='Supplied regex counts one row per iteration, including UNKNOWN. It truncates form labels at spaces (e.g. SCHEDULE 13G/A becomes SCHEDULE). Full-label counts preserve spaces. These per-type totals include duplicate IDs.',
                resource_notes='One isolated worker per row group releases page buffers; run under a whole-process-tree 2 GiB cap, no swap and one CPU core as documented in README.',
                seconds=time.monotonic()-started)


def indexed_scan(run):
    recorded=json.loads((run/'manifest.json').read_text())['config'];preflight=json.loads((run/'preflight.json').read_text())
    work=Path(recorded['work']);folders=[work]+[Path(p) for p in preflight['extensions']]
    connection=duckdb.connect(config={'threads':'1','memory_limit':'128MB'})
    paths=[str(p) for folder in folders for p in sorted((folder/'chunks').glob('*.parquet'))]
    sql='SELECT count(*) chunks, count(DISTINCT id) chunk_ids, count(DISTINCT accession) document_ids, count(DISTINCT (source_file,source_row)) source_records FROM read_parquet(?)'
    chunks,chunk_ids,documents,records=connection.execute(sql,[paths]).fetchone()
    py_ids=set();py_docs=set();py_records=set();py_chunks=0
    for path in paths:
        for batch in pq.ParquetFile(path,pre_buffer=False).iter_batches(batch_size=1024,columns=['id','accession','source_file','source_row'],use_threads=False):
            for row in batch.to_pylist():
                py_chunks+=1;py_ids.add(row['id']);py_docs.add(row['accession']);py_records.add((row['source_file'],row['source_row']))
    if (chunks,chunk_ids,documents,records)!=(py_chunks,len(py_ids),len(py_docs),len(py_records)):raise ValueError('Index SQL/Arrow counts disagree')
    if chunks!=chunk_ids or chunks!=preflight['expected_chunks']:raise ValueError('Chunk artifacts differ from recorded database snapshot')
    parts=[]
    for folder in folders:
        part=json.loads((folder/'prepared.json').read_text());parts.append(dict(workspace=str(folder),**part))
    return dict(scope='Recorded 20-question run: sample-1000 plus its recorded benchmark-additions extension.',
                unique_document_ids=documents,source_records=records,chunks_per_database=chunks,unique_chunk_ids=chunk_ids,
                chunks_per_database_match_recorded_preflight=True,independent_sql_and_arrow_match=True,parts=parts,sql=sql,
                source_parquet_access=dict(files=len(recorded['agentic']['sources']),compressed_bytes=sum(f['size'] for f in recorded['agentic']['sources']),
                    note='Agentic Search could additionally use these original source Parquet files; BM25/Vector/Hybrid searched the index.'),
                definitions=dict(document_ids='DISTINCT accession in indexed chunks; parsed filing accession, not company CIK.',
                                 source_records='DISTINCT (source_file,source_row), so repeated document IDs may occur.',
                                 chunks='Retrieval passages stored in each database, not documents.'))


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source',type=Path,default=Path('data/parsed'))
    parser.add_argument('--run',type=Path,default=Path('reports/random20-seed20261015'))
    parser.add_argument('--output',type=Path)
    parser.add_argument('--worker-file',type=Path)
    parser.add_argument('--row-group',type=int)
    args=parser.parse_args()
    if args.worker_file:
        print(json.dumps(scan_group(args.worker_file,args.row_group)));return
    if args.output is None:parser.error('--output is required')
    indexed=indexed_scan(args.run);print('Indexed sample:',json.dumps(indexed),flush=True)
    source=source_scan(args.source)
    # Third independent count: SQL reads only identifiers and footer counts.
    connection=duckdb.connect(config={'threads':'1','memory_limit':'128MB'})
    sql_rows,sql_ids=connection.execute('SELECT count(*),count(DISTINCT file_stem) FROM read_parquet(?)',[[str(p) for p in sorted(args.source.glob('*.parquet'))]]).fetchone()
    if (sql_rows,sql_ids)!=(source['total_rows'],source['unique_metadata_document_ids']):raise ValueError('Full scan and SQL disagree')
    result=dict(checked_utc=datetime.now(timezone.utc).isoformat(),indexed_20_question_corpus=indexed,source_100_question_corpus=source,independent_counts_agree=True)
    args.output.parent.mkdir(parents=True,exist_ok=True);args.output.write_text(json.dumps(result,indent=2)+'\n')
    print('FINAL',json.dumps({k:v for k,v in source.items() if k not in {'files','supplied_regex_counts','full_label_counts'}}),flush=True)


if __name__=='__main__':main()
