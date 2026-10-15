#!/usr/bin/env python3
"""Run benchmark inside the caller's resource scope and record resource telemetry."""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess
import sys
import time

p=argparse.ArgumentParser()
p.add_argument('--output',type=Path,required=True)
p.add_argument('--concurrency',type=int,default=8)
a=p.parse_args()
assert json.loads((a.output/'preflight.json').read_text())['passed'], 'Preflight must pass first'
command=[sys.executable,'-m','sfd_2025.benchmark.run','--questions',str(a.output/'questions.jsonl'),
         '--output',str(a.output),'--concurrency',str(a.concurrency)]
started=datetime.now(timezone.utc).isoformat()
manifest=dict(started_utc=started,command=command,concurrency=a.concurrency,
              limits=dict(memory_gib=2,swap_bytes=0,cpu_cores_equivalent=1),status='running')
previous=a.output/'execution.json'
if previous.exists():
    with (a.output/'execution-history.jsonl').open('a') as history:
        history.write(previous.read_text().replace('\n','')+'\n')
(a.output/'execution.json').write_text(json.dumps(manifest,indent=2))
cgroup=Path('/sys/fs/cgroup')/Path('/proc/self/cgroup').read_text().strip().split('::')[-1].lstrip('/')
child=subprocess.Popen(command)
try:
    with (a.output/'resources.jsonl').open('a') as log:
        while True:
            record=dict(timestamp_utc=datetime.now(timezone.utc).isoformat())
            for name in ['memory.events','memory.current','memory.peak','memory.max','memory.swap.current','memory.swap.max','cpu.stat','cpu.max']:
                path=cgroup/name
                if path.exists():record[name]=path.read_text().strip()
            log.write(json.dumps(record)+'\n');log.flush()
            code=child.poll()
            if code is not None:break
            time.sleep(5)
finally:
    if child.poll() is None:
        child.terminate();child.wait()
    manifest.update(finished_utc=datetime.now(timezone.utc).isoformat(),returncode=child.returncode,
                    status='complete' if child.returncode==0 else 'incomplete')
    (a.output/'execution.json').write_text(json.dumps(manifest,indent=2))
sys.exit(child.returncode)
