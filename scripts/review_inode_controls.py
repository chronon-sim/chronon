#!/usr/bin/env python3
import hashlib,json,os,shutil,sys
from pathlib import Path
source=Path(sys.argv[1]).resolve()
name='chronon_representative_workload_benchmark'
source_binary=source/'benchmark'/name
expected=hashlib.sha256(source_binary.read_bytes()).hexdigest()
records={}
for alias in ('aa-first','aa-other','aa-links'):
    build=Path(alias)/'build-perf'
    (build/'benchmark').mkdir(parents=True,exist_ok=False)
    shutil.copy2(source/'CMakeCache.txt',build/'CMakeCache.txt')
    binary=build/'benchmark'/name
    if alias=='aa-links':
        os.link(Path('aa-first/build-perf/benchmark')/name,binary)
    else:
        shutil.copy2(source_binary,binary)
    stat=binary.stat()
    digest=hashlib.sha256(binary.read_bytes()).hexdigest()
    assert digest==expected
    records[alias]={'path':str(binary.resolve()),'sha256':digest,'device':stat.st_dev,'inode':stat.st_ino}
assert records['aa-first']['inode']!=records['aa-other']['inode']
assert records['aa-first']['inode']==records['aa-links']['inode']
assert len({len(v['path']) for v in records.values()})==1
Path(sys.argv[2]).write_text(json.dumps(records,indent=2)+'\n')
