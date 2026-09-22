#!/usr/bin/env python3
"""Repeat the unmodified CI workloads in balanced, sequential paired order."""
import argparse
import csv
import hashlib
import importlib.util
import itertools
import io
import json
import os
from pathlib import Path
import platform
import random
import resource
import statistics
import subprocess
import time

p = argparse.ArgumentParser()
p.add_argument('--scripts', default='/home/changyun/projects/chronon_pub/scripts')
p.add_argument('--baseline', type=Path, required=True)
p.add_argument('--candidate', type=Path, required=True)
p.add_argument('--original', type=Path)
p.add_argument('--output', type=Path, required=True)
p.add_argument('--cases', default='all')
p.add_argument('--repeats', type=int, default=16)
p.add_argument('--cpus', default='4,6')
p.add_argument('--cycle-scale', type=int, default=1)
p.add_argument('--thread-controls', choices=('on','off'), default='on')
args = p.parse_args()
spec = importlib.util.spec_from_file_location('ci', Path(args.scripts)/'check_api_performance.py')
ci = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ci)
cpus = list(map(int,args.cpus.split(',')))
matrix = ci.cases(2)
if args.cases != 'all':
    names = args.cases.split(',')
    matrix = [c for c in matrix if c['name'] in names]
    assert len(matrix)==len(names), names
for c in matrix:
    c['cycles'] *= args.cycle_scale
builds = {'baseline':args.baseline.resolve(), 'candidate':args.candidate.resolve()}
if args.original:
    builds = {'baseline':args.baseline.resolve(), 'original':args.original.resolve(),
              'candidate':args.candidate.resolve()}
args.output.mkdir(parents=True, exist_ok=False)
meta = {'args':{k:str(v) if isinstance(v,Path) else v for k,v in vars(args).items()},
        'method':'sequential fresh processes, same CPU pair, balanced permutations; no discarded samples',
        'platform':platform.platform(),'lscpu':subprocess.check_output(['lscpu'],text=True),
        'case_definitions':matrix,'builds':{}}
for v,b in builds.items():
    meta['builds'][v]={'path':str(b),'cache':(b/'CMakeCache.txt').read_text(),
        'sha256':{n:hashlib.sha256((b/'benchmark'/n).read_bytes()).hexdigest() for n in {ci.executable(c) for c in matrix}}}
(args.output/'metadata.json').write_text(json.dumps(meta,indent=2)+'\n')
env = dict(os.environ)
for key in ('CHRONON_BENCH_PIN_WORKERS','CHRONON_BENCH_WARM_CPUS'):
    env.pop(key, None)
    if args.thread_controls == 'on': env[key]='1'
allrows=[]
oracles={}
for rep in range(args.repeats):
    order_cases=list(matrix)
    random.Random(149+rep).shuffle(order_cases)
    for case in order_cases:
        orders=list(itertools.permutations(builds))
        order=orders[rep%len(orders)]
        pair={}
        for variant in order:
            cmd=ci.command(builds[variant],case,cpus)
            before=resource.getrusage(resource.RUSAGE_CHILDREN)
            started=time.perf_counter()
            proc=subprocess.run(cmd,env=env,text=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,timeout=120)
            wall=time.perf_counter()-started
            after=resource.getrusage(resource.RUSAGE_CHILDREN)
            log=f'{case["name"]}-{rep:02d}-{variant}.log'
            (args.output/log).write_text(proc.stdout)
            assert proc.returncode==0,(cmd,proc.returncode,log)
            sim,state=ci.parse(case,proc.stdout)
            assert state==oracles.setdefault(case['name'],state),(case['name'],state,oracles[case['name']])
            row={'case':case['name'],'rep':rep,'variant':variant,'order':order.index(variant),
                 'wall_seconds':wall,'benchmark_seconds':sim,'state':state,'command':cmd,
                 'user_seconds':after.ru_utime-before.ru_utime,'system_seconds':after.ru_stime-before.ru_stime,
                 'minor_faults':after.ru_minflt-before.ru_minflt,
                 'major_faults':after.ru_majflt-before.ru_majflt,
                 'voluntary_switches':after.ru_nvcsw-before.ru_nvcsw,
                 'involuntary_switches':after.ru_nivcsw-before.ru_nivcsw,'timestamp':time.time()}
            if case['kind']=='scheduler':
                row['scheduler_metrics']=next(csv.DictReader(io.StringIO('\n'.join(proc.stdout.strip().splitlines()[-2:]))))
            with (args.output/'runs.jsonl').open('a') as f:f.write(json.dumps(row)+'\n')
            allrows.append(row); pair[variant]=row
        print(f'{rep+1}/{args.repeats} {case["name"]}: '+', '.join(f'{v}={pair[v]["wall_seconds"]:.6f}' for v in builds),flush=True)

def pct(values,q):
    v=sorted(values); pos=(len(v)-1)*q; i=int(pos)
    return v[i]+(v[min(i+1,len(v)-1)]-v[i])*(pos-i)
summary=[]
for case in matrix:
    rows={v:[r for r in allrows if r['case']==case['name'] and r['variant']==v] for v in builds}
    comparisons=[('baseline','candidate')]
    if args.original: comparisons += [('baseline','original'),('original','candidate')]
    for left,right in comparisons:
        entry={'case':case['name'],'repeats':args.repeats,'state_matches':True,'comparison':[left,right]}
        for key in ('wall_seconds','benchmark_seconds'):
            a=[r[key] for r in rows[left]]; b=[r[key] for r in rows[right]]
            ratio=[y/x for x,y in zip(a,b)]; rng=random.Random(149)
            bootstrap=[statistics.median(rng.choices(ratio,k=len(ratio))) for _ in range(10000)]
            entry[key]={'baseline_median':statistics.median(a),'candidate_median':statistics.median(b),
                        'ratio_of_medians':statistics.median(b)/statistics.median(a),
                        'paired_median_ratio':statistics.median(ratio),
                        'paired_median_ratio_bootstrap95':[pct(bootstrap,.025),pct(bootstrap,.975)],
                        'baseline_range':[min(a),max(a)],'candidate_range':[min(b),max(b)]}
        summary.append(entry)
(args.output/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
for s in summary:
    w=s['wall_seconds']; lo,hi=w['paired_median_ratio_bootstrap95']
    print(f"SUMMARY {s['case']} {s['comparison']}: median {w['baseline_median']:.6f} -> {w['candidate_median']:.6f}; paired {(w['paired_median_ratio']-1)*100:+.2f}% CI [{(lo-1)*100:+.2f}%,{(hi-1)*100:+.2f}%]",flush=True)
