"""Fixed-budget wall-time diagnosis; separate from the informational PR check."""
import argparse
import copy
import csv
import hashlib
import io
import itertools
import json
import os
from pathlib import Path
import random
import resource
import statistics
import subprocess
import sys
import time

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('mode', choices=('measure', 'profile', 'matrix'))
parser.add_argument('baseline', type=Path)
parser.add_argument('candidate', type=Path)
parser.add_argument('output', type=Path)
parser.add_argument('--perf', default='perf')
parser.add_argument('--blocks', type=int, default=24)
parser.add_argument('--scripts', type=Path)
args = parser.parse_args()
args.output.mkdir(parents=True, exist_ok=True)
sys.path.insert(0, str(args.scripts or args.candidate.resolve().parent / 'scripts'))
from check_api_performance import cases, command, parse, executable, physical_cpus
builds = {'baseline': args.baseline.resolve(), 'candidate': args.candidate.resolve(),
          'control': args.baseline.resolve()}
cpus = physical_cpus(limit=None)
assert len(cpus) >= 16, cpus
env = {**os.environ, 'CHRONON_BENCH_PIN_WORKERS': '1', 'CHRONON_BENCH_WARM_CPUS': '1'}
matrix = cases(2)
floor = next(c for c in matrix if c['name'] == 'single-clock-floor')
clock = next(c for c in matrix if c['name'] == 'clock1-threads2-dynamic1-poll64')
static = copy.deepcopy(clock)
static['args'][5] = 0
static['name'] = 'clock1-threads2-dynamic0-poll64'
modes = {'floor': (floor, cpus[14:16]), 'clock-2cpu': (clock, cpus[8:10]),
         'clock-3cpu': (clock, cpus[8:11]), 'clock-static-2cpu': (static, cpus[8:10])}


def write(name, value):
    (args.output / f'{name}.json').write_text(json.dumps(value, indent=2) + '\n')


def one(case, mask, variant, log):
    before = resource.getrusage(resource.RUSAGE_CHILDREN)
    start = time.monotonic()
    p = subprocess.run(command(builds[variant], case, mask), env=env, text=True,
                       stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=90)
    wall = time.monotonic() - start
    after = resource.getrusage(resource.RUSAGE_CHILDREN)
    log.write_text(p.stdout)
    if p.returncode:
        raise RuntimeError(f'{variant} failed with {p.returncode}: {log}')
    seconds, state = parse(case, p.stdout)
    csv_row = next(csv.DictReader(io.StringIO('\n'.join(p.stdout.strip().splitlines()[-2:]))))
    return {'wall': wall, 'simulation': seconds, 'state': state, 'csv': csv_row,
            'cpu': after.ru_utime + after.ru_stime - before.ru_utime - before.ru_stime,
            'voluntary': after.ru_nvcsw - before.ru_nvcsw,
            'involuntary': after.ru_nivcsw - before.ru_nivcsw}


def describe(values):
    quartiles = statistics.quantiles(values, n=4)
    return dict(n=len(values), median=statistics.median(values), q1=quartiles[0],
                q3=quartiles[2], minimum=min(values), maximum=max(values))


def summarize(records):
    summary = {}
    for mode, rows in records.items():
        summary[mode] = {'absolute': {}, 'percent_change': {}}
        for variant in builds:
            summary[mode]['absolute'][variant] = {
                metric: describe([r['runs'][variant][metric] for r in rows])
                for metric in ('wall', 'simulation', 'cpu', 'voluntary', 'involuntary')}
        for variant in ('candidate', 'control'):
            summary[mode]['percent_change'][variant] = {
                metric: describe([(r['runs'][variant][metric] / r['runs']['baseline'][metric] - 1) * 100
                                  for r in rows]) for metric in ('wall', 'simulation', 'cpu')}
    return summary


def measure():
    orders = list(itertools.permutations(builds)) * ((args.blocks + 5) // 6)
    random.Random('pr156-wall-time-24').shuffle(orders)
    records = {mode: [] for mode in modes}
    states = {}
    for block in range(args.blocks):
        mode_order = list(modes)
        random.Random(f'pr156-wall-mode-{block}').shuffle(mode_order)
        for mode in mode_order:
            case, mask = modes[mode]
            row = {'block': block, 'order': orders[block], 'cpus': mask, 'runs': {}}
            for variant in orders[block]:
                result = one(case, mask, variant, args.output / f'{mode}-{block}-{variant}.log')
                if mode in states:
                    assert result['state'] == states[mode], (mode, variant, result['state'], states[mode])
                states[mode] = result['state']
                row['runs'][variant] = result
            records[mode].append(row)
            write('isolated-samples', records)
        print(f'Completed isolated block {block+1}/{args.blocks}', flush=True)
    write('isolated-summary', summarize(records))
    write('binaries', {variant: {executable(case): hashlib.sha256(
        (build / 'benchmark' / executable(case)).read_bytes()).hexdigest()
        for case in (floor, clock)} for variant, build in builds.items()})


def profile():
    records = []
    for mode, (original, mask) in modes.items():
        case = copy.deepcopy(original)
        case['cycles'] *= 5
        for variant in ('baseline', 'candidate'):
            argv = command(builds[variant], case, mask)
            stem = args.output / f'{mode}-{variant}'
            stat = subprocess.run([args.perf, 'stat', '-x', ';', '-e',
                'task-clock,context-switches,cpu-migrations,cycles,instructions,branches,branch-misses',
                '-o', str(stem)+'.stat', '--', *argv], env=env, text=True, capture_output=True, timeout=90)
            Path(str(stem)+'.stat.log').write_text(stat.stdout + '\n' + stat.stderr)
            record = subprocess.run([args.perf, 'record', '-o', str(stem)+'.data', '-F', '499',
                '-e', 'cpu-clock', '--call-graph', 'dwarf,8192', '--', *argv],
                env=env, text=True, capture_output=True, timeout=90)
            Path(str(stem)+'.record.log').write_text(record.stdout+'\n'+record.stderr)
            if record.returncode == 0:
                for sort in ('dso,symbol', 'comm,pid,tid,symbol'):
                    report = subprocess.run([args.perf,'report','-i',str(stem)+'.data',
                        '--stdio','--no-children','--percent-limit','0.5','--sort',sort],
                        text=True,capture_output=True,timeout=60)
                    Path(str(stem)+'.'+sort.replace(',','-')+'.txt').write_text(report.stdout+report.stderr)
            records.append({'mode':mode,'variant':variant,'stat_exit':stat.returncode,
                            'record_exit':record.returncode,'cpus':mask,'cycles':case['cycles']})
            write('profiling-status',records)
            print(records[-1],flush=True)


def concurrent():
    summaries = []
    for attempt in range(8):
        out = args.output / f'matrix-{attempt}'
        head = args.candidate.resolve().parent
        argv = [sys.executable,str(head/'scripts/run_api_performance.py'),str(args.baseline.resolve()),
                str(args.candidate.resolve()),str(out),'--base-sha','456f67a2cbd4d22b3aef715509a32639d424febe',
                '--head-sha','9c222a16eb509726f3ab09d7630dbf197fe107eb','--jobs','8','--workers','2','--timeout','600']
        start = time.monotonic()
        proc = subprocess.run(argv,env=os.environ,text=True,capture_output=True,timeout=630)
        (args.output/f'matrix-{attempt}.log').write_text(proc.stdout+'\n'+proc.stderr)
        assert proc.returncode==0, (attempt,proc.returncode)
        rows=json.loads((out/'summary.json').read_text())
        summaries.append({'attempt':attempt,'wall':time.monotonic()-start,'cases':{
            row['case']['name']:row for row in rows if row['case']['name'] in (floor['name'],clock['name'])}})
        write('matrix-summaries',summaries)
        print('Concurrent matrix',attempt+1,'completed',flush=True)


{'measure':measure,'profile':profile,'matrix':concurrent}[args.mode]()
