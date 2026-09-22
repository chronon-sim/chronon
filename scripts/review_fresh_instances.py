#!/usr/bin/env python3
"""Diagnostic four-way comparison across 24 fresh executable instances per label."""
import argparse
import collections
import hashlib
import importlib.util
import itertools
import json
import math
import os
from pathlib import Path
import platform
import random
import resource
import statistics
import subprocess
import sys
import time

LABELS = ('baseline', 'baseline_control', 'original', 'candidate')
SEEDS = {'execution': 149, 'creation': 182, 'slots': 237}
COMPARISONS = [('baseline', 'baseline_control'), ('baseline', 'original'),
               ('baseline', 'candidate'), ('original', 'candidate')]


def schedule(seed):
    """All 24 permutations, with each position balanced within every four trials."""
    rng = random.Random(seed)
    orbits = [(LABELS[0],) + rest for rest in itertools.permutations(LABELS[1:])]
    rng.shuffle(orbits)
    result = []
    for orbit in orbits:
        rotations = [orbit[i:] + orbit[:i] for i in range(4)]
        rng.shuffle(rotations)
        result.extend(rotations)
    assert set(result) == set(itertools.permutations(LABELS))
    for start in range(0, 24, 4):
        for label in LABELS:
            assert sorted(order.index(label) for order in result[start:start + 4]) == list(range(4))
    return result


def design():
    plans = {kind: schedule(seed) for kind, seed in SEEDS.items()}
    tables = {}
    for a, b in itertools.combinations(plans, 2):
        tables[f'{a}:{b}'] = {}
        for label in LABELS:
            counts = collections.Counter((x.index(label), y.index(label))
                                         for x, y in zip(plans[a], plans[b]))
            table = [[counts[i, j] for j in range(4)] for i in range(4)]
            assert max(map(max, table)) <= 3
            tables[f'{a}:{b}'][label] = table
    return plans, tables


def interval(ratios, block=1):
    rng = random.Random(149)
    medians = []
    for _ in range(10000):
        if block == 1:
            sample = rng.choices(ratios, k=len(ratios))
        else:
            sample = []
            while len(sample) < len(ratios):
                start = rng.randrange(len(ratios))
                sample.extend(ratios[(start + j) % len(ratios)] for j in range(block))
        medians.append(statistics.median(sample[:len(ratios)]))
    medians.sort()
    def percentile(q):
        index = (len(medians) - 1) * q
        lower = int(index)
        return medians[lower] + (medians[min(lower + 1, len(medians) - 1)] - medians[lower]) * (index - lower)
    return [percentile(.025), percentile(.975)]


def summarize(rows):
    pairs = {rep: {r['variant']: r for r in rows if r['rep'] == rep} for rep in range(24)}
    assert all(set(pair) == set(LABELS) for pair in pairs.values()) and len(rows) == 96
    result = []
    for left, right in COMPARISONS:
        entry = {'comparison': [left, right], 'repeats': 24, 'state_matches': True}
        for metric in ('wall_seconds', 'benchmark_seconds'):
            a = [pairs[r][left][metric] for r in range(24)]
            b = [pairs[r][right][metric] for r in range(24)]
            ratios = [y / x for x, y in zip(a, b)]
            entry[metric] = {'baseline_median': statistics.median(a), 'candidate_median': statistics.median(b),
                'paired_median_ratio': statistics.median(ratios), 'paired_ratios': ratios,
                'paired_median_ratio_bootstrap95': interval(ratios),
                'circular_block4_bootstrap95_sensitivity': interval(ratios, 4),
                'baseline_range': [min(a), max(a)], 'candidate_range': [min(b), max(b)],
                'positive_pairs': sum(x > 1 for x in ratios),
                'half_median_ratios': [statistics.median(ratios[:12]), statistics.median(ratios[12:])],
                'left_first_median_ratio': statistics.median(ratios[r] for r in range(24) if pairs[r][left]['order'] < pairs[r][right]['order']),
                'right_first_median_ratio': statistics.median(ratios[r] for r in range(24) if pairs[r][left]['order'] > pairs[r][right]['order'])}
        result.append(entry)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('baseline', 'original', 'candidate', 'scripts', 'output', 'instances'):
        parser.add_argument('--' + name, type=Path, required=True)
    parser.add_argument('--cpus', default='0,1')
    parser.add_argument('--case', help='Case name from check_api_performance.cases(2); default: broadcast-threads2')
    parser.add_argument('--cycle-scale', type=int,
                        help='Positive multiplier of CI cycles; default: 1 with --case, otherwise 5 (broadcast 250000)')
    args = parser.parse_args()
    cpus = list(map(int, args.cpus.split(',')))
    if len(cpus) != 2 or len(set(cpus)) != 2 or not set(cpus) <= os.sched_getaffinity(0):
        parser.error('--cpus requires two distinct allowed CPUs')
    output, instances = args.output.resolve(), args.instances.resolve()
    if instances.is_relative_to(output) or output.is_relative_to(instances):
        parser.error('--instances and --output must be separate directory trees')
    spec = importlib.util.spec_from_file_location('ci', args.scripts / 'check_api_performance.py')
    ci = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(ci)
    if args.cycle_scale is None:
        args.cycle_scale = 1 if args.case is not None else 5
    if args.cycle_scale < 1:
        parser.error('--cycle-scale must be a positive integer')
    args.case = args.case or 'broadcast-threads2'
    cases = {c['name']: c for c in ci.cases(2)}
    if args.case not in cases:
        parser.error(f'unknown --case {args.case!r}; available cases: {", ".join(cases)}')
    case = cases[args.case].copy()
    case['cycles'] *= args.cycle_scale
    name = ci.executable(case)
    sources = {label: getattr(args, 'baseline' if label == 'baseline_control' else label).resolve()
               for label in LABELS}
    payloads, source_meta = {}, {}
    for label, source in sources.items():
        binary = source / 'benchmark' / name
        payloads[label] = binary.read_bytes()
        info = binary.stat()
        source_meta[label] = {'path': str(binary), 'sha256': hashlib.sha256(payloads[label]).hexdigest(),
            'device': info.st_dev, 'inode': info.st_ino, 'size': info.st_size,
            'cache': (source / 'CMakeCache.txt').read_text()}
    assert payloads['baseline'] == payloads['baseline_control']
    plans, tables = design()
    output.mkdir(parents=True, exist_ok=False)
    instances.mkdir(parents=True, exist_ok=False)
    def write(name, value):
        (output / name).write_text(json.dumps(value, indent=2) + '\n')
    env = {**os.environ, 'CHRONON_BENCH_PIN_WORKERS': '1', 'CHRONON_BENCH_WARM_CPUS': '1'}
    write('metadata.json', {'args': {k: str(v) for k, v in vars(args).items()}, 'platform': platform.platform(),
        'lscpu': subprocess.check_output(['lscpu'], text=True), 'sources': source_meta, 'case': case,
        'cpus': cpus, 'thread_controls': {k: env[k] for k in ('CHRONON_BENCH_PIN_WORKERS', 'CHRONON_BENCH_WARM_CPUS')},
        'seeds': SEEDS, 'plans': plans, 'rank_contingencies': tables,
        'method': '24 fresh independent inodes per label; all permutations; each four trials position-balanced; no discarded samples',
        'design_note': 'Independent seeded rotation schedules; seeds selected without timing data to cap pairwise rank cells at 3.',
        'uncertainty': 'Paired bootstrap intervals are within-job diagnostics, not across-host or placement guarantees; block4 is sensitivity only.',
        'performance_enforced': False})
    rows, manifest, seen, oracle = [], [], set(), None
    try:
        path_length = None
        for rep in range(24):
            trial = {}
            for rank, label in enumerate(plans['creation'][rep]):
                slot = plans['slots'][rep].index(label)
                build = instances / f'trial-{rep:02d}' / f'slot-{slot}'
                binary = build / 'benchmark' / name
                binary.parent.mkdir(parents=True, exist_ok=False)
                with binary.open('xb') as handle:
                    handle.write(payloads[label])
                    handle.flush()
                    os.fsync(handle.fileno())
                binary.chmod(0o755)
                info = binary.stat()
                identity = (info.st_dev, info.st_ino)
                assert identity not in seen and info.st_nlink == 1
                assert identity not in {(v['device'], v['inode']) for v in source_meta.values()}
                seen.add(identity)
                digest = hashlib.sha256(binary.read_bytes()).hexdigest()
                assert digest == source_meta[label]['sha256']
                path_length = len(str(binary)) if path_length is None else path_length
                assert len(str(binary)) == path_length
                record = {'rep': rep, 'variant': label, 'creation_order': rank, 'slot': slot,
                    'path': str(binary), 'sha256': digest, 'device': info.st_dev, 'inode': info.st_ino, 'size': info.st_size}
                trial[label] = (build, record)
                manifest.append(record)
                write('instances.json', manifest)
            for order, label in enumerate(plans['execution'][rep]):
                build, record = trial[label]
                command = ci.command(build, case, cpus)
                log = output / f'trial-{rep:02d}-{label}.log'
                before = resource.getrusage(resource.RUSAGE_CHILDREN)
                timestamp = time.time()
                stdout, wall = ci.run_benchmark(command, env, log, 120)
                after = resource.getrusage(resource.RUSAGE_CHILDREN)
                seconds, state = ci.parse(case, stdout)
                oracle = state if oracle is None else oracle
                assert state == oracle and math.isfinite(wall) and wall > 0
                row = {**record, 'case': case['name'], 'order': order, 'command': command, 'timestamp': timestamp,
                    'finished_timestamp': time.time(), 'wall_seconds': wall, 'benchmark_seconds': seconds, 'state': state}
                for field, attr in (('user_seconds', 'ru_utime'), ('system_seconds', 'ru_stime'),
                    ('minor_faults', 'ru_minflt'), ('major_faults', 'ru_majflt'),
                    ('voluntary_switches', 'ru_nvcsw'), ('involuntary_switches', 'ru_nivcsw')):
                    row[field] = getattr(after, attr) - getattr(before, attr)
                rows.append(row)
                with (output / 'runs.jsonl').open('a') as handle:
                    handle.write(json.dumps(row) + '\n')
            print(f'Trial {rep + 1}/24 complete; state matches; all executable instances retained', flush=True)
        for record in manifest:
            binary = Path(record['path'])
            info = binary.stat()
            assert (info.st_dev, info.st_ino) == (record['device'], record['inode'])
            assert hashlib.sha256(binary.read_bytes()).hexdigest() == record['sha256']
        write('summary.json', summarize(rows))
        write('verdict.json', {'complete': True, 'state_matches': True, 'runs': len(rows), 'unique_inodes': len(seen),
                               'performance_enforced': False, 'performance_verdict': 'requires A/A and cross-VM analysis'})
    except Exception as error:
        write('failure.json', {'error': repr(error), 'completed_runs': len(rows), 'created_instances': len(manifest)})
        write('verdict.json', {'complete': False, 'performance_enforced': False})
        raise
    return 0


if __name__ == '__main__':
    sys.exit(main())
