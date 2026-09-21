"""Fixed-budget code-generation diagnosis; not an acceptance gate."""
import json
import random
import shutil
import subprocess
import sys
from pathlib import Path

root = Path.cwd()


def prepare():
    for variant in ("named", "outlined"):
        shutil.copytree(root / "candidate", root / variant,
                        ignore=shutil.ignore_patterns(".git", "build-perf"))
    path = root / "named/src/sender/port/InPort.hpp"
    source = path.read_text()
    source = source.replace(
        """current_cycle, [this, current_cycle]() noexcept {
                    return ingressCompleteForTick_(current_cycle);
                }""", "current_cycle, IngressCertificate{this, current_cycle}")
    source = source.replace(
        """[this, cycle]() noexcept {
                return ingressCompleteForTick_(cycle);
            }""", "IngressCertificate{this, cycle}")
    source = source.replace("    template <typename Queue, typename Visitor>\n",
        """    struct IngressCertificate {
        const InPort* port;
        uint64_t cycle;
        bool operator()() const noexcept { return port->ingressCompleteForTick_(cycle); }
    };

    template <typename Queue, typename Visitor>
""")
    assert source != path.read_text()
    path.write_text(source)

    path = root / "outlined/src/sender/port/MultiProducerQueueAdapter.hpp"
    source = path.read_text()
    before = """        while (shared_fifo_->canAdmit()) {
            auto admit = [this](T& data, uint64_t arrive_cycle, uint32_t) {
                shared_fifo_->push(data, arrive_cycle);
            };
            const bool admitted = consumeReadyFromLanesWithMetadata_(current_cycle, admit);
            if (!admitted) {
                shared_fifo_->ingress_exhausted = ingressComplete_(ingress_complete);
                break;
            }
        }"""
    assert source.count(before) == 1
    source = source.replace(before, """        if (shared_fifo_->canAdmit() && fillSharedFifo_(current_cycle)) {
            shared_fifo_->ingress_exhausted = ingressComplete_(ingress_complete);
        }""")
    anchor = "    // Evaluate a port certificate only after a failed scan or when reusing\n"
    assert source.count(anchor) == 1
    source = source.replace(anchor, """    // Keep lane selection and payload movement out of the certificate's
    // inline fast path. This work is shared by every certificate/visitor type.
    [[gnu::noinline]] bool fillSharedFifo_(uint64_t current_cycle) {
        while (shared_fifo_->canAdmit()) {
            auto admit = [this](T& data, uint64_t arrive_cycle, uint32_t) {
                shared_fifo_->push(data, arrive_cycle);
            };
            if (!consumeReadyFromLanesWithMetadata_(current_cycle, admit)) return true;
        }
        return false;
    }

""" + anchor)
    path.write_text(source)


def measure():
    sys.path.insert(0, str(root / "candidate/scripts"))
    from check_api_performance import confidence, parse, command
    results = root / "performance-results"
    results.mkdir(exist_ok=True)
    variants = ["baseline", "candidate", "named", "outlined", "baseline-control"]
    # Balance all positions over 30 of 31 blocks; shuffle the complete list
    # once, before observing any measurements.
    rng = random.Random("pr156-codegen-fixed-31")
    orders = []
    for _ in range(6):
        group = rng.sample(variants, len(variants))
        orders.extend(group[i:] + group[:i] for i in range(len(variants)))
    orders.append(rng.sample(variants, len(variants)))
    rng.shuffle(orders)
    for threads, cycles in [(1, 507604), (2, 933422)]:
        case = dict(name=f"port-threads{threads}", kind="representative",
                    profile="port", threads=threads, cycles=cycles)
        records = []
        reference = None
        for i, order in enumerate(orders):
            times = {}
            for variant in order:
                directory = "baseline" if variant == "baseline-control" else variant
                output = subprocess.check_output(
                    command(root / directory / "build-perf", case, [0, 1, 2]),
                    text=True, stderr=subprocess.STDOUT, timeout=120)
                (results / f"{case['name']}-{i}-{variant}.log").write_text(output)
                seconds, state = parse(case, output)
                if reference is not None:
                    assert state == reference, (variant, state, reference)
                reference = state
                times[variant] = seconds
            records.append(dict(order=order,seconds=times))
            (results / f"{case['name']}-samples.json").write_text(json.dumps(records, indent=2))
            if (i + 1) % 5 == 0:
                print(case["name"], i + 1, "blocks", flush=True)
        summary = dict(case=case, cpus=[0] if threads == 1 else [0, 1, 2], state=reference)
        for variant in variants[1:]:
            summary[variant] = confidence([r['seconds']['baseline']/r['seconds'][variant]
                                           for r in records])
        for variant in ("named", "outlined"):
            summary[variant + "_vs_candidate"] = confidence(
                [r['seconds']['candidate']/r['seconds'][variant] for r in records])
        (results / f"{case['name']}-summary.json").write_text(json.dumps(summary, indent=2))
        print(json.dumps(summary), flush=True)


if __name__ == "__main__":
    {"prepare": prepare, "measure": measure}[sys.argv[1]]()
