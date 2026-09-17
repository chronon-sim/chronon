// SPDX-License-Identifier: MPL-2.0
#pragma once

#include <sys/resource.h>

#include <chrono>
#include <iomanip>
#include <iostream>

#include "ClockAllocationCount.hpp"
#include "chronon/Chronon.hpp"

namespace chronon::benchmark {

struct ClockWork : TickableUnit {
    struct Output {
        AsyncWritePort<uint64_t> port;
        uint64_t count = 0;
        Output(ClockWork* owner, size_t lane) : port(owner, "out" + std::to_string(lane)) {}
    };
    struct Input {
        AsyncReadPort<uint64_t> port;
        uint64_t count = 0, checksum = 0;
        Input(ClockWork* owner, size_t lane) : port(owner, "in" + std::to_string(lane)) {}
    };
    std::vector<std::unique_ptr<Output>> outputs;
    std::vector<std::unique_ptr<Input>> inputs;
    uint64_t work_digest = 1;
    unsigned work, activity;
    ClockWork(std::string name, unsigned work, unsigned activity)
        : TickableUnit(std::move(name)), work(work), activity(activity) {}
    auto& output() {
        outputs.push_back(std::make_unique<Output>(this, outputs.size()));
        return outputs.back()->port;
    }
    auto& input() {
        inputs.push_back(std::make_unique<Input>(this, inputs.size()));
        return inputs.back()->port;
    }
    void tick() override {
        for (unsigned i = 0; i < work; ++i) {
            work_digest = work_digest * 6364136223846793005ULL + i + 1;
            asm volatile("" : "+r"(work_digest));
        }
        for (auto& in : inputs) {
            if (auto packet = in->port.take()) {
                if (packet->data != in->count + 1) throw std::runtime_error("FIFO order mismatch");
                ++in->count;
                in->checksum += packet->data;
            }
            if (localCycle() % activity == 0) in->port.requestRead();
        }
        if (localCycle() % activity == 0)
            for (auto& out : outputs)
                if (out->port.send({out->count + 1, out->count + 1})) ++out->count;
    }
};

inline double clockCpuSeconds() {
    rusage usage{};
    getrusage(RUSAGE_SELF, &usage);
    return usage.ru_utime.tv_sec + usage.ru_utime.tv_usec / 1e6 + usage.ru_stime.tv_sec +
           usage.ru_stime.tv_usec / 1e6;
}

// Reuse the clock benchmark executable and its CSV runner. Fixed hardware work
// and full end-state digests make serial/static/dynamic and before/after runs
// comparable; initialization is reported separately from simulation throughput.
inline int runClockScaling(int argc, char** argv) {
    if (argc < 9) {
        std::cerr << "usage: multiclock_benchmark scaling STEPS THREADS PAIRS DOMAINS WORK "
                     "DYNAMIC SKEW [--lanes N --topology pairs|fanin|ring "
                     "--clocks staggered|coincident|coprime --activity N --segment N "
                     "--trace off|text|perfetto|both --output DIR --lossy 0|1 "
                     "--trace-capacity N --profile 0|1]\n";
        return 2;
    }
    const auto steps = std::stoull(argv[2]);
    const auto threads = std::stoull(argv[3]);
    const auto pairs = std::stoull(argv[4]);
    const auto domains = std::stoull(argv[5]);
    const auto work = std::stoull(argv[6]);
    const auto dynamic = std::stoull(argv[7]);
    const auto skew = std::stoull(argv[8]);
    if (!steps || steps > 1'000'000'000 || !threads || threads > 64 || !pairs || pairs > 512 ||
        domains < 2 || domains > 2 * pairs || work > 100'000 || dynamic > 1 || !skew || skew > 64)
        throw std::invalid_argument("scaling arguments outside supported bounds");
    size_t lanes = 1, activity = 1, segment = steps, trace_capacity = 4096;
    std::string topology = "pairs", clocks = "staggered", trace_mode = "off", output;
    bool lossy = false, profile = false;
    for (int i = 9; i < argc; i += 2) {
        if (i + 1 == argc) throw std::invalid_argument("option needs a value");
        const std::string key = argv[i], value = argv[i + 1];
        if (key == "--lanes")
            lanes = std::stoull(value);
        else if (key == "--activity")
            activity = std::stoull(value);
        else if (key == "--segment")
            segment = std::stoull(value);
        else if (key == "--topology")
            topology = value;
        else if (key == "--clocks")
            clocks = value;
        else if (key == "--trace")
            trace_mode = value;
        else if (key == "--output")
            output = value;
        else if (key == "--lossy" && (value == "0" || value == "1"))
            lossy = value == "1";
        else if (key == "--profile" && (value == "0" || value == "1"))
            profile = value == "1";
        else if (key == "--trace-capacity")
            trace_capacity = std::stoull(value);
        else
            throw std::invalid_argument("unknown scaling option: " + key);
    }
    if (!lanes || lanes > 64 || !activity || activity > 1024 || !segment ||
        (topology != "pairs" && topology != "fanin" && topology != "ring") ||
        (clocks != "staggered" && clocks != "coincident" && clocks != "coprime") ||
        (trace_mode != "off" && trace_mode != "text" && trace_mode != "perfetto" &&
         trace_mode != "both"))
        throw std::invalid_argument("invalid scaling options");
    const auto wall_begin = std::chrono::steady_clock::now();
    const auto cpu_begin = clockCpuSeconds();
    TickSimulationConfig config;
    config.num_threads = threads;
    config.enable_parallel = threads > 1;
    config.enable_dynamic_rebalance = dynamic;
    config.max_lookahead_cycles = 32;
    config.profile_clock_scheduler = profile;
    TickSimulation sim(config);
    for (size_t d = 0; d < domains; ++d)
        sim.addClockDomain(ClockDomain::fromHz(
            d + 1, "clock" + std::to_string(d),
            clocks == "coprime" ? 914'000'000 + 37'000'000 * d : 250'000'000ULL << (d % 4), 1,
            SimTime::picoseconds(clocks == "coincident" ? 0 : d * 37)));
    std::vector<ClockWork*> producers, consumers, nodes;
    for (size_t p = 0; p < pairs; ++p) {
        const unsigned cost = static_cast<unsigned>(work * (p == 0 ? skew : 1));
        producers.push_back(sim.createUnitInDomain<ClockWork>(
            (p * 2) % domains + 1, "producer" + std::to_string(p), cost, activity));
        consumers.push_back(sim.createUnitInDomain<ClockWork>(
            (p * 2 + 1) % domains + 1, "consumer" + std::to_string(p), cost, activity));
        nodes.push_back(producers.back());
        nodes.push_back(consumers.back());
    }
    std::vector<AsyncFifo<uint64_t>*> fifos;
    const auto connect = [&](ClockWork* source, ClockWork* sink) {
        for (size_t lane = 0; lane < lanes; ++lane)
            fifos.push_back(
                sim.connectAsyncFifo(fifos.size() + 1, source->output(), sink->input(), {16, 2}));
    };
    if (topology == "ring") {
        for (size_t n = 0; n < nodes.size(); ++n) connect(nodes[n], nodes[(n + 1) % nodes.size()]);
    } else {
        for (size_t p = 0; p < pairs; ++p)
            connect(producers[p], consumers[topology == "fanin" ? 0 : p]);
    }
    if (trace_mode != "off") {
        if (output.empty()) throw std::invalid_argument("trace requires --output");
        ClockTraceRecorder::Config trace;
        trace.output_dir = output;
        trace.run_id = "multiclock-scaling";
        trace.text = trace_mode != "perfetto";
        trace.perfetto = trace_mode != "text";
        trace.lossless = !lossy;
        trace.stream_capacity = trace_capacity;
        trace.drain_batch = std::min<size_t>(256, trace_capacity);
        sim.configureClockTrace(trace);
    }
    const auto begin = std::chrono::steady_clock::now();
    const auto allocations_begin = clockAllocations();
    const auto init_cpu_begin = clockCpuSeconds();
    sim.initialize();
    const auto initialized = std::chrono::steady_clock::now();
    const auto init_cpu_end = clockCpuSeconds();
    const auto allocations_initialized = clockAllocations();
    for (uint64_t done = 0; done < steps;) {
        const auto count = std::min<uint64_t>(segment, steps - done);
        if (sim.runClockEvents(count) != count) throw std::runtime_error("short scaling run");
        done += count;
    }
    const auto end = std::chrono::steady_clock::now();
    const auto run_cpu_end = clockCpuSeconds();
    const auto allocations_end = clockAllocations();
    sim.closeClockTrace();
    const auto closed = std::chrono::steady_clock::now();
    const auto cpu_end = clockCpuSeconds();
    ClockTraceRecorder::Stats stats;
    if (sim.clockTraceRecorder()) stats = sim.clockTraceRecorder()->stats();
    uint64_t ticks = 0, sent = 0, received = 0, checksum = 0, digest = 0;
    for (size_t p = 0; p < pairs; ++p) {
        ticks += producers[p]->localCycle() + consumers[p]->localCycle();

        digest ^= (p + 1) * (producers[p]->work_digest + 31 * consumers[p]->work_digest);
    }
    uint64_t fifo_digest = 0, lane_digest = 0;
    for (const auto* node : nodes) {
        for (const auto& out : node->outputs) {
            sent += out->count;
            lane_digest = lane_digest * 31 + out->count;
        }
        for (const auto& in : node->inputs) {
            received += in->count;
            checksum += in->checksum;
            lane_digest = lane_digest * 31 + in->count;
            lane_digest = lane_digest * 31 + in->checksum;
        }
    }
    for (const auto* fifo : fifos) {
        const auto state = fifo->diagnostics();
        for (const uint64_t value :
             {state.write_binary, state.read_binary, state.write_gray, state.read_gray,
              state.write_sync, state.read_sync, uint64_t(state.full), uint64_t(state.empty),
              uint64_t(state.output_valid), uint64_t(state.ram_occupancy), state.writes,
              state.reads})
            fifo_digest = fifo_digest * 31 + value;
    }
    rusage usage{};
    getrusage(RUSAGE_SELF, &usage);
    const double cpu = usage.ru_utime.tv_sec + usage.ru_utime.tv_usec / 1e6 +
                       usage.ru_stime.tv_sec + usage.ru_stime.tv_usec / 1e6;
    std::cout
        << std::setprecision(12)
        << "mode,steps,threads,pairs,domains,work,dynamic,skew,init_s,run_s,wall_s,cpu_s,"
           "maxrss_kib,unit_ticks,sent,received,checksum,work_digest,migrations,parallel,overflow,"
           "lanes,bridges,topology,clocks,activity,segment,trace,lossy,profile,lane_digest,fifo_"
           "digest,"
           "init_cpu_s,run_cpu_s,total_cpu_s,close_s,total_s,init_allocations,run_allocations,"
           "partition_ns,"
           "events,dropped,producer_stalls,producer_stall_ns,admission_retries,progress_stalls,"
           "progress_stall_ns,"
           "allocated_ingress_bytes,peak_ingress_bytes,allocated_staging_bytes,peak_staging_"
           "records,"
           "native_buffer_peak_bytes,file_bytes";
    const std::array<std::pair<const char*, uint64_t sender::ClockSchedulerProfile::*>, 16> fields{
        {{"sweeps", &sender::ClockSchedulerProfile::sweeps},
         {"idle_sweeps", &sender::ClockSchedulerProfile::idle_sweeps},
         {"retirement_ns", &sender::ClockSchedulerProfile::retirement_ns},
         {"admission_ns", &sender::ClockSchedulerProfile::admission_ns},
         {"actor_ns", &sender::ClockSchedulerProfile::actor_ns},
         {"tick_ns", &sender::ClockSchedulerProfile::tick_ns},
         {"bridge_ns", &sender::ClockSchedulerProfile::bridge_ns},
         {"wait_ns", &sender::ClockSchedulerProfile::wait_ns},
         {"cluster_polls", &sender::ClockSchedulerProfile::cluster_polls},
         {"bridge_polls", &sender::ClockSchedulerProfile::bridge_polls},
         {"cluster_ticks", &sender::ClockSchedulerProfile::cluster_ticks},
         {"bridge_commits", &sender::ClockSchedulerProfile::bridge_commits},
         {"allowance_waits", &sender::ClockSchedulerProfile::allowance_waits},
         {"dependency_waits", &sender::ClockSchedulerProfile::dependency_waits},
         {"completion_loads", &sender::ClockSchedulerProfile::completion_loads},
         {"coordinator_sweeps", &sender::ClockSchedulerProfile::sweeps}}};
    for (const auto& [name, field] : fields) {
        (void)field;
        std::cout << ",sample_" << name;
    }
    std::cout << '\n'
              << "scaling," << steps << ',' << threads << ',' << pairs << ',' << domains << ','
              << work << ',' << dynamic << ',' << skew << ','
              << std::chrono::duration<double>(initialized - begin).count() << ','
              << std::chrono::duration<double>(end - initialized).count() << ','
              << std::chrono::duration<double>(end - begin).count() << ',' << cpu << ','
              << usage.ru_maxrss << ',' << ticks << ',' << sent << ',' << received << ','
              << checksum << ',' << digest << ',' << sim.rebalanceCount() << ','
              << sim.useParallelExecution() << ',' << sim.totalTransportOverflowEvents() << ','
              << lanes << ',' << fifos.size() << ',' << topology << ',' << clocks << ',' << activity
              << ',' << segment << ',' << trace_mode << ',' << lossy << ',' << profile << ','
              << lane_digest << ',' << fifo_digest << ',' << init_cpu_end - init_cpu_begin << ','
              << run_cpu_end - init_cpu_end << ',' << cpu_end - cpu_begin << ','
              << std::chrono::duration<double>(closed - end).count() << ','
              << std::chrono::duration<double>(closed - wall_begin).count() << ','
              << allocations_initialized - allocations_begin << ','
              << allocations_end - allocations_initialized << ',' << sim.clockPartitionTimeNs()
              << ',' << stats.events << ',' << stats.dropped << ',' << stats.producer_stalls << ','
              << stats.producer_stall_ns << ',' << stats.admission_retries << ','
              << stats.progress_stalls << ',' << stats.progress_stall_ns << ','
              << stats.allocated_buffer_bytes << ',' << stats.peak_buffer_bytes << ','
              << stats.allocated_staging_bytes << ',' << stats.peak_staging_records << ','
              << stats.native_buffer_peak_bytes << ',' << stats.file_bytes;
    for (const auto& [name, field] : fields) {
        uint64_t total = 0;
        if (std::string_view(name) == "coordinator_sweeps") {
            if (!sim.clockSchedulerProfile().empty()) total = sim.clockSchedulerProfile()[0].sweeps;
        } else {
            for (const auto& sample : sim.clockSchedulerProfile()) total += sample.*field;
        }
        std::cout << ',' << total;
    }
    std::cout << '\n';
    return 0;
}

}  // namespace chronon::benchmark
