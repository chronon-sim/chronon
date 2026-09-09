// SPDX-License-Identifier: MPL-2.0
#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <thread>

#include "../sender/clock_reference.hpp"
#include "observe/ClockTraceRecorder.hpp"

using namespace chronon;
using namespace chronon::observe;
int main(int argc, char** argv) {
    auto root =
        argc > 1
            ? std::filesystem::path(argv[1])
            : std::filesystem::temp_directory_path() /
                  ("chronon-clock-recorder-" +
                   std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    const bool keep = argc > 1;
    for (unsigned variant = 0; variant < 4; ++variant) {
        ClockTraceRecorder::Config config;
        config.output_dir = root / ("workers-" + std::to_string(variant));
        config.stream_capacity = 128;
        config.drain_batch = 1 + variant * 31;
        config.reverse_drain = variant & 1;
        config.perfetto_options.compress = variant & 2;
        config.perfetto_options.checkpoint_interval_packets = 13;
        ClockTraceRecorder recorder(config);
        auto sm = ClockDomain::fromHz(1, "sm", 914'000'000);
        auto lts = ClockDomain::fromHz(2, "lts", 1'326'000'000, 1, SimTime::picoseconds(137));
        ClockTraceStream* streams[4];
        for (unsigned i = 0; i < 4; ++i) {
            streams[i] = recorder.addStream(i < 2 ? sm : lts, i, "worker-" + std::to_string(i));
        }
        recorder.start();
        std::vector<std::thread> workers;
        constexpr uint64_t count = 20000;
        for (unsigned i = 0; i < 4; ++i)
            workers.emplace_back([&, i] {
                // At least 750 simulated seconds of cross-stream disorder. No runtime sorting.
                const uint64_t start = (i & 1) ? 1'000'000'000'000ULL : 0;
                for (uint64_t n = 0; n < count; ++n) {
                    streams[i]->record(start + n, ClockEventKind::User, 0, n);
                }
            });
        for (auto& worker : workers) worker.join();
        recorder.close();
        const auto stats = recorder.stats();
        assert(stats.events == 4 * count && stats.dropped == 0);
        assert(stats.peak_buffer_bytes <= stats.allocated_buffer_bytes);
        std::ofstream expected(config.output_dir / "reference.tsv");
        expected
            << "ts\tunit\tlocal_cycle\tevent\tphase\ttransaction_id\tfifo_id\tvalue\tordinal\n";
        for (unsigned i = 0; i < 4; ++i)
            for (uint64_t n = 0; n < count; ++n) {
                const auto cycle = ((i & 1) ? 1'000'000'000'000ULL : 0) + n;
                auto time = clock_reference::edge(i < 2 ? 914'000'000 : 1'326'000'000,
                                                  i < 2 ? 0 : 137, cycle);
                expected << time.ns() << "\tworker-" << i << '\t' << cycle << "\tuser\t0\t0\t0\t"
                         << n << '\t' << n << '\n';
            }
        // Exactly one text file per domain even with several simultaneous producers.
        assert(std::filesystem::exists(config.output_dir / "text-domain-sm.log"));
        assert(std::filesystem::exists(config.output_dir / "text-domain-lts.log"));
    }
    // Regressing timestamps within one sequence must use its absolute clock
    // without corrupting the incremental baseline or checkpoint mappings.
    {
        PerfettoTraceWriter writer;
        PerfettoTraceWriter::Options options;
        options.compress = false;
        options.checkpoint_interval_packets = 2;
        assert(writer.open(root / "regressing.pftrace", options));
        auto clock = ClockDomain::fromHz(7, "clock", 1'001'000'000, 1, SimTime::picoseconds(999));
        auto sequence = writer.addClockStream(clock);
        auto track = writer.addTrack("regressing");
        for (uint64_t cycle : {1000, 1, 2000, 0, 3000})
            writer.clockInstant(sequence, track, "clock", "edge", cycle, 0);
        bool rejected = false;
        try {
            writer.wallInstant(track, "host", "wrong", 0);
        } catch (const std::logic_error&) {
            rejected = true;
        }
        assert(rejected);
        writer.close();
    }
    if (!keep) std::filesystem::remove_all(root);
    std::cout << "clock tracing: concurrent per-domain streams, bounded queues, "
                 "checkpoint/compression variants passed\n";
}
