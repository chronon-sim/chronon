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

void writerCollisions(const std::filesystem::path& root) {
    using A = PerfettoTraceWriter::Annotation;
    for (unsigned variant = 0; variant < 4; ++variant) {
        PerfettoTraceWriter writer;
        PerfettoTraceWriter::Options options;
        options.compress = variant & 2;
        options.checkpoint_interval_packets = 2;
        // 160 records / 2 forces multiple bounded external merge passes.
        options.clock_sort_run_records = 2;
        assert(writer.open(root / ("writer-collision-" + std::to_string(variant) + ".pftrace"),
                           options));
        const auto clock = ClockDomain::fromHz(9, "four-ghz", 4'000'000'000ULL);
        const auto source = writer.addClockStream(clock), target = writer.addClockStream(clock);
        const auto source_track = writer.addTrack("z-source"),
                   target_track = writer.addTrack("a-target");
        const auto emit = [&](bool is_source) {
            for (uint64_t i = 0; i < 80; ++i) {
                const uint64_t n = variant & 1 ? 79 - i : i;
                // Even: 0 ns -> 0.75 ns within one displayed ns.
                // Odd: same exact time across units, Evaluate -> Commit.
                const uint64_t cycle = n * 4 + (n & 1 ? 1 : is_source ? 0 : 3);
                const uint64_t phase = (n & 1) && is_source ? 0 : 1;
                std::string label = is_source ? "owned-source" : "owned-target";
                const std::array<A, 4> annotations{{{"phase", A::Kind::Uint, phase},
                                                    {"ordinal", A::Kind::Uint, n},
                                                    {"transaction_id", A::Kind::Uint, 42 + n},
                                                    {"label", A::Kind::String, 0, label}}};
                writer.clockInstant(is_source ? source : target,
                                    is_source ? source_track : target_track, "clock",
                                    is_source ? "write" : "visible", cycle, 42 + n, annotations);
                label.assign("mutated-after-record");
                if (i % 17 == 0) writer.flush();
            }
        };
        emit(!(variant & 1));
        emit(variant & 1);
        assert(writer.eventsWritten() == 160);
        std::string oversized(65536, 'x');
        const std::array<A, 1> bad{{{"label", A::Kind::String, 0, oversized}}};
        bool rejected = false;
        try {
            writer.clockInstant(source, source_track, "clock", "oversized", 0, 0, bad);
        } catch (const std::invalid_argument&) {
            rejected = true;
        }
        assert(rejected && writer.eventsWritten() == 160);
        writer.close();
        writer.close();
        assert(writer.eventsWritten() == 160 && writer.bytesWritten() > 0);
    }
}

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
        config.perfetto_options.clock_sort_run_records = 1024;
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
                // At least 750 simulated seconds of cross-stream disorder; sorting is offline.
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
    writerCollisions(root);
    // Regressing input is normalized before encoding without changing event times
    // or checkpoint mappings.
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
