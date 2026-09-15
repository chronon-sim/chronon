// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// SPDX-License-Identifier: MPL-2.0
// Uses existing public APIs so the same source can be linked against main.
// Times emission through final close; disk writes use the OS page cache.
// Modes: 0/1 writer raw/compressed, 2/3 immediate backend raw/compressed,
//        4/5 reordered backend raw/compressed.
#include <chrono>
#include <filesystem>
#include <iostream>

#include "observe/ObservationBackend.hpp"
#include "observe/ObservationContext.hpp"
using namespace chronon::observe;
int main(int argc, char** argv) {
    if (argc != 4) {
        std::cerr << "Usage: " << argv[0] << " <mode 0..5> <events> <new-output-directory>\n";
        return 2;
    }
    const int mode = std::stoi(argv[1]);
    const uint64_t count = std::stoull(argv[2]);
    const std::filesystem::path root = argv[3];
    if (mode < 0 || mode > 5 || count == 0 || count > 100'000'000 || std::filesystem::exists(root))
        return 2;
    std::filesystem::create_directories(root);
    const bool compress = mode & 1;
    double seconds;
    uint64_t bytes;
    if (mode < 2) {
        PerfettoTraceWriter writer;
        PerfettoTraceWriter::Options options;
        options.compress = compress;
        if (!writer.open(root / "writer.pftrace", options)) return 3;
        const auto track = writer.addTrack("events");
        const auto begin = std::chrono::steady_clock::now();
        for (uint64_t i = 0; i < count; ++i) writer.instant(track, "trace", "event", i);
        writer.close();
        seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - begin).count();
        bytes = writer.bytesWritten();
    } else {
        ThreadContextManager::instance().setQueueCapacity(65536);
        ThreadContextManager::instance().setBackpressurePolicy(BackpressurePolicy::SpinWait);
        ObservationQueue queue(65536);
        ObservationBackend::Config config;
        config.output_dir = root.string();
        config.enable_counter_csv = false;
        config.timeline_compress = compress;
        config.enable_reordering = mode >= 4;
        ObservationBackend backend(queue, config);
        ObservationContext context(&queue, [] { return 0ULL; }, 0, "producer", 1);
        context.enableCategory(category::TRACE);
        const auto track = TimelineTrackRegistry::instance().registerTrack(
            {"events", 1, 1, TimelineTrackInfo::Layout::Normal});
        const auto name = EventName<"event">::ref().id;
        backend.start();
        const auto begin = std::chrono::steady_clock::now();
        for (uint64_t i = 0; i < count; ++i) {
            context.setCurrentCycleValue(i);
            if (!context.timelineEvent(category::TRACE, TimelineEventKind::Instant, track, 0, name,
                                       0, nullptr, 0))
                return 4;
        }
        ThreadContextManager::instance().flushAll();
        backend.stop();
        seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - begin).count();
        if (backend.eventsProcessed() != count) return 5;
        bytes = std::filesystem::file_size(backend.outputDir() / "timeline.pftrace");
    }
    std::cout << mode << ',' << count << ',' << seconds << ',' << bytes << '\n';
    std::filesystem::remove_all(root);
}
