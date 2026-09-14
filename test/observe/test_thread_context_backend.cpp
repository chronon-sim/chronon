// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0

#include <array>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <thread>

#include "../TestAssertions.hpp"
#include "observe/ObservationBackend.hpp"
#include "observe/ObservationContext.hpp"
#include "observe/ObservationManager.hpp"

using namespace chronon::observe;
using namespace std::chrono_literals;

namespace {

template <typename Predicate>
void waitUntil(Predicate predicate) {
    const auto deadline = std::chrono::steady_clock::now() + 10s;
    while (!predicate()) {
        CHECK(std::chrono::steady_clock::now() < deadline);
        std::this_thread::yield();
    }
}

void testBackend(bool reorder) {
    auto& manager = ThreadContextManager::instance();
    const FormatId format = FormatRegistry::instance().registerFormat(
        "thread churn {}", __FILE__, __LINE__, {ArgType::UInt64}, true, LogLevel::Info);
    const auto root = std::filesystem::temp_directory_path() /
                      ("chronon-thread-lifecycle-" +
                       std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    for (int session = 0; session < 3; ++session) {
        ObservationQueue queue(4096);
        ObservationBackend::Config config;
        config.output_dir = (root / std::to_string(session)).string();
        config.info_file = "info.log";
        config.enable_counter_csv = false;
        config.timeline_enabled = false;
        config.enable_reordering = reorder;
        ObservationBackend backend(queue, config);
        backend.start();
        std::this_thread::sleep_for(20ms);  // Allow the idle backend to enter its wait.
        for (uint64_t i = 0; i < 128; ++i) {
            std::thread([&] {
                ObservationContext context(nullptr, [i] { return i; }, 0, "producer", 1);
                context.enableCategory(category::LOG_INFO);
                context.log<LogLevel::Info>(format, i);
                const auto& stats = context.observationStats().get<ObservationChannel::Info>();
                CHECK(stats.emitted == 1 && stats.dropped == 0);
            }).join();
            // No explicit producer flush/wakeup or manager flushAll here.
            waitUntil([&] { return backend.eventsProcessed() == i + 1; });
            CHECK(manager.activeThreadCount() == 0);
            CHECK(manager.allocatedContextCount() == 1);
        }
        backend.stop();
        std::ifstream input(backend.outputDir() / "info.log");
        CHECK(input.good());
        std::array<bool, 128> seen{};
        size_t count = 0;
        std::string line;
        while (std::getline(input, line)) {
            const auto pos = line.find("thread churn ");
            if (pos == std::string::npos) continue;
            const auto value = std::stoull(line.substr(pos + 13));
            CHECK(value < seen.size() && !seen[value]);
            seen[value] = true;
            ++count;
        }
        CHECK(count == seen.size());
    }
    std::filesystem::remove_all(root);
}

}  // namespace

int main(int argc, char** argv) {
    CHECK(argc == 2);
    const std::string mode = argv[1];
    if (mode == "shutdown") {
        // Leave cleanup to the singleton destructor, including the main
        // thread's TLS lease. The queue pool must outlive this backend.
        auto& observation = ObservationManager::instance();
        ObservationYAMLConfig config;
        config.enabled = true;
        config.output_dir = "thread-context-shutdown-output";
        config.timeline.enabled = false;
        config.counters.csv_output = false;
        observation.initialize(config);
        observation.startBackend();
        CHECK(ThreadContextManager::instance().getContext() != nullptr);
    } else {
        ThreadContextManager::instance().setQueueCapacity(4096);
        CHECK(mode == "backend" || mode == "reordered_backend");
        testBackend(mode == "reordered_backend");
    }
    std::cout << "Thread context " << mode << ": PASSED\n";
}
