// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// SPDX-License-Identifier: MPL-2.0

#include <atomic>
#include <chrono>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "chronon/Chronon.hpp"

namespace {

class ManualUnit : public chronon::Unit {
public:
    explicit ManualUnit(std::string name) : Unit(std::move(name)) {}
    void setCycle(uint64_t cycle) { setLocalCycle(cycle); }
};

}  // namespace

int main() {
    constexpr size_t connections = 16;
    constexpr uint64_t iterations = 20'000'000;
    ManualUnit consumer("consumer");
    chronon::InPort<uint64_t> input{&consumer, "in", 64};
    std::vector<std::unique_ptr<ManualUnit>> producers;
    std::vector<std::unique_ptr<chronon::OutPort<uint64_t>>> outputs;
    std::vector<std::atomic<uint64_t>> progress(connections);
    std::unordered_map<chronon::Unit*, const std::atomic<uint64_t>*> progress_map;

    for (size_t i = 0; i < connections; ++i) {
        producers.push_back(std::make_unique<ManualUnit>("producer" + std::to_string(i)));
        outputs.push_back(
            std::make_unique<chronon::OutPort<uint64_t>>(producers.back().get(), "out"));
        auto* conn = outputs.back()->connect(&input, 1);
        conn->setConnId(static_cast<uint32_t>(i));
        conn->optimizeForMPSC();
        conn->setThreadQueueId(conn->registerProducerThread(i + 1));
        conn->registerOnDestMPSC();
        progress[i].store(iterations + 1, std::memory_order_relaxed);
        progress_map.emplace(producers.back().get(), &progress[i]);
    }
    input.setArbitrationConnProgress(progress_map);

    const auto begin = std::chrono::steady_clock::now();
    uint64_t checksum = 0;
    for (uint64_t cycle = 0; cycle < iterations; ++cycle) {
        consumer.setCycle(cycle);
        if (auto value = input.tryReceive(cycle)) checksum += *value;
    }
    const auto end = std::chrono::steady_clock::now();
    const double ns = std::chrono::duration<double, std::nano>(end - begin).count() /
                      static_cast<double>(iterations);
    std::cout << "direct-lanes=1" << std::fixed << std::setprecision(2)
              << " empty-fanin=" << connections << " ns/receive=" << ns << " checksum=" << checksum
              << '\n';
}
