// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// SPDX-License-Identifier: MPL-2.0

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <vector>

#include "chronon/Chronon.hpp"

namespace {

struct WakeupLike {
    uint32_t prf = 0;
    uint32_t reg_file = 0;
    uint64_t value = 0;
    bool has_value = false;
};

class Producer final : public chronon::TickableUnit {
public:
    explicit Producer(size_t rate) : TickableUnit("producer"), out(this, "out", rate) {}
    void tick() override {}
    void setCycle(uint64_t cycle) { setLocalCycle(cycle); }
    chronon::OutPort<WakeupLike> out;
};

class Consumer final : public chronon::TickableUnit {
public:
    Consumer() : TickableUnit("consumer") {}
    void tick() override {}
    void setCycle(uint64_t cycle) { setLocalCycle(cycle); }
    chronon::InPort<WakeupLike> in{this, "in"};
};

struct Result {
    double ns_per_message = 0.0;
    uint64_t checksum = 0;
};

Result runOnce(uint64_t messages, size_t batch, bool slab) {
    setenv("CHRONON_EXPERIMENTAL_DELAY_ONE_CYCLE_QUEUE", slab ? "1" : "0", 1);
    chronon::TickSimulationConfig config;
    config.enable_parallel = false;
    chronon::TickSimulation simulation(config);
    auto* producer = simulation.createUnit<Producer>(batch);
    auto* consumer = simulation.createUnit<Consumer>();
    simulation.connect(producer->out, consumer->in, 1);
    simulation.initialize();

    uint64_t sent = 0;
    uint64_t received = 0;
    uint64_t checksum = 0;
    uint64_t cycle = 0;
    const auto begin = std::chrono::steady_clock::now();
    while (received < messages) {
        consumer->setCycle(cycle);
        while (auto value = consumer->in.tryReceive(cycle)) {
            checksum += value->value;
            ++received;
        }
        producer->setCycle(cycle);
        for (size_t i = 0; i < batch && sent < messages; ++i, ++sent) {
            if (!producer->out.send(WakeupLike{static_cast<uint32_t>(sent), 1, sent, true})) {
                std::abort();
            }
        }
        ++cycle;
    }
    const auto end = std::chrono::steady_clock::now();
    const double ns = std::chrono::duration<double, std::nano>(end - begin).count();
    return {.ns_per_message = ns / static_cast<double>(messages), .checksum = checksum};
}

double median(std::vector<double> values) {
    std::sort(values.begin(), values.end());
    const size_t middle = values.size() / 2;
    if (values.size() % 2 != 0) return values[middle];
    return (values[middle - 1] + values[middle]) / 2.0;
}

}  // namespace

int main(int argc, char** argv) {
    const uint64_t messages = argc > 1 ? std::strtoull(argv[1], nullptr, 10) : 4'000'000;
    const size_t batch = argc > 2 ? std::strtoull(argv[2], nullptr, 10) : 1;
    const size_t repeats = argc > 3 ? std::strtoull(argv[3], nullptr, 10) : 7;
    if (messages == 0 || batch == 0 || repeats == 0) return 2;

    std::vector<double> heap_results;
    std::vector<double> slab_results;
    uint64_t checksum = 0;
    for (size_t i = 0; i < repeats; ++i) {
        const auto heap = runOnce(messages, batch, false);
        const auto slab = runOnce(messages, batch, true);
        heap_results.push_back(heap.ns_per_message);
        slab_results.push_back(slab.ns_per_message);
        checksum ^= heap.checksum ^ slab.checksum;
    }
    unsetenv("CHRONON_EXPERIMENTAL_DELAY_ONE_CYCLE_QUEUE");

    std::cout << std::fixed << std::setprecision(2) << "messages=" << messages << " batch=" << batch
              << " heap_ns=" << median(heap_results) << " slab_ns=" << median(slab_results)
              << " checksum=" << checksum << '\n';
}
