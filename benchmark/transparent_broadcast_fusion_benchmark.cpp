// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// SPDX-License-Identifier: MPL-2.0

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "sender/core/TickSimulation.hpp"

namespace {

using chronon::sender::InPort;
using chronon::sender::OutPort;
using chronon::sender::TickableUnit;
using chronon::sender::TickSimulation;
using chronon::sender::TickSimulationConfig;

uint64_t parsePositive(const char* value, const char* name) {
    try {
        size_t parsed = 0;
        const auto result = std::stoull(value, &parsed);
        if (parsed == 0 || value[parsed] != '\0' || result == 0) {
            throw std::invalid_argument("not positive");
        }
        return result;
    } catch (const std::exception&) {
        throw std::invalid_argument(std::string("invalid ") + name + ": " + value);
    }
}

class Producer final : public TickableUnit {
public:
    Producer(std::string name, size_t messages_per_cycle)
        : TickableUnit(std::move(name)), out(this, "out", messages_per_cycle) {}

    void tick() override {}
    void setCycle(uint64_t cycle) { setLocalCycle(cycle); }

    OutPort<uint64_t> out;
};

class Consumer final : public TickableUnit {
public:
    explicit Consumer(std::string name) : TickableUnit(std::move(name)) {}

    void tick() override {}
    void setCycle(uint64_t cycle) { setLocalCycle(cycle); }

    InPort<uint64_t> in{this, "in"};
};

}  // namespace

int main(int argc, char** argv) {
    try {
        const size_t producer_count =
            static_cast<size_t>(argc > 1 ? parsePositive(argv[1], "producer count") : 10);
        const size_t consumer_count =
            static_cast<size_t>(argc > 2 ? parsePositive(argv[2], "consumer count") : 11);
        const uint64_t cycles = argc > 3 ? parsePositive(argv[3], "cycle count") : 500'000;
        const size_t messages_per_cycle =
            static_cast<size_t>(argc > 4 ? parsePositive(argv[4], "messages per cycle") : 1);
        if (producer_count > std::numeric_limits<uint64_t>::max() / consumer_count ||
            producer_count * consumer_count >
                std::numeric_limits<uint64_t>::max() / cycles / messages_per_cycle) {
            throw std::overflow_error("benchmark message count overflows uint64_t");
        }

        TickSimulationConfig config;
        config.enable_parallel = false;
        TickSimulation simulation(config);
        std::vector<Producer*> producers;
        std::vector<Consumer*> consumers;
        producers.reserve(producer_count);
        consumers.reserve(consumer_count);
        for (size_t index = 0; index < producer_count; ++index) {
            producers.push_back(simulation.createUnit<Producer>("producer" + std::to_string(index),
                                                                messages_per_cycle));
        }
        for (size_t index = 0; index < consumer_count; ++index) {
            consumers.push_back(
                simulation.createUnit<Consumer>("consumer" + std::to_string(index)));
        }
        for (auto* producer : producers) {
            for (auto* consumer : consumers) simulation.connect(producer->out, consumer->in, 1);
        }
        simulation.initialize();

        uint64_t checksum = 0;
        uint64_t received = 0;
        const auto begin = std::chrono::steady_clock::now();
        for (uint64_t cycle = 0; cycle < cycles; ++cycle) {
            for (size_t producer = 0; producer < producers.size(); ++producer) {
                producers[producer]->setCycle(cycle);
                for (size_t message = 0; message < messages_per_cycle; ++message) {
                    const uint64_t value =
                        (cycle << 16) ^ (static_cast<uint64_t>(producer) << 8) ^ message;
                    if (!producers[producer]->out.send(value)) {
                        throw std::runtime_error("transparent broadcast send failed");
                    }
                }
            }
            for (auto* consumer : consumers) {
                consumer->setCycle(cycle + 1);
                while (auto value = consumer->in.tryReceive(cycle + 1)) {
                    checksum += *value;
                    ++received;
                }
            }
        }
        const auto end = std::chrono::steady_clock::now();

        const uint64_t expected =
            static_cast<uint64_t>(producer_count) * consumer_count * cycles * messages_per_cycle;
        if (received != expected) {
            throw std::runtime_error("transparent broadcast benchmark lost messages");
        }
        const double elapsed_seconds = std::chrono::duration<double>(end - begin).count();
        const double ns_per_message =
            std::chrono::duration<double, std::nano>(end - begin).count() /
            static_cast<double>(received);
        const char* fusion = std::getenv("CHRONON_EXPERIMENTAL_TRANSPARENT_BROADCAST_FUSION");
#if CHRONON_ENABLE_OUTPORT_CANCELLATION
        const bool fusion_enabled = !(fusion && fusion[0] == '0' && fusion[1] == '\0');
#else
        (void)fusion;
        const bool fusion_enabled = false;
#endif
        std::cout << std::fixed << std::setprecision(3)
                  << "fusion=" << (fusion_enabled ? "on" : "off") << " producers=" << producer_count
                  << " consumers=" << consumer_count << " messages_per_cycle=" << messages_per_cycle
                  << " cycles=" << cycles << " deliveries=" << received
                  << " elapsed_s=" << elapsed_seconds << " ns/delivery=" << ns_per_message
                  << " checksum=" << checksum << '\n';
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
    return 0;
}
