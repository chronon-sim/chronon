// SPDX-License-Identifier: MPL-2.0
// Uses only pre-multiclock public APIs so exactly this source can be linked
// against origin/main for an interleaved before/after scheduler measurement.
#include <chrono>
#include <iostream>

#include "chronon/Chronon.hpp"

struct Unit : chronon::TickableUnit {
    uint64_t value = 1;
    Unit() : TickableUnit("unit") {}
    void tick() override { value = value * 6364136223846793005ULL + 1442695040888963407ULL; }
};
int main(int argc, char** argv) {
    const uint64_t cycles = argc > 1 ? std::stoull(argv[1]) : 100'000'000;
    if (!cycles || cycles > 1'000'000'000) return 2;
    chronon::TickSimulationConfig config;
    config.num_threads = 1;
    config.enable_parallel = false;
    chronon::TickSimulation sim(config);
    auto* unit = sim.createUnit<Unit>();
    sim.initialize();
    sim.run(10000);
    const auto begin = std::chrono::steady_clock::now();
    sim.run(cycles);
    const auto end = std::chrono::steady_clock::now();
    std::cout << "cycles,wall_s,digest\n"
              << cycles << ',' << std::chrono::duration<double>(end - begin).count() << ','
              << unit->value << '\n';
}
