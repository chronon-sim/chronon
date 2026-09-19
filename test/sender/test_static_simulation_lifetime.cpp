// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// SPDX-License-Identifier: MPL-2.0

#include <chrono>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <string>
#include <string_view>

#include "../TestAssertions.hpp"
#include "../observe/PftraceTestDecoder.hpp"
#include "chronon/Chronon.hpp"

using namespace chronon;

namespace {
// Constructed before the static simulation so verification runs after its
// destructor, including its finalizer and observation backend drain.
struct OutputVerifier {
    std::filesystem::path root =
        std::filesystem::temp_directory_path() /
        ("chronon-static-session-" +
         std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    std::filesystem::path log;
    std::filesystem::path timeline;
    bool explicitly_finalized = false;
    bool finalized = false;
    ~OutputVerifier() {
        std::ifstream input(log);
        CHECK(input.good());
        const std::string contents{std::istreambuf_iterator<char>{input},
                                   std::istreambuf_iterator<char>{}};
        CHECK(finalized);
        CHECK(contents.find("tick 2") != std::string::npos);
        if (explicitly_finalized)
            CHECK(contents.find("static finalizer at cycle 3") != std::string::npos);
        const auto decoded = pftrace_test::decodeFile(timeline);
        uint64_t expected_cycle = 0;
        for (const auto& event : decoded.events) {
            if (event.name != "static_tick") continue;
            CHECK(event.uint_annotations.at("cycle") == expected_cycle++);
            CHECK(event.string_annotations.at("phase") == "running");
        }
        CHECK(expected_cycle == 3);
        std::filesystem::remove_all(root);
    }
} verify;

TickSimulationConfig singleThread() {
    TickSimulationConfig result;
    result.num_threads = 1;
    result.setExecutionPolicy(ExecutionPolicy::Sequential);
    return result;
}

TickSimulation simulation(singleThread());
inline const auto STATIC_CATEGORY = Category<"static_lifetime", "Static session lifetime">{};

class StaticUnit : public TickableUnit, public ObservableUnit {
public:
    StaticUnit() : TickableUnit("static_unit") {}
    void tick() override {
        info<"tick {}">(localCycle());
        event<"static_tick">(STATIC_CATEGORY, arg<"cycle">(localCycle()),
                             arg<"phase">(std::string_view("running")));
    }
    void finalize() override {
        CHECK(localCycle() == 3);
        info<"static finalizer at cycle {}">(getObserveCycle());
        finalized_ = true;
        verify.finalized = true;
    }
    ~StaticUnit() override { CHECK(finalized_); }

private:
    bool finalized_ = false;
};
}  // namespace

int main(int argc, char**) {
    observe::ObservationYAMLConfig config;
    config.enabled = true;
    config.output_dir = verify.root.string();
    config.unified_logging.enabled = true;
    config.unified_logging.info_channel.enabled = true;
    config.unified_logging.info_channel.file = "info.log";
    config.unified_logging.trace_channel.enabled = true;
    config.timeline.enabled = true;
    config.timeline.compress = false;
    simulation.configureObservation(config);
    auto& manager = observe::ObservationManager::instance();
    auto* unit = simulation.createUnit<StaticUnit>();
    unit->setObservationContext(
        manager.createContextForUnit(unit->name(), [unit] { return unit->localCycle(); }));
    simulation.initialize();
    manager.startBackend();
    verify.log = manager.backend()->outputDir() / "info.log";
    verify.timeline = manager.backend()->outputDir() / config.timeline.file;
    CHECK(simulation.run(3) == 3);
    if (argc > 1) {
        verify.explicitly_finalized = true;
        simulation.finalize();
    }
    // Static destruction must finalize and drain queued events safely. Logs
    // from a destructor after main-thread TLS retirement may be dropped;
    // explicit finalize() before main returns must preserve the finalizer log.
}
