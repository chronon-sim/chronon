// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#include <iostream>
#include <vector>

#include "../TestAssertions.hpp"
#include "params/ParameterSet.hpp"
#include "sender/config/SenderConfigLoader.hpp"
#include "sender/config/SenderSimulationBuilder.hpp"
#include "sender/factory/SenderFactory.hpp"

using namespace chronon::sender::config;

namespace {

class DefaultIntervalYamlUnit
    : public chronon::sender::factory::AutoRegisteredUnit<DefaultIntervalYamlUnit> {
public:
    using ParameterSet = chronon::params::ParameterSet;
    static constexpr const char* unit_type_name = "DefaultIntervalYamlUnit";
    static constexpr const char* unit_description = "Unit with constructor default tick interval";

    explicit DefaultIntervalYamlUnit(const ParameterSet*) : DefaultIntervalYamlUnit() {}

    DefaultIntervalYamlUnit() : AutoRegisteredUnit("default_interval_yaml") { setTickInterval(7); }

    void tick() override {}
};

void test_tuning_fields_parse() {
    std::cout << "Testing tuning fields parse... ";

    SenderConfigLoader loader;
    auto config = loader.loadFromString(R"yaml(
simulation:
  name: tuning_parse
  num_workers: 8
  enable_parallel: true
  enable_lookahead: true
  trace_execution: true
  max_lookahead_cycles: 256
  epoch_size: 1024
  run_cycles: 12345
  rebalance_min_gain: 0.05
  rebalance_cooldown_cycles: 2048
  partition_solver: SA
  initial_partition_sync_cost_ns: 12.5
)yaml");

    CHECK(config.name == "tuning_parse");
    CHECK(config.num_workers == 8);
    CHECK(config.enable_parallel);
    CHECK(config.enable_lookahead);
    CHECK(config.trace_execution);
    CHECK(config.max_lookahead_cycles == 256);
    CHECK(config.epoch_size == 1024);
    CHECK(config.run_cycles == 12345);
    CHECK(config.rebalance_min_gain == 0.05);
    CHECK(config.rebalance_cooldown_cycles == 2048);
    CHECK(config.partition_solver == "SA");
    CHECK(config.initial_partition_sync_cost_ns == 12.5);

    std::cout << "PASSED\n";
}

void test_tuning_defaults() {
    std::cout << "Testing tuning defaults... ";

    SenderConfigLoader loader;
    auto config = loader.loadFromString(R"yaml(
simulation:
  name: defaults
)yaml");

    CHECK(!config.trace_execution);
    CHECK(config.max_lookahead_cycles == 100);
    CHECK(config.epoch_size == 64);
    CHECK(!config.enable_dynamic_rebalance);
    CHECK(config.rebalance_check_interval_cycles == 8192);
    CHECK(config.rebalance_min_gain == 0.05);
    CHECK(config.rebalance_cooldown_cycles == 0);
    CHECK(config.partition_solver == "SA");
    CHECK(config.initial_partition_sync_cost_ns == 8.0);

    std::cout << "PASSED\n";
}

void test_scheduler_timeline_cpu_time_parse() {
    std::cout << "Testing scheduler timeline thread CPU time parse... ";

    SenderConfigLoader loader;
    auto config = loader.loadFromString(R"yaml(
simulation:
  observation:
    timeline:
      scheduler:
        enabled: true
        trace_thread_cpu_time: true
        min_duration_ns: 100
)yaml");

    CHECK(config.timeline_trace.enabled);
    CHECK(config.timeline_trace.trace_thread_cpu_time);
    CHECK(config.timeline_trace.min_duration_ns == 100);

    std::cout << "PASSED\n";
}

void test_unit_names_include_programmatic_additions() {
    std::cout << "Testing unitNames includes programmatic additions... ";

    SenderConfigLoader loader;
    auto config = loader.loadFromString(R"yaml(
simulation:
  unit:
    fetch:
      type: FetchUnit
    decode:
      type: DecodeUnit
)yaml");

    UnitConfig rename;
    rename.instance_name = "rename";
    rename.type_name = "RenameUnit";
    config.units.emplace(rename.instance_name, rename);

    const std::vector<std::string> names = config.unitNames();
    const std::vector<std::string> expected = {"fetch", "decode", "rename"};
    CHECK(names == expected);

    std::cout << "PASSED\n";
}

void test_unit_names_skip_programmatic_deletions() {
    std::cout << "Testing unitNames skips programmatic deletions... ";

    SenderConfigLoader loader;
    auto config = loader.loadFromString(R"yaml(
simulation:
  unit:
    fetch:
      type: FetchUnit
    decode:
      type: DecodeUnit
    rename:
      type: RenameUnit
)yaml");

    config.units.erase("decode");

    const std::vector<std::string> names = config.unitNames();
    const std::vector<std::string> expected = {"fetch", "rename"};
    CHECK(names == expected);

    std::cout << "PASSED\n";
}

void test_unit_tick_interval_parse() {
    std::cout << "Testing unit tick_interval parse... ";

    SenderConfigLoader loader;
    auto config = loader.loadFromString(R"yaml(
simulation:
  unit:
    uart:
      type: UART
      tick_interval: 1000
    fetch:
      type: FetchUnit
)yaml");

    const auto* uart = config.getUnit("uart");
    const auto* fetch = config.getUnit("fetch");
    (void)uart;
    (void)fetch;
    CHECK(uart != nullptr);
    CHECK(fetch != nullptr);
    CHECK(uart->tick_interval == 1000);
    CHECK(uart->has_tick_interval);
    CHECK(fetch->tick_interval == 1);
    CHECK(!fetch->has_tick_interval);

    std::cout << "PASSED\n";
}

void test_registered_edge_fields_parse() {
    std::cout << "Testing registered edge capacity/rate parse... ";

    SenderConfigLoader loader;
    auto config = loader.loadFromString(R"yaml(
simulation:
  bus:
    wakeup:
      delay: 1
      capacity: 32
      rate: 4
      inputs: [exe0.out_wakeup]
      outputs: [iq0.in_wakeup, iq1.in_wakeup]
  unit:
    fetch:
      type: FetchUnit
      port:
        out_fetch:
          to: decode.in_fetch
          delay: 2
          capacity: 16
          rate: 2
    decode:
      type: DecodeUnit
)yaml");

    CHECK(config.connections.size() == 3);
    const auto& direct = config.connections[2];
    CHECK(direct.source_path == "fetch.out_fetch");
    CHECK(direct.dest_path == "decode.in_fetch");
    CHECK(direct.delay == 2);
    CHECK(direct.capacity.has_value() && *direct.capacity == 16);
    CHECK(direct.rate.has_value() && *direct.rate == 2);

    for (size_t i = 0; i < 2; ++i) {
        CHECK(config.connections[i].capacity.has_value());
        CHECK(*config.connections[i].capacity == 32);
        CHECK(config.connections[i].rate.has_value());
        CHECK(*config.connections[i].rate == 4);
    }

    std::cout << "PASSED\n";
}

void test_builder_preserves_constructor_tick_interval_when_yaml_omits_override() {
    std::cout << "Testing builder preserves constructor tick_interval defaults... ";

    SenderSimulationBuilder builder;
    auto result = builder.buildFromYAMLString(R"yaml(
simulation:
  name: tick_interval_builder
  num_workers: 1
  enable_parallel: false
  unit:
    omitted:
      type: DefaultIntervalYamlUnit
    explicit:
      type: DefaultIntervalYamlUnit
      tick_interval: 2
)yaml");

    auto* omitted = result.unit_map.at("omitted");
    auto* explicit_unit = result.unit_map.at("explicit");
    (void)omitted;
    (void)explicit_unit;
    CHECK(omitted->tickInterval() == 7);
    CHECK(explicit_unit->tickInterval() == 2);

    std::cout << "PASSED\n";
}

}  // namespace

int main() {
    std::cout << "=== Config Loader Tuning Tests ===\n\n";

    test_tuning_fields_parse();
    test_tuning_defaults();
    test_scheduler_timeline_cpu_time_parse();
    test_unit_names_include_programmatic_additions();
    test_unit_names_skip_programmatic_deletions();
    test_unit_tick_interval_parse();
    test_registered_edge_fields_parse();
    test_builder_preserves_constructor_tick_interval_when_yaml_omits_override();

    std::cout << "\n=== Config loader tuning tests PASSED ===\n";
    return 0;
}
