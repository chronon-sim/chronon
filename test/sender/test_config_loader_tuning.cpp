// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#include <cassert>
#include <iostream>
#include <vector>

#include "sender/config/SenderConfigLoader.hpp"

using namespace chronon::sender::config;

namespace {

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

    assert(config.name == "tuning_parse");
    assert(config.num_workers == 8);
    assert(config.enable_parallel);
    assert(config.enable_lookahead);
    assert(config.trace_execution);
    assert(config.max_lookahead_cycles == 256);
    assert(config.epoch_size == 1024);
    assert(config.run_cycles == 12345);
    assert(config.rebalance_min_gain == 0.05);
    assert(config.rebalance_cooldown_cycles == 2048);
    assert(config.partition_solver == "SA");
    assert(config.initial_partition_sync_cost_ns == 12.5);

    std::cout << "PASSED\n";
}

void test_tuning_defaults() {
    std::cout << "Testing tuning defaults... ";

    SenderConfigLoader loader;
    auto config = loader.loadFromString(R"yaml(
simulation:
  name: defaults
)yaml");

    assert(!config.trace_execution);
    assert(config.max_lookahead_cycles == 100);
    assert(config.epoch_size == 64);
    assert(!config.enable_dynamic_rebalance);
    assert(config.rebalance_check_interval_cycles == 8192);
    assert(config.rebalance_min_gain == 0.05);
    assert(config.rebalance_cooldown_cycles == 0);
    assert(config.partition_solver == "SA");
    assert(config.initial_partition_sync_cost_ns == 8.0);

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
    assert(names == expected);

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
    assert(names == expected);

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
    assert(uart != nullptr);
    assert(fetch != nullptr);
    assert(uart->tick_interval == 1000);
    assert(fetch->tick_interval == 1);

    std::cout << "PASSED\n";
}

}  // namespace

int main() {
    std::cout << "=== Config Loader Tuning Tests ===\n\n";

    test_tuning_fields_parse();
    test_tuning_defaults();
    test_unit_names_include_programmatic_additions();
    test_unit_names_skip_programmatic_deletions();
    test_unit_tick_interval_parse();

    std::cout << "\n=== Config loader tuning tests PASSED ===\n";
    return 0;
}
