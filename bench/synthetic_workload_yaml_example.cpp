// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

/**
 * @file synthetic_workload_yaml_example.cpp
 *
 * YAML-driven entry point proving SyntheticUnit is factory-registered and
 * usable from configuration. Simply including synthetic_workload.hpp registers
 * the "SyntheticUnit" type with the factory.
 *
 * Build:
 *   cmake -S . -B build_bench -DCHRONON_BUILD_BENCHMARKS=ON
 *   cmake --build build_bench --target synthetic_workload_yaml_example
 *
 * Run:
 *   cd build_bench
 *   ./bench/synthetic_workload_yaml_example
 *   ./bench/synthetic_workload_yaml_example synthetic_workload.yaml --threads=4
 *   ./bench/synthetic_workload_yaml_example -p simulation.unit.a.params.arith_ops=128
 */

#include <iostream>

#include "chronon/Chronon.hpp"
#include "synthetic_workload.hpp"

using namespace chronon;

int main(int argc, char* argv[]) {
    return SimulationApp("Synthetic Workload (YAML)",
                         "Tunable synthetic units for framework benchmarking")
        .setDefaultConfig("synthetic_workload.yaml")
        .setConfigSearchPaths({".", "../bench", "bench"})
        .setVersion("1.0.0")
        .onPostBuild([](SimulationApp::Result& /*result*/) {
            std::cout << "Registered unit types:\n";
            for (const auto& [name, desc] :
                 FactoryRegistry::instance().listFactoriesWithDescriptions()) {
                std::cout << "  - " << name << ": " << desc << "\n";
            }
            std::cout << "\n";
        })
        .run(argc, argv);
}
