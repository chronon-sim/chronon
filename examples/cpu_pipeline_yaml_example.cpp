// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

/**
 * @file cpu_pipeline_yaml_example.cpp
 *
 * CPU Pipeline Simulation Example - YAML Configuration
 *
 * This example demonstrates YAML-driven simulation construction using SimulationApp:
 * - 4-wide superscalar pipeline with 4 ALUs
 * - Branch prediction with dual internal pipelines
 * - Flush engine for branch misprediction recovery
 * - Loading simulation configuration from YAML
 * - Automatic CLI support with parameter overrides
 * - Automatic observation lifecycle management
 *
 * Build:
 *   cd build
 *   cmake .. -DCHRONON_BUILD_EXAMPLES=ON
 *   make cpu_pipeline_yaml_example
 *
 * Run:
 *   ./examples/cpu_pipeline_yaml_example                    # Uses default cpu_pipeline.yaml
 *   ./examples/cpu_pipeline_yaml_example path/to/config.yaml
 *   ./examples/cpu_pipeline_yaml_example config.yaml -p
 * simulation.observation.logging.warn.enabled=true
 *   ./examples/cpu_pipeline_yaml_example config.yaml -p
 * simulation.unit.fetch.params.icache_lines=20
 *   ./examples/cpu_pipeline_yaml_example config.yaml -p key1=val1 -p key2=val2
 *   ./examples/cpu_pipeline_yaml_example --threads=4 --run-cycles=1000000
 *   ./examples/cpu_pipeline_yaml_example --help
 */

#include <iostream>

#include "chronon/Chronon.hpp"
#include "cpu_pipeline_common.hpp"

using namespace chronon;
using namespace cpu_pipeline;

int main(int argc, char* argv[]) {
    return SimulationApp("4-Wide CPU Pipeline YAML Configuration Example",
                         "Demonstrating YAML-driven simulation with Chronon")
        .setDefaultConfig("cpu_pipeline.yaml")
        .setConfigSearchPaths({".", "../examples", "examples"})
        .setVersion("0.4.1")
        .onPostBuild([](SimulationApp::Result& /*result*/) {
            // Print pipeline architecture info after build
            std::cout << "Pipeline Architecture:\n";
            std::cout << "  Frontend:   Fetch (with branch prediction) -> Decode (4-wide)\n";
            std::cout << "  Execute:    ALU0, ALU1, ALU2, ALU3 (4 parallel ALUs)\n";
            std::cout << "  Backend:    Writeback (4-wide)\n";
            std::cout << "  Memory:     I-Cache -> L2\n";
            std::cout << "  Flush:      ALUs -> FlushEngine -> All units\n\n";

            // List registered unit types
            std::cout << "Registered unit types:\n";
            for (const auto& [name, desc] :
                 FactoryRegistry::instance().listFactoriesWithDescriptions()) {
                std::cout << "  - " << name << ": " << desc << "\n";
            }
            std::cout << "\n";
        })
        .run(argc, argv);
}
