// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#pragma once

#include <yaml-cpp/yaml.h>

#include <chrono>
#include <cstdint>
#include <functional>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "../../observe/ObservationManager.hpp"
#include "../../tree/TreeNode.hpp"
#include "../config/SenderSimulationBuilder.hpp"
#include "../config/YAMLOverride.hpp"
#include "../core/TerminationRequest.hpp"
#include "../core/TickSimulation.hpp"

namespace chronon {

/**
 * @brief Unified entry point: CLI, YAML override handling, and observation lifecycle.
 *
 * Standard CLI: `[config.yaml]`, `-c/--config`, `-p/--param KEY=VALUE`,
 * `-o/--output-dir`, `-n/--run-cycles`, `-t/--threads`, `--epoch-size`,
 * `--no-observe`, `-v/--verbose`, `-h/--help`, `--version`.
 */
class SimulationApp {
public:
    /** @brief Simulation outcome: builder output plus runtime stats and termination state. */
    struct Result {
        std::unique_ptr<sender::TickSimulation> simulation;
        std::unique_ptr<tree::TreeNode> root_node;
        sender::config::SimulationYAMLConfig config;
        size_t units_created = 0;
        size_t ports_registered = 0;
        size_t connections_made = 0;
        std::unordered_map<std::string, sender::Unit*> unit_map;
        bool observation_enabled = false;

        uint64_t cycles_executed = 0;
        std::chrono::milliseconds wall_time{0};
        double mcycles_per_sec = 0.0;

        sender::TerminationRequest termination;

        int exit_code = 0;
        std::string error_message;

        bool success() const { return exit_code == 0; }
        bool wasTerminated() const { return termination.isRequested(); }
        int terminationExitCode() const { return termination.exit_code; }

        template <typename UnitT>
        UnitT* getUnit(const std::string& name) const {
            auto it = unit_map.find(name);
            if (it == unit_map.end()) return nullptr;
            return dynamic_cast<UnitT*>(it->second);
        }
    };

    /// @p name appears in help and banner.
    explicit SimulationApp(std::string name, std::string description = "");

    /// Path may be absolute or relative to the configured search paths.
    SimulationApp& setDefaultConfig(std::string path);

    /// Default search paths are {"."}.
    SimulationApp& setConfigSearchPaths(std::vector<std::string> paths);

    /// Shown with --version. Default "1.0.0".
    SimulationApp& setVersion(std::string version);

    /// 0 = inherit YAML or system default.
    SimulationApp& setDefaultThreads(uint32_t n);

    /// 0 = run until completion.
    SimulationApp& setDefaultCycles(uint64_t n);

    /// Hook to mutate YAML before parsing — useful for programmatic config changes.
    using PreBuildHook = std::function<void(YAML::Node&)>;
    SimulationApp& onPreBuild(PreBuildHook hook);

    /// Hook called with the built simulation; lets clients touch created units.
    using PostBuildHook = std::function<void(Result&)>;
    SimulationApp& onPostBuild(PostBuildHook hook);

    /// Hook for post-processing after the run completes.
    using PostRunHook = std::function<void(Result&)>;
    SimulationApp& onPostRun(PostRunHook hook);

    /// Main entry: parse args, load YAML, build, run, report. Returns process exit code.
    int run(int argc, char* argv[]);

private:
    struct ParsedOptions {
        std::string config_path;
        std::vector<std::string> overrides;
        std::optional<std::string> output_dir;
        std::optional<uint64_t> run_cycles;
        std::optional<uint32_t> threads;
        std::optional<uint64_t> epoch_size;
        bool no_observe = false;
        bool verbose = false;
        bool show_help = false;
        bool show_version = false;
    };

    ParsedOptions parseArgs(int argc, char* argv[]);

    /// Searches the exact path first, then each search path joined with filename.
    std::string resolveConfigPath(const std::string& hint);

    /// Maps --threads / --output-dir / etc. to YAML override entries.
    void applyConvenienceOverrides(YAML::Node& yaml, const ParsedOptions& opts);

    void printUsage(std::ostream& os, const char* program);
    void printVersion(std::ostream& os);
    void printStatistics(const Result& result, std::ostream& os);
    void printBanner(std::ostream& os);

    std::string name_;
    std::string description_;
    std::string version_ = "1.0.0";
    std::string default_config_;
    std::vector<std::string> search_paths_ = {"."};
    uint32_t default_threads_ = 0;  ///< 0 = inherit YAML.
    uint64_t default_cycles_ = 0;   ///< 0 = inherit YAML.

    PreBuildHook pre_build_hook_;
    PostBuildHook post_build_hook_;
    PostRunHook post_run_hook_;
};

}  // namespace chronon
