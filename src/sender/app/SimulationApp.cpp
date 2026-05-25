// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#include "SimulationApp.hpp"

#include <algorithm>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <sstream>

#include "../../observe/ThreadContextManager.hpp"
#include "../core/CrashHandler.hpp"

namespace chronon {

SimulationApp::SimulationApp(std::string name, std::string description)
    : name_(std::move(name)), description_(std::move(description)) {}

SimulationApp& SimulationApp::setDefaultConfig(std::string path) {
    default_config_ = std::move(path);
    return *this;
}

SimulationApp& SimulationApp::setConfigSearchPaths(std::vector<std::string> paths) {
    search_paths_ = std::move(paths);
    return *this;
}

SimulationApp& SimulationApp::setVersion(std::string version) {
    version_ = std::move(version);
    return *this;
}

SimulationApp& SimulationApp::setDefaultThreads(uint32_t n) {
    default_threads_ = n;
    return *this;
}

SimulationApp& SimulationApp::setDefaultCycles(uint64_t n) {
    default_cycles_ = n;
    return *this;
}

SimulationApp& SimulationApp::onPreBuild(PreBuildHook hook) {
    pre_build_hook_ = std::move(hook);
    return *this;
}

SimulationApp& SimulationApp::onPostBuild(PostBuildHook hook) {
    post_build_hook_ = std::move(hook);
    return *this;
}

SimulationApp& SimulationApp::onProfilingBuild(ProfilingBuildHook hook) {
    profiling_build_hook_ = std::move(hook);
    return *this;
}

SimulationApp& SimulationApp::onPostRun(PostRunHook hook) {
    post_run_hook_ = std::move(hook);
    return *this;
}

int SimulationApp::run(int argc, char* argv[]) {
    Result result;

    // Crash handlers go first so they cover everything below.
    sender::CrashHandler::install();

    try {
        ParsedOptions opts = parseArgs(argc, argv);

        if (opts.show_help) {
            printUsage(std::cout, argv[0]);
            return 0;
        }

        if (opts.show_version) {
            printVersion(std::cout);
            return 0;
        }

        printBanner(std::cout);

        std::string config_path = opts.config_path;
        if (config_path.empty()) {
            config_path = default_config_;
        }
        if (config_path.empty()) {
            std::cerr << "Error: No configuration file specified.\n";
            std::cerr << "Use --help for usage information.\n";
            return 1;
        }

        config_path = resolveConfigPath(config_path);
        if (config_path.empty()) {
            std::cerr << "Error: Cannot find configuration file.\n";
            std::cerr << "Searched paths:\n";
            for (const auto& path : search_paths_) {
                std::cerr << "  - " << path << "\n";
            }
            return 1;
        }

        if (opts.verbose) {
            std::cout << "Loading configuration from: " << config_path << "\n";
        }

        std::ifstream file(config_path);
        if (!file.is_open()) {
            std::cerr << "Error: Cannot open config file: " << config_path << "\n";
            return 1;
        }
        std::string yaml_content{std::istreambuf_iterator<char>(file),
                                 std::istreambuf_iterator<char>()};
        file.close();

        YAML::Node yaml_node = YAML::Load(yaml_content);

        applyConvenienceOverrides(yaml_node, opts);

        if (!opts.overrides.empty()) {
            if (opts.verbose) {
                std::cout << "Applying " << opts.overrides.size() << " override(s):\n";
                for (const auto& override_str : opts.overrides) {
                    std::cout << "  - " << override_str << "\n";
                }
            }
            try {
                sender::config::YAMLOverride::applyOverrides(yaml_node, opts.overrides);
            } catch (const sender::config::YAMLOverrideError& e) {
                std::cerr << "Override error: " << e.what() << "\n";
                return 1;
            }
        }

        if (pre_build_hook_) {
            pre_build_hook_(yaml_node);
        }

        // Optional profiling pass: build a throw-away simulation, extract tick costs, destroy
        // it, then build the real simulation with pre-computed costs. Keeps profiling state
        // out of the real simulation.
        std::vector<double> precomputed_unit_costs;
        sender::PlatformMetrics precomputed_platform_metrics{};
        bool have_precomputed_costs = false;

        bool profiling_likely_needed = false;
        {
            auto sim_node = yaml_node["simulation"];
            if (sim_node) {
                bool enable_parallel =
                    sim_node["enable_parallel"] ? sim_node["enable_parallel"].as<bool>(true) : true;
                bool enable_weighted = sim_node["enable_weighted_partitioning"]
                                           ? sim_node["enable_weighted_partitioning"].as<bool>(true)
                                           : true;
                uint32_t num_workers =
                    sim_node["num_workers"] ? sim_node["num_workers"].as<uint32_t>(1) : 1;
                profiling_likely_needed = enable_parallel && enable_weighted && num_workers > 1;
            }
        }

        if (profiling_likely_needed && profiling_build_hook_) {
            if (opts.verbose) {
                std::cout << "\nBuilding independent profiling simulation...\n";
            }

            // YAML-CPP's Clone() can share sub-nodes — modifying the clone corrupts the
            // original. Round-trip through emit+parse for a fully independent copy.
            YAML::Emitter emitter;
            emitter << yaml_node;
            YAML::Node profiling_yaml = YAML::Load(emitter.c_str());
            if (!profiling_yaml["simulation"]["observation"]) {
                profiling_yaml["simulation"]["observation"] = YAML::Node(YAML::NodeType::Map);
            }
            profiling_yaml["simulation"]["observation"]["enabled"] = false;

            sender::config::SenderSimulationBuilder profiling_builder;
            auto profiling_build = profiling_builder.buildFromYAMLNode(profiling_yaml, config_path);

            Result profiling_result;
            profiling_result.simulation = std::move(profiling_build.simulation);
            profiling_result.root_node = std::move(profiling_build.root_node);
            profiling_result.config = std::move(profiling_build.config);
            profiling_result.units_created = profiling_build.units_created;
            profiling_result.ports_registered = profiling_build.ports_registered;
            profiling_result.connections_made = profiling_build.connections_made;
            profiling_result.unit_map = std::move(profiling_build.unit_map);
            profiling_result.observation_enabled = false;

            profiling_build_hook_(profiling_result);

            // initialize() runs the profile and assigns threads.
            profiling_result.simulation->initialize();

            precomputed_unit_costs = profiling_result.simulation->getProfiledUnitCosts();
            precomputed_platform_metrics = profiling_result.simulation->getPlatformMetrics();
            have_precomputed_costs = !precomputed_unit_costs.empty();

            if (opts.verbose) {
                std::cout << "Profiling complete: extracted costs for "
                          << precomputed_unit_costs.size() << " units\n";
            }

            // profiling_result goes out of scope here, destroying the throw-away sim.
        }

        sender::config::SenderSimulationBuilder builder;
        auto build_result = builder.buildFromYAMLNode(yaml_node, config_path);

        result.simulation = std::move(build_result.simulation);
        result.root_node = std::move(build_result.root_node);
        result.config = std::move(build_result.config);
        result.units_created = build_result.units_created;
        result.ports_registered = build_result.ports_registered;
        result.connections_made = build_result.connections_made;
        result.unit_map = std::move(build_result.unit_map);
        result.observation_enabled = build_result.observation_enabled;

        if (opts.verbose) {
            std::cout << "\nConfiguration loaded successfully:\n";
            std::cout << "  Name:        " << result.config.name << "\n";
            std::cout << "  Workers:     " << result.config.num_workers << "\n";
            std::cout << "  Lookahead:   "
                      << (result.config.enable_lookahead ? "enabled" : "disabled") << "\n";
            std::cout << "  Epoch size:  " << result.config.epoch_size << "\n";
            std::cout << "  Units:       " << result.units_created << "\n";
            std::cout << "  Ports:       " << result.ports_registered << "\n";
            std::cout << "  Connections: " << result.connections_made << "\n";
        }

        if (post_build_hook_) {
            post_build_hook_(result);
        }

        if (have_precomputed_costs) {
            result.simulation->setPrecomputedProfilingData(std::move(precomputed_unit_costs),
                                                           precomputed_platform_metrics);
        }

        // initialize() also runs unit initialization (which registers Counters). When
        // precomputed costs are set, this skips live profiling.
        result.simulation->initialize();

        // Force-commit SPSC observation queues so cycle-0 init events are visible to the
        // consumer before it starts draining. Otherwise a small init batch may sit below the
        // batched-commit threshold, only flushed once later events push past it — causing
        // cycle-0 events to appear out-of-order.
        observe::ThreadContextManager::instance().flushAll();

        // Must come after initialize() — unit init registers Counters that can resize the
        // counter vector, which would invalidate any previously registered pointers.
        if (result.observation_enabled) {
            auto& obs_mgr = observe::ObservationManager::instance();
            if (opts.verbose) {
                std::cout << "\nObservation enabled:\n";
                std::cout << "  Output directory: " << obs_mgr.outputDir() << "\n";
                std::cout << "  Contexts created: " << obs_mgr.contextCount() << "\n";
            }
            obs_mgr.reregisterAllCounters();
            obs_mgr.startBackend();
        }

        uint64_t run_cycles = result.config.run_cycles;
        if (run_cycles == 0 && default_cycles_ > 0) {
            run_cycles = default_cycles_;
        }
        if (run_cycles == 0) {
            run_cycles = 10'000'000;  // Safety upper bound when YAML didn't specify.
            if (opts.verbose) {
                std::cout << "\nRunning until completion (max " << run_cycles << " cycles)...\n";
            }
        } else {
            if (opts.verbose) {
                std::cout << "\nRunning for " << run_cycles << " cycles...\n";
            }
        }

        // runUntilTermination supports unit-initiated termination.
        auto start = std::chrono::high_resolution_clock::now();
        result.cycles_executed = result.simulation->runUntilTermination(run_cycles);
        auto end = std::chrono::high_resolution_clock::now();
        result.wall_time = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

        if (result.simulation->timelineTraceEnabled()) {
            result.simulation->writeTimelineTrace();
            if (opts.verbose) {
                std::cout << "Scheduler timeline trace: " << result.simulation->timelineTraceFile()
                          << "\n";
            }
        }

        if (result.simulation->wasTerminationRequested()) {
            result.termination = result.simulation->terminationRequest();
        }

        if (result.wall_time.count() > 0) {
            result.mcycles_per_sec =
                static_cast<double>(result.cycles_executed) / result.wall_time.count() / 1000.0;
        }

        if (result.observation_enabled) {
            auto& obs_mgr = observe::ObservationManager::instance();

            // Final counter snapshot before stopping the backend.
            if (obs_mgr.dumpOnShutdown() && obs_mgr.config().counters.enabled) {
                if (opts.verbose) {
                    std::cout << "Dumping final counter snapshots at cycle "
                              << result.cycles_executed << "...\n";
                }
                obs_mgr.dumpCounterSnapshots(result.cycles_executed);
            }

            obs_mgr.stopBackend();
        }

        if (post_run_hook_) {
            post_run_hook_(result);
        }

        printStatistics(result, std::cout);

        if (result.observation_enabled) {
            auto& obs_mgr = observe::ObservationManager::instance();
            std::cout << "\n=== Observation Report ===\n";
            obs_mgr.printReport(std::cout);
        }

        std::cout << "\n=== SIMULATION COMPLETE ===\n";
        return 0;

    } catch (const sender::TickException& e) {
        std::cerr << "\n=== SIMULATION CRASH ===\n";
        std::cerr << "Unit:    " << e.unitName() << "\n";
        std::cerr << "Cycle:   " << e.cycle() << "\n";
        std::cerr << "Error:   " << e.what() << "\n";
        std::cerr << "Flushing observer data...\n";
        sender::CrashHandler::emergencyFlush();
        std::cerr << "Observer data flushed.\n";
        result.exit_code = 1;
        result.error_message = e.what();
        return 1;
    } catch (const sender::config::BuildError& e) {
        std::cerr << "Build error: " << e.what() << "\n";
        result.exit_code = 1;
        result.error_message = e.what();
        return 1;
    } catch (const sender::config::ConfigLoadError& e) {
        std::cerr << "Configuration error: " << e.what() << "\n";
        result.exit_code = 1;
        result.error_message = e.what();
        return 1;
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
        std::cerr << "Flushing observer data...\n";
        sender::CrashHandler::emergencyFlush();
        result.exit_code = 1;
        result.error_message = e.what();
        return 1;
    }
}

SimulationApp::ParsedOptions SimulationApp::parseArgs(int argc, char* argv[]) {
    ParsedOptions opts;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];

        if (arg == "-h" || arg == "--help") {
            opts.show_help = true;
            continue;
        }

        if (arg == "--version") {
            opts.show_version = true;
            continue;
        }

        if (arg == "-v" || arg == "--verbose") {
            opts.verbose = true;
            continue;
        }

        if (arg == "--no-observe") {
            opts.no_observe = true;
            continue;
        }

        if (arg == "-c" || arg == "--config") {
            if (i + 1 >= argc) {
                throw std::runtime_error("Missing value for " + arg);
            }
            opts.config_path = argv[++i];
            continue;
        }

        if (arg == "-p" || arg == "--param" || arg == "--parameter") {
            if (i + 1 >= argc) {
                throw std::runtime_error("Missing value for " + arg);
            }
            opts.overrides.push_back(argv[++i]);
            continue;
        }

        if (arg == "-o" || arg == "--output-dir") {
            if (i + 1 >= argc) {
                throw std::runtime_error("Missing value for " + arg);
            }
            opts.output_dir = argv[++i];
            continue;
        }

        if (arg == "-n" || arg == "--run-cycles") {
            if (i + 1 >= argc) {
                throw std::runtime_error("Missing value for " + arg);
            }
            opts.run_cycles = std::stoull(argv[++i]);
            continue;
        }

        if (arg == "-t" || arg == "--threads") {
            if (i + 1 >= argc) {
                throw std::runtime_error("Missing value for " + arg);
            }
            opts.threads = std::stoul(argv[++i]);
            continue;
        }

        if (arg == "--epoch-size") {
            if (i + 1 >= argc) {
                throw std::runtime_error("Missing value for " + arg);
            }
            opts.epoch_size = std::stoull(argv[++i]);
            continue;
        }

        // --option=value form.
        size_t eq_pos = arg.find('=');
        if (eq_pos != std::string::npos && arg[0] == '-') {
            std::string opt = arg.substr(0, eq_pos);
            std::string val = arg.substr(eq_pos + 1);

            if (opt == "-c" || opt == "--config") {
                opts.config_path = val;
            } else if (opt == "-p" || opt == "--param" || opt == "--parameter") {
                opts.overrides.push_back(val);
            } else if (opt == "-o" || opt == "--output-dir") {
                opts.output_dir = val;
            } else if (opt == "-n" || opt == "--run-cycles") {
                opts.run_cycles = std::stoull(val);
            } else if (opt == "-t" || opt == "--threads") {
                opts.threads = std::stoul(val);
            } else if (opt == "--epoch-size") {
                opts.epoch_size = std::stoull(val);
            } else {
                throw std::runtime_error("Unknown option: " + opt);
            }
            continue;
        }

        if (arg[0] == '-') {
            throw std::runtime_error("Unknown option: " + arg);
        }

        // Positional argument is the config file path.
        if (opts.config_path.empty()) {
            opts.config_path = arg;
        } else {
            throw std::runtime_error("Multiple config files specified: " + opts.config_path +
                                     " and " + arg);
        }
    }

    return opts;
}

std::string SimulationApp::resolveConfigPath(const std::string& hint) {
    {
        std::ifstream test(hint);
        if (test.good()) {
            return hint;
        }
    }

    for (const auto& search_path : search_paths_) {
        std::string full_path = search_path;
        if (!full_path.empty() && full_path.back() != '/') {
            full_path += '/';
        }
        full_path += hint;

        std::ifstream test(full_path);
        if (test.good()) {
            return full_path;
        }
    }

    return "";
}

void SimulationApp::applyConvenienceOverrides(YAML::Node& yaml, const ParsedOptions& opts) {
    if (!yaml["simulation"]) {
        yaml["simulation"] = YAML::Node(YAML::NodeType::Map);
    }

    if (opts.threads.has_value()) {
        yaml["simulation"]["num_workers"] = *opts.threads;
    } else if (default_threads_ > 0 && !yaml["simulation"]["num_workers"]) {
        yaml["simulation"]["num_workers"] = default_threads_;
    }

    if (opts.run_cycles.has_value()) {
        yaml["simulation"]["run_cycles"] = *opts.run_cycles;
    }

    if (opts.epoch_size.has_value()) {
        yaml["simulation"]["epoch_size"] = *opts.epoch_size;
    }

    if (opts.no_observe) {
        if (!yaml["simulation"]["observation"]) {
            yaml["simulation"]["observation"] = YAML::Node(YAML::NodeType::Map);
        }
        yaml["simulation"]["observation"]["enabled"] = false;
    }

    if (opts.output_dir.has_value()) {
        if (!yaml["simulation"]["observation"]) {
            yaml["simulation"]["observation"] = YAML::Node(YAML::NodeType::Map);
        }
        yaml["simulation"]["observation"]["output_dir"] = *opts.output_dir;
    }
}

void SimulationApp::printBanner(std::ostream& os) {
    os << "==============================================\n";
    os << name_ << "\n";
    if (!description_.empty()) {
        os << description_ << "\n";
    }
    os << "Using Chronon Simulation Framework\n";
    os << "==============================================\n\n";
}

void SimulationApp::printUsage(std::ostream& os, const char* program) {
    os << "Usage: " << program << " [config.yaml] [options]\n\n";

    if (!description_.empty()) {
        os << description_ << "\n\n";
    }

    os << "Options:\n";
    os << "  -c, --config <path>       YAML configuration file\n";
    os << "  -p, --param KEY=VALUE     Override YAML value (repeatable)\n";
    os << "  -o, --output-dir <path>   Override observation output directory\n";
    os << "  -n, --run-cycles <N>      Override simulation run cycles (0 = until completion)\n";
    os << "  -t, --threads <N>         Override number of worker threads\n";
    os << "  --epoch-size <N>          Override simulation epoch size in cycles\n";
    os << "  --no-observe              Disable observation system\n";
    os << "  -v, --verbose             Increase output verbosity\n";
    os << "  -h, --help                Show this help message\n";
    os << "  --version                 Show version information\n";
    os << "\n";

    os << "Examples:\n";
    os << "  " << program << " config.yaml\n";
    os << "  " << program << " config.yaml -p simulation.num_workers=4\n";
    os << "  " << program << " config.yaml --threads=2 --run-cycles=1000000\n";
    os << "  " << program << " config.yaml --epoch-size=1024\n";
    os << "  " << program << " config.yaml --output-dir=/tmp/sim_output\n";
    os << "  " << program << " config.yaml --no-observe\n";
    os << "\n";

    os << "Override path format:\n";
    os << "  Use dot notation to specify nested YAML keys.\n";
    os << "  Example: simulation.unit.fetch.params.max_instructions=500000\n";

    if (!default_config_.empty()) {
        os << "\n";
        os << "Default configuration: " << default_config_ << "\n";
    }
}

void SimulationApp::printVersion(std::ostream& os) {
    os << name_ << " version " << version_ << "\n";
    os << "Powered by Chronon Simulation Framework\n";
}

void SimulationApp::printStatistics(const Result& result, std::ostream& os) {
    os << "\n=== Simulation Statistics ===\n";
    os << "  Cycles executed: " << result.cycles_executed << "\n";

    if (result.simulation) {
        uint64_t freq_hz = result.simulation->tickFrequencyHz();
        uint64_t freq_mhz = freq_hz / 1'000'000;
        double simulated_time_s =
            static_cast<double>(result.cycles_executed) / static_cast<double>(freq_hz);
        if (simulated_time_s >= 1.0) {
            os << "  Simulated time:  " << std::fixed << std::setprecision(3) << simulated_time_s
               << " s @ " << freq_mhz << " MHz\n";
        } else if (simulated_time_s >= 1e-3) {
            os << "  Simulated time:  " << std::fixed << std::setprecision(3)
               << simulated_time_s * 1e3 << " ms @ " << freq_mhz << " MHz\n";
        } else {
            os << "  Simulated time:  " << std::fixed << std::setprecision(3)
               << simulated_time_s * 1e6 << " us @ " << freq_mhz << " MHz\n";
        }
    }

    os << "  Wall time:       " << result.wall_time.count() << " ms\n";
    if (result.wall_time.count() > 0) {
        os << "  Throughput:      " << std::fixed << std::setprecision(2) << result.mcycles_per_sec
           << " Mcycles/sec\n";
    }

    if (result.wasTerminated()) {
        os << "\n=== Termination Info ===\n";
        os << "  Reason:    " << result.termination.reasonString() << "\n";
        os << "  Exit code: " << result.termination.exit_code << "\n";
        os << "  Cycle:     " << result.termination.cycle << "\n";
        if (!result.termination.unit_name.empty()) {
            os << "  Unit:      " << result.termination.unit_name << "\n";
        }
        if (!result.termination.message.empty()) {
            os << "  Message:   " << result.termination.message << "\n";
        }
    }
}

}  // namespace chronon
