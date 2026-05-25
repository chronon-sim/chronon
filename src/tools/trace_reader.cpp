// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// trace_reader.cpp
//
// CLI tool for reading and analyzing binary trace files.
// Supports dumping, filtering, and converting trace data.
//
// Usage:
//   trace_reader info <file.ctrace>     - Show trace file information
//   trace_reader dump <file.ctrace>     - Dump all events as text
//   trace_reader filter <file.ctrace> --cycles 1000-2000  - Filter by cycle range
//   trace_reader convert <file.ctrace> -o output.log      - Convert to text format

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <optional>
#include <string>
#include <vector>

#include "observe/BinaryTraceReader.hpp"

using namespace chronon::observe;

namespace {

void printUsage(const char* prog) {
    std::cerr << "Usage: " << prog << " <command> [options] <file.ctrace>\n"
              << "\nCommands:\n"
              << "  info     Show trace file information\n"
              << "  dump     Dump all events as text\n"
              << "  filter   Filter events by criteria\n"
              << "  convert  Convert to text format\n"
              << "\nOptions:\n"
              << "  --cycles START-END  Filter by cycle range\n"
              << "  --unit NAME         Filter by unit name\n"
              << "  --limit N           Limit output to N events\n"
              << "  -o, --output FILE   Output file (default: stdout)\n"
              << "  -v, --verbose       Verbose output\n"
              << "  -h, --help          Show this help\n";
}

struct Options {
    std::string command;
    std::string input_file;
    std::string output_file;
    std::optional<uint64_t> cycle_start;
    std::optional<uint64_t> cycle_end;
    std::string unit_filter;
    uint64_t limit = 0;
    bool verbose = false;
};

bool parseOptions(int argc, char* argv[], Options& opts) {
    if (argc < 3) {
        return false;
    }

    opts.command = argv[1];
    int i = 2;

    while (i < argc) {
        std::string arg = argv[i];

        if (arg == "-h" || arg == "--help") {
            return false;
        } else if (arg == "-v" || arg == "--verbose") {
            opts.verbose = true;
            i++;
        } else if (arg == "-o" || arg == "--output") {
            if (i + 1 >= argc) {
                std::cerr << "Error: " << arg << " requires an argument\n";
                return false;
            }
            opts.output_file = argv[++i];
            i++;
        } else if (arg == "--cycles") {
            if (i + 1 >= argc) {
                std::cerr << "Error: --cycles requires an argument\n";
                return false;
            }
            std::string range = argv[++i];
            auto dash = range.find('-');
            if (dash != std::string::npos) {
                opts.cycle_start = std::stoull(range.substr(0, dash));
                opts.cycle_end = std::stoull(range.substr(dash + 1));
            } else {
                opts.cycle_start = std::stoull(range);
            }
            i++;
        } else if (arg == "--unit") {
            if (i + 1 >= argc) {
                std::cerr << "Error: --unit requires an argument\n";
                return false;
            }
            opts.unit_filter = argv[++i];
            i++;
        } else if (arg == "--limit") {
            if (i + 1 >= argc) {
                std::cerr << "Error: --limit requires an argument\n";
                return false;
            }
            opts.limit = std::stoull(argv[++i]);
            i++;
        } else if (arg[0] != '-') {
            opts.input_file = arg;
            i++;
        } else {
            std::cerr << "Error: Unknown option: " << arg << "\n";
            return false;
        }
    }

    if (opts.input_file.empty()) {
        std::cerr << "Error: No input file specified\n";
        return false;
    }

    return true;
}

int cmdInfo(const Options& opts) {
    BinaryTraceReader reader;
    if (!reader.open(opts.input_file)) {
        std::cerr << "Error: Failed to open " << opts.input_file << "\n";
        return 1;
    }

    const auto& info = reader.info();

    std::cout << "Trace File Information\n";
    std::cout << "======================\n";
    std::cout << "File: " << opts.input_file << "\n";
    std::cout << "Version: " << info.version_major << "." << info.version_minor << "\n";
    std::cout << "Simulation: " << info.simulation_name << "\n\n";

    std::cout << "Statistics:\n";
    std::cout << "  Total events: " << info.total_events << "\n";
    std::cout << "  Cycle range: " << info.min_cycle << " - " << info.max_cycle << "\n";
    std::cout << "  Duration: " << (info.max_cycle - info.min_cycle) << " cycles\n\n";

    std::cout << "Schema:\n";
    std::cout << "  Format entries: " << info.format_count << "\n";
    std::cout << "  Unit entries: " << info.unit_count << "\n";
    std::cout << "  Blocks: " << info.block_count << "\n\n";

    std::cout << "Compression:\n";
    std::cout << "  Enabled: " << (info.compressed ? "yes" : "no") << "\n";
    if (info.compressed) {
        std::cout << "  Average ratio: " << std::fixed << std::setprecision(2)
                  << (info.avg_compression_ratio * 100) << "%\n";
    }
    std::cout << "\n";

    // Timestamp
    if (info.created_timestamp > 0) {
        time_t ts = static_cast<time_t>(info.created_timestamp);
        std::cout << "Created: " << std::ctime(&ts);
    }

    // Verbose: list formats and units
    if (opts.verbose) {
        std::cout << "\nRegistered Formats:\n";
        for (const auto& fmt : reader.formats()) {
            std::cout << "  [" << fmt.id << "] " << fmt.format_string;
            if (!fmt.file.empty()) {
                std::cout << " (" << fmt.file << ":" << fmt.line << ")";
            }
            std::cout << "\n";
        }

        std::cout << "\nRegistered Units:\n";
        for (const auto& unit : reader.units()) {
            std::cout << "  [" << unit.id << "] " << unit.name;
            if (!unit.type_name.empty()) {
                std::cout << " (" << unit.type_name << ")";
            }
            std::cout << "\n";
        }
    }

    return 0;
}

int cmdDump(const Options& opts, std::ostream& out) {
    BinaryTraceReader reader;
    if (!reader.open(opts.input_file)) {
        std::cerr << "Error: Failed to open " << opts.input_file << "\n";
        return 1;
    }

    uint64_t count = 0;

    while (auto event = reader.readEvent()) {
        // Apply filters
        if (opts.cycle_start && event->cycle < *opts.cycle_start) {
            continue;
        }
        if (opts.cycle_end && event->cycle > *opts.cycle_end) {
            break;  // Events are sorted by cycle
        }
        if (!opts.unit_filter.empty() &&
            event->source_name.find(opts.unit_filter) == std::string::npos) {
            continue;
        }

        // Reconstruct and output message
        std::string message = reader.reconstructMessage(*event);
        if (!event->source_name.empty()) {
            out << "[" << std::setw(10) << event->cycle << "] " << event->source_name << ": "
                << message << "\n";
        } else {
            out << "[" << std::setw(10) << event->cycle << "] " << message << "\n";
        }

        count++;
        if (opts.limit > 0 && count >= opts.limit) {
            break;
        }
    }

    if (opts.verbose) {
        std::cerr << "Dumped " << count << " events\n";
    }

    return 0;
}

int cmdFilter(const Options& opts, std::ostream& out) {
    // Filter is essentially dump with filters applied
    return cmdDump(opts, out);
}

int cmdConvert(const Options& opts, std::ostream& out) {
    // Convert is dump with all events
    Options convert_opts = opts;
    convert_opts.limit = 0;
    return cmdDump(convert_opts, out);
}

}  // anonymous namespace

int main(int argc, char* argv[]) {
    Options opts;
    if (!parseOptions(argc, argv, opts)) {
        printUsage(argv[0]);
        return 1;
    }

    // Setup output stream
    std::ofstream outfile;
    std::ostream* out = &std::cout;
    if (!opts.output_file.empty()) {
        outfile.open(opts.output_file);
        if (!outfile.is_open()) {
            std::cerr << "Error: Failed to open output file " << opts.output_file << "\n";
            return 1;
        }
        out = &outfile;
    }

    // Dispatch command
    if (opts.command == "info") {
        return cmdInfo(opts);
    } else if (opts.command == "dump") {
        return cmdDump(opts, *out);
    } else if (opts.command == "filter") {
        return cmdFilter(opts, *out);
    } else if (opts.command == "convert") {
        return cmdConvert(opts, *out);
    } else {
        std::cerr << "Error: Unknown command: " << opts.command << "\n";
        printUsage(argv[0]);
        return 1;
    }
}
