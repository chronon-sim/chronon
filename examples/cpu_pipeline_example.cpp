// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

/**
 * @file cpu_pipeline_example.cpp
 *
 * CPU Pipeline Simulation Example using Chronon Sender Framework
 *
 * This example demonstrates high-performance tick-based simulation:
 * - 4-wide superscalar pipeline with 4 ALUs
 * - Branch prediction with dual internal pipelines (tight coupling demo)
 * - Flush engine for branch misprediction recovery
 * - State machine based execution with TickableUnit
 * - TickSimulation with stdexec parallel execution
 * - Port-based communication between pipeline stages
 * - Macro-free observability with auto-assigned IDs
 *
 * Pipeline: Fetch -> Decode (4-wide) -> ALU0/ALU1/ALU2/ALU3 -> Writeback (4-wide)
 * Memory:   I-Cache -> L2
 * Flush:    ALUs -> FlushEngine -> All units (broadcast)
 *
 * Build:
 *   cd build
 *   cmake .. -DCHRONON_BUILD_EXAMPLES=ON
 *   make cpu_pipeline_example
 *
 * Run:
 *   ./examples/cpu_pipeline_example [--threads=N] [--no-lookahead]
 */

#include <chrono>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <thread>

#include "cpu_pipeline_common.hpp"

using namespace chronon;
using namespace cpu_pipeline;

// ============================================================================
// Main
// ============================================================================

int main(int argc, char* argv[]) {
    std::cout << "==============================================\n";
    std::cout << "4-Wide Superscalar CPU Pipeline Simulation\n";
    std::cout << "Using Chronon Sender Framework (Tick Mode)\n";
    std::cout << "==============================================\n\n";

    unsigned int num_threads = 0;  // 0 = auto (hardware_concurrency)
    bool enable_lookahead = true;
    bool enable_observe = true;

    for (int i = 1; i < argc; ++i) {
        if (std::strncmp(argv[i], "--threads=", 10) == 0) {
            num_threads = static_cast<unsigned int>(std::atoi(argv[i] + 10));
        } else if (std::strcmp(argv[i], "--threads") == 0 && i + 1 < argc) {
            num_threads = static_cast<unsigned int>(std::atoi(argv[++i]));
        } else if (std::strcmp(argv[i], "--no-lookahead") == 0) {
            enable_lookahead = false;
        } else if (std::strcmp(argv[i], "--no-observe") == 0) {
            enable_observe = false;
        } else if (std::strcmp(argv[i], "--help") == 0 || std::strcmp(argv[i], "-h") == 0) {
            std::cout << "Usage: " << argv[0] << " [options]\n\n";
            std::cout << "Options:\n";
            std::cout << "  --threads=N       Number of threads (default: auto)\n";
            std::cout << "  --no-lookahead    Disable lookahead (use barrier sync per cycle)\n";
            std::cout << "  --no-observe      Disable all tracing and logging\n";
            std::cout << "  --help, -h        Show this help message\n";
            return 0;
        }
    }

    TickSimulationConfig config;
    config.num_threads = (num_threads > 0) ? num_threads : std::thread::hardware_concurrency();
    config.enable_parallel = (config.num_threads > 1);
    config.enable_lookahead = enable_lookahead;
    config.epoch_size = 1024;

    std::cout << "Configuration:\n";
    std::cout << "  Threads:    " << config.num_threads << "\n";
    std::cout << "  Parallel:   " << (config.enable_parallel ? "yes" : "no") << "\n";
    std::cout << "  Lookahead:  " << (config.enable_lookahead ? "yes" : "no") << "\n";
    std::cout << "  Epoch size: " << config.epoch_size << " cycles\n\n";

    TickSimulation sim(config);

    // ========================================================================
    // Create units for 4-wide superscalar pipeline
    // ========================================================================

    // Frontend: Fetch with branch prediction
    auto* fetch = sim.createUnit<FetchUnit>(
        1000000, 16384);  // 1M instructions, 16K I-Cache lines (covers 1M PCs)

    // Decode: 4-wide decode with round-robin dispatch
    auto* decode = sim.createUnit<DecodeUnit>(4);  // 4 instructions per cycle max

    // Execute: 4 standalone ALUs
    // Each ALU has ~0.1% chance of generating a branch misprediction
    auto* alu0 = sim.createUnit<ALUUnit>(0, 0.001);
    auto* alu1 = sim.createUnit<ALUUnit>(1, 0.001);
    auto* alu2 = sim.createUnit<ALUUnit>(2, 0.001);
    auto* alu3 = sim.createUnit<ALUUnit>(3, 0.001);

    // Backend: 4-wide writeback
    auto* writeback = sim.createUnit<WritebackUnit>();

    // Memory: L2 cache for I-Cache misses
    auto* l2 = sim.createUnit<L2CacheUnit>(10, 1000);  // 10 cycle latency, 1000 L2 lines

    // Flush coordination: receives flush requests, broadcasts to all units
    auto* flush_engine = sim.createUnit<FlushEngine>();

    // ========================================================================
    // Observability Setup
    // ========================================================================

    using namespace observe;

    // Create observation queue and context (always needed for counters)
    ObservationQueue queue(256 * 1024);  // 256KB buffer
    ObservationContext ctx(&queue, [&]() { return sim.currentCycle(); });

    // Optional backend for trace output
    std::unique_ptr<ObservationBackend> backend;

    if (enable_observe) {
        // Enable all trace categories
        ctx.filter().enableCategory(trace_cat::ICACHE_HIT);
        ctx.filter().enableCategory(trace_cat::ICACHE_MISS);
        ctx.filter().enableCategory(trace_cat::FETCH);
        ctx.filter().enableCategory(trace_cat::DECODE);
        ctx.filter().enableCategory(trace_cat::EXECUTE);
        ctx.filter().enableCategory(trace_cat::COMMIT);
        ctx.filter().enableCategory(trace_cat::L2_ACCESS);
        ctx.filter().enableCategory(trace_cat::FLUSH);
        ctx.filter().enableCategory(trace_cat::BRANCH_PRED);
        ctx.filter().enableCategory(trace_cat::DISPATCH);

        // Set log level (Info for production, Debug for verbose)
        ctx.filter().setMinLogLevel(LogLevel::Info);

        // Start backend for file output
        ObservationBackend::Config obs_config;
        obs_config.output_dir = "out";
        backend = std::make_unique<ObservationBackend>(queue, obs_config);
        backend->start();

        std::cout << "Observability enabled: trace output will be written to out/\n\n";
    } else {
        // Disable all traces and logs (counters still work)
        ctx.filter().setMinLogLevel(LogLevel::Error);  // Only errors
        std::cout << "Observability disabled (counters only)\n\n";
    }

    // Attach observation context to all units (needed for counters)
    fetch->setObservationContext(&ctx);
    decode->setObservationContext(&ctx);
    alu0->setObservationContext(&ctx);
    alu1->setObservationContext(&ctx);
    alu2->setObservationContext(&ctx);
    alu3->setObservationContext(&ctx);
    writeback->setObservationContext(&ctx);
    l2->setObservationContext(&ctx);
    flush_engine->setObservationContext(&ctx);

    // ========================================================================
    // Main pipeline connections
    // ========================================================================

    // Fetch -> Decode
    sim.connect(fetch->out_instr, decode->in_instr, 1);

    // Decode -> ALUs (4-wide dispatch)
    sim.connect(decode->out_decoded_0, alu0->in_op, 1);
    sim.connect(decode->out_decoded_1, alu1->in_op, 1);
    sim.connect(decode->out_decoded_2, alu2->in_op, 1);
    sim.connect(decode->out_decoded_3, alu3->in_op, 1);

    // ALUs -> Writeback (4-wide results)
    sim.connect(alu0->out_result, writeback->in_result_0, 1);
    sim.connect(alu1->out_result, writeback->in_result_1, 1);
    sim.connect(alu2->out_result, writeback->in_result_2, 1);
    sim.connect(alu3->out_result, writeback->in_result_3, 1);

    // ========================================================================
    // Memory connections
    // ========================================================================

    sim.connect(fetch->out_icache_miss, l2->in_req, 1);
    sim.connect(l2->out_resp, fetch->in_l2_resp, 1);

    // ========================================================================
    // Flush network: ALUs -> FlushEngine -> All units
    // ========================================================================

    // ALUs send flush requests to FlushEngine (delay=0 for immediate response)
    sim.connect(alu0->out_flush_request, flush_engine->in_flush_request, 0);
    sim.connect(alu1->out_flush_request, flush_engine->in_flush_request, 0);
    sim.connect(alu2->out_flush_request, flush_engine->in_flush_request, 0);
    sim.connect(alu3->out_flush_request, flush_engine->in_flush_request, 0);

    // FlushEngine broadcasts flush to all units (delay=1 for next cycle)
    sim.connect(flush_engine->out_flush_broadcast, fetch->in_flush, 1);
    sim.connect(flush_engine->out_flush_broadcast, decode->in_flush, 1);
    sim.connect(flush_engine->out_flush_broadcast, alu0->in_flush, 1);
    sim.connect(flush_engine->out_flush_broadcast, alu1->in_flush, 1);
    sim.connect(flush_engine->out_flush_broadcast, alu2->in_flush, 1);
    sim.connect(flush_engine->out_flush_broadcast, alu3->in_flush, 1);
    sim.connect(flush_engine->out_flush_broadcast, writeback->in_flush, 1);

    std::cout << "Pipeline Architecture:\n";
    std::cout << "  Frontend:   Fetch (with branch prediction) -> Decode (4-wide)\n";
    std::cout << "  Execute:    ALU0, ALU1, ALU2, ALU3 (4 parallel ALUs)\n";
    std::cout << "  Backend:    Writeback (4-wide)\n";
    std::cout << "  Memory:     I-Cache -> L2\n";
    std::cout << "  Flush:      ALUs -> FlushEngine -> All units\n\n";

    std::cout << "Features:\n";
    std::cout << "  - 4-wide decode with round-robin dispatch\n";
    std::cout << "  - Branch prediction with dual internal pipelines\n";
    std::cout << "  - Random branch mispredictions (~0.1% per ALU op)\n";
    std::cout << "  - Flush recovery with PC redirect\n\n";

    sim.initialize();

    constexpr uint64_t NUM_CYCLES = 1'000'000;  // 1M cycles for demo
    std::cout << "Running simulation for 1M cycles...\n";

    auto start = std::chrono::high_resolution_clock::now();
    uint64_t cycles = sim.run(NUM_CYCLES);
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    // Stop backend to flush all events to disk
    if (backend) {
        backend->stop();
    }

    std::cout << "\n=== Simulation Statistics ===\n";
    std::cout << "  Cycles executed: " << cycles << "\n";
    std::cout << "  Wall time:       " << duration.count() << " ms\n";
    if (duration.count() > 0) {
        double mcycles_per_sec = static_cast<double>(cycles) / duration.count() / 1000.0;
        std::cout << "  Throughput:      " << std::fixed << std::setprecision(2) << mcycles_per_sec
                  << " Mcycles/sec\n";
    }

    // ========================================================================
    // Validation Report
    // ========================================================================

    std::cout << "\n=== Validation Report ===\n";

    // Per-unit validation results
    auto printUnitValidation = [](const char* name, auto* unit) {
        std::cout << "  " << name << ": " << unit->getValidationPassCount() << " pass, "
                  << unit->getValidationFailCount() << " fail"
                  << (unit->validationPassed() ? " [OK]" : " [FAIL]") << "\n";
    };

    printUnitValidation("Decode", decode);
    printUnitValidation("ALU0", alu0);
    printUnitValidation("ALU1", alu1);
    printUnitValidation("ALU2", alu2);
    printUnitValidation("ALU3", alu3);
    printUnitValidation("Writeback", writeback);

    // Overall validation status
    bool all_validation_passed = decode->validationPassed() && alu0->validationPassed() &&
                                 alu1->validationPassed() && alu2->validationPassed() &&
                                 alu3->validationPassed() && writeback->validationPassed();

    std::cout << "\n  Overall Validation: " << (all_validation_passed ? "[PASSED]" : "[FAILED]")
              << "\n";

    if (backend) {
        std::cout << "\nTrace output written to: " << backend->outputDir() << "\n";
    }

    std::cout << "\n=== SIMULATION COMPLETE ===\n";
    return all_validation_passed ? 0 : 1;
}
