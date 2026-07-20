// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// SPDX-License-Identifier: MPL-2.0

#pragma once

#include <cstddef>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string_view>

namespace chronon::benchmark {

inline void printMachineResult(size_t workers, uint32_t units, uint64_t cycles, double median,
                               double p10, double p90, double cv_percent, double mcycles,
                               double mticks, double memory_ops, double messages, double gib,
                               double blocked, double speedup, std::string_view mode,
                               uint64_t modeled_working_set_bytes,
                               uint64_t estimated_port_storage_bytes) {
    std::ostringstream machine;
    machine << std::setprecision(17) << "RESULT"
            << " workers=" << workers << " units=" << units << " cycles=" << cycles
            << " median_seconds=" << median << " p10_seconds=" << p10 << " p90_seconds=" << p90
            << " cv_percent=" << cv_percent << " mcycles_per_second=" << mcycles
            << " mcycles_units_per_second=" << mticks << " gmemops_per_second=" << memory_ops
            << " mmessages_per_second=" << messages << " payload_gib_per_second=" << gib
            << " blocked_percent=" << blocked << " speedup=" << speedup << " mode=" << mode
            << " modeled_working_set_bytes=" << modeled_working_set_bytes
            << " estimated_port_storage_bytes=" << estimated_port_storage_bytes;
    std::cout << machine.str() << '\n';
}

}  // namespace chronon::benchmark
