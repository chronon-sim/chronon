// SPDX-License-Identifier: MPL-2.0
#pragma once
#include <atomic>
#include <cstddef>

// Linker wrappers count C++ scalar/array allocations in the statically linked
// executable, not malloc, aligned new, shared-library internals, or peak live bytes.
namespace chronon::benchmark {
inline std::atomic<uint64_t> clock_allocations{0};
inline uint64_t clockAllocations() { return clock_allocations.load(std::memory_order_relaxed); }
}  // namespace chronon::benchmark
#ifdef CHRONON_COUNT_CLOCK_ALLOCATIONS
extern "C" void* __real__Znwm(std::size_t);
extern "C" void* __real__Znam(std::size_t);
extern "C" void* __wrap__Znwm(std::size_t size) {
    chronon::benchmark::clock_allocations.fetch_add(1, std::memory_order_relaxed);
    return __real__Znwm(size);
}
extern "C" void* __wrap__Znam(std::size_t size) {
    chronon::benchmark::clock_allocations.fetch_add(1, std::memory_order_relaxed);
    return __real__Znam(size);
}
#endif
