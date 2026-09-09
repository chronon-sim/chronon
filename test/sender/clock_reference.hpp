// SPDX-License-Identifier: MPL-2.0
// Independent serial oracle. Deliberately includes no Chronon header and uses
// unwrapped binary counters and a deque, not the production Gray/ring circuit.
#pragma once
#include <cstdint>
#include <deque>
#include <optional>
#include <stdexcept>
#include <vector>

namespace clock_reference {
__extension__ using Wide = unsigned __int128;
struct Time {
    Wide numerator;  // rational picoseconds
    uint64_t denominator;
    friend bool operator==(Time a, Time b) {
        return a.numerator * b.denominator == b.numerator * a.denominator;
    }
    friend bool operator<(Time a, Time b) {
        return a.numerator * b.denominator < b.numerator * a.denominator;
    }
    uint64_t ns() const { return static_cast<uint64_t>(numerator / (Wide(denominator) * 1000)); }
};
inline Time edge(uint64_t hz, uint64_t phase_ps, uint64_t cycle) {
    return {Wide(phase_ps) * hz + Wide(cycle) * 1'000'000'000'000ULL, hz};
}
inline uint64_t mix(uint64_t value) {
    value += 0x9e3779b97f4a7c15ULL;
    value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9ULL;
    value = (value ^ (value >> 27)) * 0x94d049bb133111ebULL;
    return value ^ (value >> 31);
}
inline bool writeEnabled(uint64_t cycle, unsigned pattern, uint64_t seed) {
    return pattern == 0 || (pattern == 1 ? cycle % 29 < 11 : (mix(cycle ^ seed) & 7) != 0);
}
inline bool readEnabled(uint64_t cycle, unsigned pattern, uint64_t seed) {
    return pattern == 0 || (pattern == 1 ? cycle % 37 > 12 : (mix(cycle ^ seed ^ 0xa55a) & 7) < 4);
}
struct Event {
    uint32_t domain;
    uint64_t cycle;
    uint16_t kind;
    uint8_t phase;
    uint64_t transaction, value, ordinal;
};
class Fifo {
public:
    Fifo(size_t depth_, size_t stages, bool events_ = false)
        : depth(depth_),
          write_history(stages, 0),
          read_history(stages, 0),
          events_enabled(events_) {}
    size_t depth;
    uint64_t writes = 0, reads = 0, consumed = 0;
    bool full = false, empty = true;
    std::optional<uint64_t> output;
    std::deque<uint64_t> memory;
    std::deque<uint64_t> write_history, read_history;
    std::vector<Event> events;
    bool events_enabled;
    bool accepted_write = false, accepted_read = false;
    uint64_t last_consumed = 0;

    void step(bool w_edge, bool r_edge, bool wen, bool ren, uint64_t wc, uint64_t rc) {
        accepted_write = accepted_read = false;
        last_consumed = 0;
        const auto old_writes = writes, old_reads = reads;
        const auto observed_writes = write_history.back(), observed_reads = read_history.back();
        const bool old_full = full, old_empty = empty;
        if (r_edge && ren && output) {
            last_consumed = *output;
            emit(2, rc, 5, 0, *output);
            if (*output != consumed + 1) throw std::runtime_error("reference order invariant");
            ++consumed;
            output.reset();
        }
        if (w_edge && wen && !full) {
            ++writes;
            accepted_write = true;
        }
        if (r_edge && ren && !empty && !output) {
            ++reads;
            accepted_read = true;
            if (memory.empty()) throw std::runtime_error("reference invalid read");
            output = memory.front();
            memory.pop_front();
        }
        if (accepted_write) memory.push_back(writes);
        if (w_edge) full = writes - observed_reads == depth;
        if (r_edge) empty = reads == observed_writes;
        if (accepted_write) emit(1, wc, 1, 1, writes);
        if (r_edge) {
            for (auto n = visible + 1; n <= observed_writes; ++n) emit(2, rc, 2, 1, n);
            visible = observed_writes;
        }
        if (accepted_read) {
            emit(2, rc, 3, 1, *output);
            emit(2, rc, 4, 1, *output);
        }
        if (full != old_full) emit(1, wc, 6, 1, 0, full);
        if (empty != old_empty) emit(2, rc, 7, 1, 0, empty);
        if (w_edge) {
            read_history.pop_back();
            read_history.push_front(old_reads);
        }
        if (r_edge) {
            write_history.pop_back();
            write_history.push_front(old_writes);
        }
        if (memory.size() != writes - reads || memory.size() > depth) {
            throw std::runtime_error("reference storage invariant");
        }
    }

private:
    uint64_t visible = 0;
    uint64_t ordinals[2]{};
    void emit(uint32_t domain, uint64_t cycle, uint16_t kind, uint8_t phase, uint64_t transaction,
              uint64_t value = 0) {
        if (events_enabled)
            events.push_back(
                {domain, cycle, kind, phase, transaction, value, ordinals[domain - 1]++});
    }
};
}  // namespace clock_reference
