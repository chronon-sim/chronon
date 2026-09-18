// SPDX-License-Identifier: MPL-2.0
#pragma once
#include "chronon/Chronon.hpp"

namespace chronon::benchmark {
// Fixed public-API workload shared by the invocation benchmark and lifecycle
// differential test. Identical sources also build against the baseline.
struct InvocationUnit : TickableUnit {
    OutPort<uint64_t> out{this, "out", 1};
    InPort<uint64_t> in{this, "in", 16};
    AsyncWritePort<uint64_t> async_out{this, "async_out"};
    AsyncReadPort<uint64_t> async_in{this, "async_in"};
    bool clock, writer;
    size_t work;
    uint64_t count = 0, checksum = 0, digest = 1;
    InvocationUnit(std::string name, bool clock, bool writer, size_t work)
        : TickableUnit(std::move(name)), clock(clock), writer(writer), work(work) {}
    void tick() override {
        for (size_t i = 0; i < work; ++i) {
            digest = digest * 6364136223846793005ULL + i + 1;
            asm volatile("" : "+r"(digest));
        }
        if (writer) {
            if (clock) {
                if (async_out.send({count + 1, count + 1})) ++count;
            } else if (out.canSend() && out.send(count + 1)) {
                ++count;
            }
        } else if (clock) {
            if (auto packet = async_in.take()) {
                if (packet->data != ++count) throw std::runtime_error("FIFO order mismatch");
                checksum += packet->data;
            }
            async_in.requestRead();
        } else if (auto data = in.tryReceive(localCycle())) {
            if (*data != ++count) throw std::runtime_error("port order mismatch");
            checksum += *data;
        }
    }
};

inline std::vector<InvocationUnit*> invocationModel(TickSimulation& sim, bool clock, size_t pairs,
                                                    size_t work, size_t skew) {
    if (clock) {
        sim.addClockDomain(ClockDomain::fromHz(1, "a", 250000000));
        sim.addClockDomain(ClockDomain::fromHz(2, "b", 500000000, 1, SimTime::picoseconds(37)));
    }
    std::vector<InvocationUnit*> units;
    for (size_t i = 0; i < pairs; ++i) {
        const auto create = [&](bool writer, size_t unit_work) {
            const auto name = (writer ? "producer" : "consumer") + std::to_string(i);
            return clock ? sim.createUnitInDomain<InvocationUnit>(writer ? 1 : 2, name, true,
                                                                  writer, unit_work)
                         : sim.createUnit<InvocationUnit>(name, false, writer, unit_work);
        };
        auto* p = create(true, work * (i == 0 ? skew : 1));
        auto* c = create(false, work);
        if (clock)
            sim.connectAsyncFifo(i + 1, p->async_out, c->async_in, {16, 2});
        else
            sim.connect(p->out, c->in, 2);
        units.push_back(p);
        units.push_back(c);
    }
    return units;
}

inline std::vector<uint64_t> invocationState(const std::vector<InvocationUnit*>& units) {
    std::vector<uint64_t> result;
    for (const auto* u : units)
        result.insert(result.end(), {u->localCycle(), u->count, u->checksum, u->digest});
    return result;
}
}  // namespace chronon::benchmark
