// SPDX-License-Identifier: MPL-2.0
#include <cassert>
#include <iostream>

#include "chronon/Chronon.hpp"
#include "clock_reference.hpp"

using namespace chronon;
template <typename F>
void rejects(F&& f) {
    bool caught = false;
    try {
        f();
    } catch (const std::exception&) {
        caught = true;
    }
    assert(caught);
}
int main() {
    static_assert(!CdcPayloadTraits<int*>::value);
    static_assert(CdcPayloadTraits<std::unique_ptr<int>>::value);
    for (size_t d : {0, 1, 3, 7}) rejects([&] { AsyncFifoCircuit<int> fifo({d, 2}); });
    for (size_t n : {0, 1, 65}) rejects([&] { AsyncFifoCircuit<int> fifo({4, n}); });
    AsyncFifoCircuit<int> hand({2, 2});
    const auto initial = hand.state();
    assert(!initial.full && initial.empty && !initial.output_valid);
    assert(initial.ram_occupancy == 0 && initial.writes == 0 && initial.reads == 0);
    assert(initial.write_binary == 0 && initial.read_binary == 0);
    assert(initial.write_gray == 0 && initial.read_gray == 0);
    assert(initial.write_sync == 0 && initial.read_sync == 0);
    rejects([&] { (void)hand.canWrite(); });
    rejects([&] { (void)hand.take(); });
    // 1:1 same phase: write at edge 0; tail at 2; !empty at 3;
    // read accepted/output committed at 4; consumer takes at 5.
    for (uint64_t edge = 0; edge < 6; ++edge) {
        hand.beginEdges(true, true);
        if (edge == 0) assert(hand.tryWrite({1, 42}));
        if (edge < 4) assert(!hand.canRead());
        if (edge == 4) assert(hand.requestRead());
        if (edge < 5) assert(!hand.outputValid());
        if (edge == 5) {
            auto packet = hand.take();
            assert(packet && packet->data == 42);
        }
        hand.commit();
        if (edge < 3) assert(hand.state().empty);
        if (edge == 3) assert(!hand.state().empty);
    }
    // RAM depth two fills at edges 0,1. Read acceptance at 4 frees an entry;
    // full clears at 7 and the write interface can accept again at 8.
    AsyncFifoCircuit<int> full_timeline({2, 2});
    for (unsigned edge = 0; edge <= 8; ++edge) {
        full_timeline.beginEdges(true, true);
        const bool accepted = full_timeline.tryWrite({edge + 1, int(edge)});
        assert(accepted == (edge < 2 || edge == 8));
        if (edge == 4) assert(full_timeline.requestRead());
        full_timeline.commit();
        if (edge >= 1 && edge < 7) assert(full_timeline.state().full);
        if (edge == 7) assert(!full_timeline.state().full);
    }
    AsyncFifoCircuit<std::unique_ptr<int>> owned({2, 2});
    for (unsigned cycle = 0; cycle < 8; ++cycle) {
        owned.beginEdges(true, true);
        if (!cycle) {
            assert(owned.tryWrite({99, std::make_unique<int>(123)}));
        }
        if (auto packet = owned.take())
            assert(packet->transaction_id == 99 && *packet->data == 123);
        owned.requestRead();
        owned.commit();
    }
    assert(owned.drained());

    // Circuit-only asynchronous edge masks, pointer wrap and random backpressure.
    constexpr uint64_t seed = 0x9141326;
    for (size_t depth : {2, 4, 16, 64})
        for (size_t stages : {2, 3, 5}) {
            AsyncFifoCircuit<uint64_t> actual({depth, stages});
            clock_reference::Fifo reference(depth, stages);
            uint64_t wc = 0, rc = 0;
            for (uint64_t step = 0; step < 100'000; ++step) {
                const auto mask = clock_reference::mix(step ^ seed);
                const bool w = (mask & 3) != 0, r = (mask & 12) != 0;
                const bool wen = clock_reference::writeEnabled(wc, 2, seed);
                const bool ren = clock_reference::readEnabled(rc, 2, seed);
                const auto next_packet = reference.writes + 1;
                actual.beginEdges(w, r);
                std::optional<CdcPacket<uint64_t>> taken;
                bool aw = false, ar = false;
                if (w && wen) aw = actual.tryWrite({next_packet, next_packet});
                if (r && ren) {
                    taken = actual.take();
                    ar = actual.requestRead();
                }
                actual.commit();
                reference.step(w, r, wen, ren, wc, rc);
                const auto state = actual.state();
                assert(aw == reference.accepted_write && ar == reference.accepted_read);
                assert((taken ? taken->data : 0) == reference.last_consumed);
                assert(state.full == reference.full && state.empty == reference.empty);
                assert(state.output_valid == bool(reference.output));
                assert(state.ram_occupancy == reference.memory.size());
                assert(state.write_binary == reference.writes % (2 * depth));
                assert(state.read_binary == reference.reads % (2 * depth));
                wc += w;
                rc += r;
            }
        }
    std::cout << "async FIFO: hand timeline, ownership, initialization and 1200000 independent "
                 "circuit steps passed\n";
}
