// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// SPDX-License-Identifier: MPL-2.0

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <vector>

#include "EpochFreeDifferentialHarness.hpp"
#include "chronon/Chronon.hpp"

using namespace chronon;
using namespace chronon::sender::test;

namespace {

class ManualUnit : public Unit {
public:
    explicit ManualUnit(std::string name) : Unit(std::move(name)) {}
    void setCycle(uint64_t cycle) { setLocalCycle(cycle); }
};

[[noreturn]] void fail(const std::string& message) { throw std::runtime_error(message); }

void require(bool condition, const std::string& message) {
    if (!condition) fail(message);
}

struct ThrowingMovePayload {
    int value = 0;

    ThrowingMovePayload() = default;
    explicit ThrowingMovePayload(int payload) : value(payload) {}
    ThrowingMovePayload(const ThrowingMovePayload&) = default;
    ThrowingMovePayload& operator=(const ThrowingMovePayload&) = default;
    ThrowingMovePayload(ThrowingMovePayload&&) {
        throw std::runtime_error("ThrowingMovePayload move construction");
    }
    ThrowingMovePayload& operator=(ThrowingMovePayload&&) {
        throw std::runtime_error("ThrowingMovePayload move assignment");
    }
};

template <typename T>
void requireEmpty(InPort<T>& port, uint64_t cycle, const std::string& message) {
    require(!port.tryReceive(cycle).has_value(), message);
}

void testFailedReservationPublishesNothing() {
    ManualUnit producer("producer");
    ManualUnit sink0("sink0");
    ManualUnit sink1("sink1");
    OutPort<int> out0{&producer, "out0", 2};
    OutPort<int> out1{&producer, "out1", 2};
    InPort<int> in0{&sink0, "in0", 1};
    InPort<int> in1{&sink1, "in1", 1};
    out0.connect(&in0, 0);
    out1.connect(&in1, 0);

    producer.setCycle(0);
    require(out1.send(7), "failed to create asymmetric destination backpressure");

    auto tx = reserve(out0, out1);
    require(!tx, "transaction reserved when one destination was full");
    require(out0.sentThisCycle() == 0,
            "failed reservation consumed the first OutPort's cycle credit");
    requireEmpty(in0, 0, "failed reservation partially published to the first destination");
    const auto prefill = in1.tryReceive(0);
    require(prefill.has_value() && *prefill == 7,
            "failed reservation changed the pre-existing destination payload");
    requireEmpty(in1, 0, "failed reservation added a payload to the full destination");
}

void testPositionalCommitIsExactlyOnce() {
    ManualUnit producer("producer");
    ManualUnit sink0("sink0");
    ManualUnit sink1("sink1");
    OutPort<int> out0{&producer, "out0", 1};
    OutPort<uint64_t> out1{&producer, "out1", 1};
    InPort<int> in0{&sink0, "in0", 1};
    InPort<uint64_t> in1{&sink1, "in1", 1};
    out0.connect(&in0, 0);
    out1.connect(&in1, 0);

    producer.setCycle(4);
    auto tx = reserve(out0, out1);
    require(static_cast<bool>(tx), "valid transaction could not reserve both ports");
    require(!out0.canSend() && !out1.canSend(),
            "reservation was not charged to ordinary per-cycle admission");
    require(tx.commit(11, uint64_t{22}), "valid positional commit failed");
    require(!tx.commit(33, uint64_t{44}), "a committed transaction was reusable");

    const auto value0 = in0.tryReceive(4);
    const auto value1 = in1.tryReceive(4);
    require(value0.has_value() && *value0 == 11, "first committed payload was incorrect");
    require(value1.has_value() && *value1 == 22, "second committed payload was incorrect");
    requireEmpty(in0, 4, "retry duplicated the first committed payload");
    requireEmpty(in1, 4, "retry duplicated the second committed payload");
}

void testStagedCommitSupportsRuntimeSelectedSameTypePort() {
    ManualUnit producer("producer");
    ManualUnit sink0("sink0");
    ManualUnit sink1("sink1");
    ManualUnit rob_sink("rob_sink");
    OutPort<int> iq0{&producer, "iq0", 1};
    OutPort<int> iq1{&producer, "iq1", 1};
    OutPort<int> rob{&producer, "rob", 1};
    InPort<int> in0{&sink0, "in0", 1};
    InPort<int> in1{&sink1, "in1", 1};
    InPort<int> in_rob{&rob_sink, "in_rob", 1};
    iq0.connect(&in0, 0);
    iq1.connect(&in1, 0);
    rob.connect(&in_rob, 0);

    producer.setCycle(8);
    OutPort<int>* selected_iq = &iq1;
    auto tx = reserve(*selected_iq, rob);
    require(static_cast<bool>(tx), "runtime-selected IQ transaction did not reserve");
    require(tx.send(*selected_iq, 81), "failed to stage runtime-selected IQ payload");
    require(tx.send(rob, 81), "failed to stage ROB payload");
    require(tx.commit(), "staged transaction failed to commit");

    requireEmpty(in0, 8, "transaction published to the unselected IQ");
    const auto iq_value = in1.tryReceive(8);
    const auto rob_value = in_rob.tryReceive(8);
    require(iq_value.has_value() && *iq_value == 81,
            "selected IQ did not receive its staged payload");
    require(rob_value.has_value() && *rob_value == 81,
            "ROB did not receive the matching staged payload");
}

void testOutPortCapacityChangeInvalidatesClaim() {
    ManualUnit producer("producer");
    ManualUnit sink0("sink0");
    ManualUnit sink1("sink1");
    OutPort<int> out0{&producer, "out0", 2};
    OutPort<int> out1{&producer, "out1", 2};
    InPort<int> in0{&sink0, "in0", 2};
    InPort<int> in1{&sink1, "in1", 2};
    out0.connect(&in0, 0);
    out1.connect(&in1, 0);

    producer.setCycle(3);
    auto invalidated = reserve(out0, out1);
    require(static_cast<bool>(invalidated), "capacity-change test could not reserve");
    out1.setPerCycleCapacity(1);
    require(!invalidated.commit(1, 1), "capacity mutation did not invalidate reservation");
    requireEmpty(in0, 3, "invalidated capacity transaction partially published first payload");
    requireEmpty(in1, 3, "invalidated capacity transaction partially published second payload");
    require(out0.sentThisCycle() == 0 && out1.sentThisCycle() == 0,
            "invalidated capacity transaction leaked cycle credit");

    auto retry = reserve(out0, out1);
    require(static_cast<bool>(retry) && retry.commit(2, 2),
            "released capacity claim could not be retried");
}

void testDestinationCapacityChangeInvalidatesClaim() {
    ManualUnit producer("producer");
    ManualUnit sink0("sink0");
    ManualUnit sink1("sink1");
    OutPort<int> out0{&producer, "out0", 2};
    OutPort<int> out1{&producer, "out1", 2};
    InPort<int> in0{&sink0, "in0", 2};
    InPort<int> in1{&sink1, "in1", 2};
    out0.connect(&in0, 0);
    out1.connect(&in1, 0);

    producer.setCycle(5);
    auto tx = reserve(out0, out1);
    require(static_cast<bool>(tx), "destination-capacity test could not reserve");
    in1.setCapacity(1);
    require(!tx.commit(5, 5), "destination capacity mutation did not invalidate reservation");
    requireEmpty(in0, 5, "destination mutation partially published first payload");
    requireEmpty(in1, 5, "destination mutation partially published second payload");
}

void testTopologyChangeReleasesOnlyOriginallyClaimedConnections() {
    ManualUnit producer("producer");
    ManualUnit sink0("sink0");
    ManualUnit sink1("sink1");
    ManualUnit added_sink("added_sink");
    OutPort<int> out0{&producer, "out0", 3};
    OutPort<int> out1{&producer, "out1", 1};
    InPort<int> in0{&sink0, "in0", 4};
    InPort<int> in1{&sink1, "in1", 2};
    InPort<int> added_in{&added_sink, "added_in", 4};
    out0.connect(&in0, 0);
    out1.connect(&in1, 0);

    producer.setCycle(13);
    auto invalidated = reserve(out0, out1);
    require(static_cast<bool>(invalidated), "topology-change test could not reserve");

    auto* added_connection = out0.connect(&added_in, 0);
    added_connection->configureRegisteredEdge(std::nullopt, size_t{1});
    require(out0.send(70), "ordinary send could not use the newly added connection");
    require(!invalidated.commit(80, 80), "topology mutation did not invalidate transaction");

    // Releasing the invalid transaction must not refund the new connection's
    // independent rate credit: it was not part of the original claim set.
    require(!out0.send(71), "transaction release refunded an unclaimed connection");
    const auto old_value = in0.tryReceive(13);
    const auto added_value = added_in.tryReceive(13);
    require(old_value.has_value() && *old_value == 70,
            "topology mutation changed the original connection payload");
    require(added_value.has_value() && *added_value == 70,
            "topology mutation changed the added connection payload");
    requireEmpty(in0, 13, "failed retry partially published to original connection");
    requireEmpty(added_in, 13, "failed retry partially published to added connection");
    requireEmpty(in1, 13, "invalidated transaction published its peer payload");

    producer.setCycle(14);
    auto retry = reserve(out0, out1);
    require(static_cast<bool>(retry) && retry.commit(72, 72),
            "topology-mutated port could not reserve on the next cycle");
    const auto retried_old = in0.tryReceive(14);
    const auto retried_added = added_in.tryReceive(14);
    const auto retried_peer = in1.tryReceive(14);
    require(retried_old.has_value() && *retried_old == 72, "retry missed original connection");
    require(retried_added.has_value() && *retried_added == 72, "retry missed added connection");
    require(retried_peer.has_value() && *retried_peer == 72, "retry missed peer port");
}

void testCancelBetweenReserveAndCommitDeliversNone() {
    ManualUnit producer("producer");
    ManualUnit sink0("sink0");
    ManualUnit sink1("sink1");
    OutPort<int> out0{&producer, "out0", 1};
    OutPort<int> out1{&producer, "out1", 1};
    InPort<int> in0{&sink0, "in0", 1};
    InPort<int> in1{&sink1, "in1", 1};
    out0.connect(&in0, 0);
    out1.connect(&in1, 0);

    producer.setCycle(6);
    auto tx = reserve(out0, out1);
    require(static_cast<bool>(tx), "cancel test could not reserve");
    out0.cancelInFlight();
#if CHRONON_ENABLE_OUTPORT_CANCELLATION
    require(!tx.commit(6, 6), "sender cancellation did not invalidate reservation");
    requireEmpty(in0, 6, "canceled transaction published first payload");
    requireEmpty(in1, 6, "canceled transaction published second payload");

    auto retry = reserve(out0, out1);
    require(static_cast<bool>(retry) && retry.commit(7, 7),
            "canceled transaction did not release all claims");
#else
    require(tx.commit(6, 6), "non-cancelable build changed transaction semantics");
#endif
}

void testReceiverFlushDoesNotCancelUnpublishedTransaction() {
    ManualUnit producer("producer");
    ManualUnit sink0("sink0");
    ManualUnit sink1("sink1");
    OutPort<int> out0{&producer, "out0", 2};
    OutPort<int> out1{&producer, "out1", 1};
    InPort<int> in0{&sink0, "in0", 2};
    InPort<int> in1{&sink1, "in1", 1};
    out0.connect(&in0, 0);
    out1.connect(&in1, 0);

    producer.setCycle(9);
    require(out0.send(90), "failed to enqueue receiver-flush prefix");
    auto tx = reserve(out0, out1);
    require(static_cast<bool>(tx), "receiver-flush test could not reserve");
    in0.flush();
    require(tx.commit(91, 91), "receiver flush canceled a not-yet-published transaction");

    const auto value0 = in0.tryReceive(9);
    const auto value1 = in1.tryReceive(9);
    require(value0.has_value() && *value0 == 91,
            "receiver flush did not distinguish reserved post-flush payload");
    require(value1.has_value() && *value1 == 91,
            "receiver flush changed the matching transaction payload");
    requireEmpty(in0, 9, "receiver flush retained the pre-flush prefix");
}

void testCycleBoundaryAndExplicitRelease() {
    ManualUnit producer("producer");
    ManualUnit sink0("sink0");
    ManualUnit sink1("sink1");
    OutPort<int> out0{&producer, "out0", 1};
    OutPort<int> out1{&producer, "out1", 1};
    InPort<int> in0{&sink0, "in0", 1};
    InPort<int> in1{&sink1, "in1", 1};
    out0.connect(&in0, 0);
    out1.connect(&in1, 0);

    producer.setCycle(1);
    auto expired = reserve(out0, out1);
    require(static_cast<bool>(expired), "cycle-boundary test could not reserve");
    producer.setCycle(2);
    require(!expired.commit(1, 1), "transaction survived its producer cycle");
    requireEmpty(in0, 2, "expired transaction published first payload");
    requireEmpty(in1, 2, "expired transaction published second payload");

    auto released = reserve(out0, out1);
    require(static_cast<bool>(released), "explicit-release test could not reserve");
    released.cancel();
    require(!released.commit(2, 2), "explicitly released transaction committed");
    auto retry = reserve(out0, out1);
    require(static_cast<bool>(retry) && retry.commit(3, 3),
            "explicit release leaked a reservation claim");
}

void testInvalidPortSetsAreRejected() {
    ManualUnit producer0("producer0");
    ManualUnit producer1("producer1");
    OutPort<int> out0{&producer0, "out0", 1};
    OutPort<int> out1{&producer1, "out1", 1};

    producer0.setCycle(0);
    producer1.setCycle(0);
    auto duplicate = reserve(out0, out0);
    require(!duplicate, "transaction accepted the same OutPort twice");
    auto cross_owner = reserve(out0, out1);
    require(!cross_owner, "transaction accepted ports owned by different Units");
}

void testNoopThrowingPayloadCannotPartiallyCommit() {
    ManualUnit producer("producer");
    ManualUnit sink("sink");
    ManualUnit dependency_sink("dependency_sink");
    OutPort<int> real{&producer, "real", 3};
    OutPort<ThrowingMovePayload> dependency_only{&producer, "dependency_only", 1};
    OutPort<ThrowingMovePayload> unconnected{&producer, "unconnected", 1};
    InPort<int> real_in{&sink, "real_in", 3};
    InPort<ThrowingMovePayload> dependency_in{&dependency_sink, "dependency_in", 1};
    real.connect(&real_in, 0);
    dependency_only.connect(&dependency_in, 0);
    dependency_only.setDependencyOnlyTransport();

    const ThrowingMovePayload payload{42};

    producer.setCycle(9);
    auto positional = reserve(real, dependency_only);
    require(static_cast<bool>(positional),
            "dependency-only throwing payload could not reserve positionally");
    require(positional.commit(90, payload),
            "dependency-only throwing payload failed positional commit");
    const auto positional_value = real_in.tryReceive(9);
    require(positional_value.has_value() && *positional_value == 90,
            "positional no-op payload changed the real transaction delivery");
    requireEmpty(dependency_in, 9, "dependency-only transaction published a payload");

    producer.setCycle(10);
    auto staged = reserve(real, dependency_only);
    require(static_cast<bool>(staged),
            "dependency-only throwing payload could not reserve for staging");
    require(staged.send(real, 100), "failed to stage the real transaction payload");
    require(staged.send(dependency_only, payload),
            "failed to stage the dependency-only transaction payload");
    require(staged.commit(), "dependency-only throwing payload failed staged commit");
    const auto staged_value = real_in.tryReceive(10);
    require(staged_value.has_value() && *staged_value == 100,
            "staged no-op payload changed the real transaction delivery");
    requireEmpty(dependency_in, 10, "staged dependency-only transaction published a payload");

    producer.setCycle(11);
    auto no_connection = reserve(real, unconnected);
    require(static_cast<bool>(no_connection), "unconnected throwing payload could not reserve");
    require(no_connection.commit(110, payload),
            "unconnected throwing payload failed positional commit");
    const auto unconnected_value = real_in.tryReceive(11);
    require(unconnected_value.has_value() && *unconnected_value == 110,
            "unconnected no-op payload changed the real transaction delivery");
}

void testMixedDependencyOnlyEdgeParticipatesWithoutPayload() {
    ManualUnit producer("producer");
    ManualUnit real_sink("real_sink");
    ManualUnit dependency_sink("dependency_sink");
    ManualUnit peer_sink("peer_sink");
    OutPort<int> mixed{&producer, "mixed", 2};
    OutPort<int> peer{&producer, "peer", 2};
    InPort<int> real_in{&real_sink, "real_in", 4};
    InPort<int> dependency_in{&dependency_sink, "dependency_in", 1};
    InPort<int> peer_in{&peer_sink, "peer_in", 1};
    auto* real_edge = mixed.connect(&real_in, 0);
    auto* dependency_edge = mixed.connect(&dependency_in, 0);
    peer.connect(&peer_in, 0);
    real_edge->configureRegisteredEdge(std::nullopt, size_t{2});
    dependency_edge->configureRegisteredEdge(std::nullopt, size_t{1});
    dependency_edge->setDependencyOnlyTransport(true, 2);

    producer.setCycle(12);
    auto committed = reserve(mixed, peer);
    require(static_cast<bool>(committed),
            "mixed payload/dependency-only OutPort could not reserve");
    require(committed.commit(120, 121),
            "mixed payload/dependency-only transaction could not commit");
    const auto real_value = real_in.tryReceive(12);
    const auto peer_value = peer_in.tryReceive(12);
    require(real_value.has_value() && *real_value == 120,
            "mixed transaction lost its physical payload");
    require(peer_value.has_value() && *peer_value == 121,
            "mixed transaction lost its peer payload");
    requireEmpty(dependency_in, 12, "mixed transaction published on a dependency-only edge");
    require(!mixed.canSend(),
            "committed transaction did not account dependency-only edge rate credit");

    producer.setCycle(13);
    require(peer.send(130), "failed to backpressure the mixed-edge peer destination");
    auto rejected = reserve(mixed, peer);
    require(!rejected, "mixed-edge transaction ignored peer backpressure");
    require(mixed.canSend(),
            "failed mixed-edge reservation did not release dependency-only credit");
    require(mixed.send(131), "ordinary send could not reuse a released mixed-edge reservation");
    const auto retried_value = real_in.tryReceive(13);
    require(retried_value.has_value() && *retried_value == 131,
            "released mixed-edge reservation lost the ordinary payload");
    requireEmpty(dependency_in, 13, "ordinary mixed-edge send published on a dependency-only edge");
}

void testSharedBoundedDestinationIsRejectedBeforeClaim() {
    ManualUnit producer("producer");
    ManualUnit sink("sink");
    OutPort<int> out0{&producer, "out0", 2};
    OutPort<int> out1{&producer, "out1", 2};
    InPort<int> bounded{&sink, "bounded", 1};
    out0.connect(&bounded, 0);
    out1.connect(&bounded, 0);

    producer.setCycle(10);
    auto tx = reserve(out0, out1);
    require(!tx, "transaction accepted two claims on one bounded destination");
    require(out0.sentThisCycle() == 0 && out1.sentThisCycle() == 0,
            "rejected shared-destination transaction consumed port credit");

    require(out0.send(10), "rejected transaction leaked the first connection's claim");
    require(!out1.send(11), "bounded destination accepted a second ordinary payload");
    const auto value = bounded.tryReceive(10);
    require(value.has_value() && *value == 10,
            "shared-destination rejection changed the surviving payload");
    requireEmpty(bounded, 10, "shared-destination rejection partially published a transaction");
}
void testOrdinarySendInvalidatesClaimedBoundedDestination() {
    ManualUnit producer("producer");
    ManualUnit sink("sink");
    OutPort<int> transactional{&producer, "transactional", 2};
    OutPort<int> ordinary{&producer, "ordinary", 2};
    InPort<int> bounded{&sink, "bounded", 1};
    transactional.connect(&bounded, 0);
    ordinary.connect(&bounded, 0);
    producer.setCycle(14);
    auto claimed = reserve(transactional);
    require(static_cast<bool>(claimed), "bounded transaction could not claim an empty destination");
    require(ordinary.send(141), "ordinary peer send unexpectedly observed the private claim");
    require(!claimed.commit(140),
            "transaction ignored an ordinary peer send to its claimed destination");
    const auto ordinary_value = bounded.tryReceive(14);
    require(ordinary_value.has_value() && *ordinary_value == 141,
            "invalidated transaction changed the ordinary peer payload");
    requireEmpty(bounded, 14, "invalidated transaction overfilled the bounded destination");
    producer.setCycle(15);
    auto retry = reserve(transactional);
    require(static_cast<bool>(retry) && retry.commit(150),
            "ordinary peer invalidation leaked the destination transaction claim");
    require(bounded.tryReceive(15) == std::optional<int>{150},
            "retried bounded transaction lost its payload");
}

void testIndependentTransactionsSerializeBoundedDestinationClaims() {
    ManualUnit producer("producer");
    ManualUnit sink("sink");
    OutPort<int> out0{&producer, "out0", 2};
    OutPort<int> out1{&producer, "out1", 2};
    InPort<int> bounded{&sink, "bounded", 1};
    out0.connect(&bounded, 0);
    out1.connect(&bounded, 0);
    producer.setCycle(16);
    auto first = reserve(out0);
    require(static_cast<bool>(first), "first independent transaction could not claim destination");
    auto overlapping = reserve(out1);
    require(!overlapping, "independent transaction stole an active destination claim");
    first.cancel();
    auto retry = reserve(out1);
    require(static_cast<bool>(retry) && retry.commit(160),
            "canceling an independent transaction did not release its destination claim");
    const auto value = bounded.tryReceive(16);
    require(value.has_value() && *value == 160,
            "serialized independent transaction lost its payload");
    requireEmpty(bounded, 16, "serialized independent transactions duplicated a payload");
}

void testRepeatedBoundedEdgeOnOnePortIsRejected() {
    ManualUnit producer("producer");
    ManualUnit sink("sink");
    OutPort<int> out{&producer, "out", 2};
    InPort<int> bounded{&sink, "bounded", 2};
    out.connect(&bounded, 0);
    out.connect(&bounded, 0);

    producer.setCycle(11);
    auto tx = reserve(out);
    require(!tx, "transaction accepted repeated physical edges to one bounded destination");
    require(out.sentThisCycle() == 0,
            "repeated-edge rejection consumed the OutPort's cycle credit");
}

void testSharedFiniteSPSCTransportIsRejected() {
    ManualUnit producer("producer");
    ManualUnit sink("sink");
    OutPort<int> out0{&producer, "out0", 2};
    OutPort<int> out1{&producer, "out1", 2};
    InPort<int> unbounded{&sink, "unbounded"};
    out0.connect(&unbounded, 0);
    out1.connect(&unbounded, 0);
    unbounded.useLockFreeQueue(2);

    require(unbounded.configuredCapacity() == InPort<int>::UNLIMITED_CAPACITY,
            "finite-transport test accidentally bounded model admission");
    require(unbounded.storageCapacity() != InPort<int>::UNLIMITED_CAPACITY,
            "finite-transport test did not install bounded SPSC storage");
    producer.setCycle(12);
    auto tx = reserve(out0, out1);
    require(!tx, "transaction accepted duplicate claims on one finite SPSC transport");
    require(out0.sentThisCycle() == 0 && out1.sentThisCycle() == 0,
            "finite-SPSC rejection consumed port credit");
}

void testSharedUnboundedDestinationRemainsSupported() {
    ManualUnit producer("producer");
    ManualUnit sink("sink");
    OutPort<int> out0{&producer, "out0", 1};
    OutPort<int> out1{&producer, "out1", 1};
    InPort<int> unbounded{&sink, "unbounded"};
    out0.connect(&unbounded, 0);
    out1.connect(&unbounded, 0);

    producer.setCycle(12);
    auto tx = reserve(out0, out1);
    require(static_cast<bool>(tx) && tx.commit(12, 13),
            "transaction rejected a shared unbounded destination");
    const auto first = unbounded.tryReceive(12);
    const auto second = unbounded.tryReceive(12);
    require(first.has_value() && *first == 12,
            "shared unbounded destination lost the first payload");
    require(second.has_value() && *second == 13,
            "shared unbounded destination lost the second payload");
    requireEmpty(unbounded, 12, "shared unbounded transaction duplicated a payload");
}

void testSharedUnboundedMPSCDestinationUsesIndependentLanes() {
    ManualUnit producer("producer");
    ManualUnit peer("peer");
    ManualUnit sink("sink");
    OutPort<int> out0{&producer, "out0", 1};
    OutPort<int> out1{&producer, "out1", 1};
    OutPort<int> peer_out{&peer, "peer_out", 1};
    InPort<int> unbounded{&sink, "unbounded"};
    auto* connection0 = out0.connect(&unbounded, 0);
    auto* connection1 = out1.connect(&unbounded, 0);
    auto* peer_connection = peer_out.connect(&unbounded, 0);

    std::array<Connection<int>*, 3> connections{connection0, connection1, peer_connection};
    for (uint32_t i = 0; i < connections.size(); ++i) {
        connections[i]->setConnId(i);
        connections[i]->optimizeForMPSC();
        const size_t lane = connections[i]->registerProducerThread(i + 1);
        require(lane != SIZE_MAX, "unbounded MPSC lane registration failed");
        connections[i]->setThreadQueueId(lane);
        require(connections[i]->registerOnDestMPSC() != nullptr,
                "unbounded MPSC destination registration failed");
    }

    producer.setCycle(13);
    peer.setCycle(13);
    auto tx = reserve(out0, out1);
    require(static_cast<bool>(tx) && tx.commit(130, 131),
            "transaction rejected independent unbounded MPSC lanes");
    require(peer_out.send(132), "peer could not publish through its independent MPSC lane");

    const auto first = unbounded.tryReceive(13);
    const auto second = unbounded.tryReceive(13);
    const auto third = unbounded.tryReceive(13);
    require(first.has_value() && *first == 130, "first transaction MPSC lane was reordered");
    require(second.has_value() && *second == 131, "second transaction MPSC lane was reordered");
    require(third.has_value() && *third == 132, "peer MPSC lane was lost or reordered");
    requireEmpty(unbounded, 13, "unbounded MPSC transaction duplicated a payload");
}

void testTransactionSpansSharedBroadcastTransport() {
    ManualUnit producer("producer");
    ManualUnit broadcast_sink0("broadcast_sink0");
    ManualUnit broadcast_sink1("broadcast_sink1");
    ManualUnit peer_sink("peer_sink");
    OutPort<int> broadcast{&producer, "broadcast", 2};
    OutPort<int> peer{&producer, "peer", 2};
    InPort<int> broadcast_in0{&broadcast_sink0, "broadcast_in0"};
    InPort<int> broadcast_in1{&broadcast_sink1, "broadcast_in1"};
    InPort<int> peer_in{&peer_sink, "peer_in", 2};
    broadcast.connect(&broadcast_in0, 1);
    broadcast.connect(&broadcast_in1, 1);
    peer.connect(&peer_in, 1);
    require(broadcast.enableTransparentBroadcast(4),
            "shared-broadcast transaction test could not enable transport");

    producer.setCycle(20);
    auto tx = reserve(broadcast, peer);
    require(static_cast<bool>(tx) && tx.commit(200, 201),
            "shared-broadcast transaction failed to commit");
    const auto broadcast_value0 = broadcast_in0.tryReceive(21);
    const auto broadcast_value1 = broadcast_in1.tryReceive(21);
    const auto peer_value = peer_in.tryReceive(21);
    require(broadcast_value0.has_value() && *broadcast_value0 == 200,
            "first shared consumer missed transaction payload");
    require(broadcast_value1.has_value() && *broadcast_value1 == 200,
            "second shared consumer missed transaction payload");
    require(peer_value.has_value() && *peer_value == 201,
            "physical peer missed shared transaction payload");

#if CHRONON_ENABLE_OUTPORT_CANCELLATION
    producer.setCycle(21);
    auto canceled = reserve(broadcast, peer);
    require(static_cast<bool>(canceled), "shared cancellation test could not reserve");
    broadcast.cancelInFlight();
    require(!canceled.commit(210, 211),
            "shared sender cancellation did not invalidate transaction");

    auto retry = reserve(broadcast, peer);
    require(static_cast<bool>(retry) && retry.commit(212, 213),
            "shared sender cancellation leaked transaction credit");
    const auto retried_broadcast0 = broadcast_in0.tryReceive(22);
    const auto retried_broadcast1 = broadcast_in1.tryReceive(22);
    const auto retried_peer = peer_in.tryReceive(22);
    require(retried_broadcast0.has_value() && *retried_broadcast0 == 212,
            "first shared consumer received canceled or missing retry payload");
    require(retried_broadcast1.has_value() && *retried_broadcast1 == 212,
            "second shared consumer received canceled or missing retry payload");
    require(retried_peer.has_value() && *retried_peer == 213,
            "physical peer received canceled or missing retry payload");
    requireEmpty(broadcast_in0, 22, "canceled shared payload remained on first consumer");
    requireEmpty(broadcast_in1, 22, "canceled shared payload remained on second consumer");
    requireEmpty(peer_in, 22, "canceled peer payload was published");
#endif
}

void testTransactionSpansSPSCAndMPSC() {
    ManualUnit producer("producer");
    ManualUnit peer("peer");
    ManualUnit spsc_sink("spsc_sink");
    ManualUnit mpsc_sink("mpsc_sink");
    OutPort<int> out_spsc{&producer, "out_spsc", 1};
    OutPort<int> out_mpsc{&producer, "out_mpsc", 1};
    OutPort<int> peer_out{&peer, "peer_out", 1};
    InPort<int> in_spsc{&spsc_sink, "in_spsc", 2};
    InPort<int> in_mpsc{&mpsc_sink, "in_mpsc", 2};

    auto* spsc = out_spsc.connect(&in_spsc, 1);
    spsc->optimizeForSPSC();

    auto* mpsc0 = out_mpsc.connect(&in_mpsc, 1);
    auto* mpsc1 = peer_out.connect(&in_mpsc, 1);
    mpsc0->setConnId(0);
    mpsc1->setConnId(1);
    mpsc0->optimizeForMPSC();
    mpsc1->optimizeForMPSC();
    const size_t lane0 = mpsc0->registerProducerThread(1);
    const size_t lane1 = mpsc1->registerProducerThread(2);
    require(lane0 != SIZE_MAX && lane1 != SIZE_MAX, "MPSC lane registration failed");
    mpsc0->setThreadQueueId(lane0);
    mpsc1->setThreadQueueId(lane1);
    require(mpsc0->registerOnDestMPSC() != nullptr && mpsc1->registerOnDestMPSC() != nullptr,
            "MPSC destination registration failed");

    producer.setCycle(0);
    peer.setCycle(0);
    auto tx = reserve(out_spsc, out_mpsc);
    require(static_cast<bool>(tx) && tx.commit(100, 100), "SPSC/MPSC transaction failed to commit");
    require(peer_out.send(200), "peer MPSC producer failed to publish");

    const auto spsc_value = in_spsc.tryReceive(1);
    mpsc_sink.setCycle(1);
    in_mpsc.prepareConsumerCycle(1);
    const auto mpsc_first = in_mpsc.tryReceive(1);
    const auto mpsc_second = in_mpsc.tryReceive(1);
    require(spsc_value.has_value() && *spsc_value == 100,
            "SPSC side of mixed transaction was lost");
    require(mpsc_first.has_value() && *mpsc_first == 100,
            "MPSC side of mixed transaction was lost or reordered");
    require(mpsc_second.has_value() && *mpsc_second == 200,
            "peer MPSC payload was lost or reordered");
}

constexpr uint64_t kTransactionMessages = 64;
constexpr uint64_t kPeerMessages = 40;
constexpr uint64_t kDifferentialCycles = 800;

struct TransactionMessage {
    uint64_t sequence = 0;
    uint64_t send_cycle = 0;
    uint32_t source = 0;
};

uint64_t burn(uint64_t value, uint32_t iterations) noexcept {
    for (uint32_t i = 0; i < iterations; ++i) {
        value = value * 6364136223846793005ULL + 1442695040888963407ULL;
        value ^= value >> 29;
    }
    return value;
}

class TransactionProducer final : public TickableUnit {
public:
    TransactionProducer() : TickableUnit("transaction_producer"), events_(0) {
        events_.reserve(kDifferentialCycles * 3);
    }

    OutPort<TransactionMessage> out_spsc{this, "out_spsc", 1};
    OutPort<TransactionMessage> out_mpsc{this, "out_mpsc", 1};

    void tick() override {
        if (next_sequence_ == kTransactionMessages) return;

        const TransactionMessage message{
            .sequence = next_sequence_, .send_cycle = localCycle(), .source = 0};
        auto tx = reserve(out_spsc, out_mpsc);
        if (!tx) {
            ++backpressure_count_;
            events_.record(localCycle(), ModelEventKind::Backpressure, next_sequence_);
            return;
        }

        require(tx.send(out_spsc, message), "epoch-free producer failed to stage SPSC payload");
        require(tx.send(out_mpsc, message), "epoch-free producer failed to stage MPSC payload");

        bool committed = false;
        if (!sender_cancel_exercised_) {
            // No older payload exists yet, so this isolates reserve/cancel from
            // cancellation of already-committed traffic.
#if CHRONON_ENABLE_OUTPORT_CANCELLATION
            out_mpsc.cancelInFlight();
#else
            tx.cancel();
#endif
            committed = tx.commit();
            sender_cancel_exercised_ = true;
            require(!committed, "epoch-free sender cancellation committed a transaction");
        } else if (next_sequence_ == 11 && !explicit_release_exercised_) {
            tx.cancel();
            explicit_release_exercised_ = true;
            committed = tx.commit();
            require(!committed, "explicitly released epoch-free transaction committed");
        } else {
            committed = tx.commit();
            require(committed, "valid epoch-free transaction failed after reservation");
        }

        events_.record(localCycle(), ModelEventKind::SendResult, next_sequence_, committed);
        if (committed) ++next_sequence_;
        state_ = burn(state_ ^ next_sequence_, 2400);
        events_.record(localCycle(), ModelEventKind::State, next_sequence_, state_);
    }

    [[nodiscard]] uint64_t committed() const noexcept { return next_sequence_; }
    [[nodiscard]] bool senderCancelExercised() const noexcept { return sender_cancel_exercised_; }
    [[nodiscard]] bool explicitReleaseExercised() const noexcept {
        return explicit_release_exercised_;
    }
    [[nodiscard]] uint64_t backpressureCount() const noexcept { return backpressure_count_; }
    UnitEventLog& eventLog() noexcept { return events_; }

private:
    uint64_t next_sequence_ = 0;
    uint64_t backpressure_count_ = 0;
    uint64_t state_ = 0x6a09e667f3bcc909ULL;
    bool sender_cancel_exercised_ = false;
    bool explicit_release_exercised_ = false;
    UnitEventLog events_;
};

class PeerProducer final : public TickableUnit {
public:
    PeerProducer() : TickableUnit("peer_producer"), events_(1) {
        events_.reserve(kDifferentialCycles * 2);
    }

    OutPort<TransactionMessage> out{this, "out", 1};

    void tick() override {
        if (next_sequence_ == kPeerMessages) return;
        const TransactionMessage message{
            .sequence = next_sequence_, .send_cycle = localCycle(), .source = 1};
        const bool sent = out.send(message);
        events_.record(localCycle(), ModelEventKind::SendResult, next_sequence_, sent);
        if (sent) ++next_sequence_;
        state_ = burn(state_ ^ next_sequence_, 80);
        events_.record(localCycle(), ModelEventKind::State, next_sequence_, state_);
    }

    [[nodiscard]] uint64_t sent() const noexcept { return next_sequence_; }
    UnitEventLog& eventLog() noexcept { return events_; }

private:
    uint64_t next_sequence_ = 0;
    uint64_t state_ = 0xbb67ae8584caa73bULL;
    UnitEventLog events_;
};

class TransactionSink final : public TickableUnit {
public:
    TransactionSink(std::string name, uint32_t component, bool accepts_peer)
        : TickableUnit(std::move(name)), accepts_peer_(accepts_peer), events_(component) {
        events_.reserve(kDifferentialCycles * 3);
        transaction_sequences_.reserve(kTransactionMessages);
    }

    InPort<TransactionMessage> in{this, "in", 4};

    void tick() override {
        const size_t budget = (localCycle() % 4 == 0) ? 2 : 0;
        for (size_t slot = 0; slot < budget; ++slot) {
            auto message = in.tryReceive(localCycle());
            if (!message) break;
            const uint64_t identity = (uint64_t{message->source} << 32) | message->sequence;
            events_.record(localCycle(), ModelEventKind::Receive, identity, message->send_cycle);
            if (message->source == 0) {
                transaction_sequences_.push_back(message->sequence);
            } else {
                require(accepts_peer_, "peer payload reached the SPSC transaction sink");
                ++peer_received_;
            }
        }
        events_.record(localCycle(), ModelEventKind::State, transaction_sequences_.size(),
                       peer_received_);
    }

    [[nodiscard]] const std::vector<uint64_t>& transactionSequences() const noexcept {
        return transaction_sequences_;
    }
    [[nodiscard]] uint64_t peerReceived() const noexcept { return peer_received_; }
    UnitEventLog& eventLog() noexcept { return events_; }

private:
    bool accepts_peer_ = false;
    uint64_t peer_received_ = 0;
    std::vector<uint64_t> transaction_sequences_;
    UnitEventLog events_;
};

class TransactionScenario {
public:
    explicit TransactionScenario(TickSimulation& simulation) {
        producer_ = simulation.createUnit<TransactionProducer>();
        peer_ = simulation.createUnit<PeerProducer>();
        spsc_sink_ = simulation.createUnit<TransactionSink>("spsc_sink", 2, false);
        mpsc_sink_ = simulation.createUnit<TransactionSink>("mpsc_sink", 3, true);

        simulation.connect(producer_->out_spsc, spsc_sink_->in, 1);
        simulation.connect(producer_->out_mpsc, mpsc_sink_->in, 1);
        simulation.connect(peer_->out, mpsc_sink_->in, 1);
    }

    std::vector<std::string> componentNames() const {
        return {"transaction_producer", "peer_producer", "spsc_sink", "mpsc_sink"};
    }

    std::vector<CanonicalEvent> canonicalEvents() const {
        require(producer_->committed() == kTransactionMessages,
                "transaction producer did not complete its stream");
        require(peer_->sent() == kPeerMessages, "peer producer did not complete its stream");
        require(producer_->senderCancelExercised(),
                "epoch-free matrix did not exercise sender cancellation");
        require(producer_->explicitReleaseExercised(),
                "epoch-free matrix did not exercise explicit release");
        require(producer_->backpressureCount() != 0,
                "epoch-free matrix did not exercise reservation backpressure");
        require(mpsc_sink_->peerReceived() == kPeerMessages,
                "MPSC sink did not receive the complete peer stream");

        const auto& spsc = spsc_sink_->transactionSequences();
        const auto& mpsc = mpsc_sink_->transactionSequences();
        require(spsc.size() == kTransactionMessages && mpsc.size() == kTransactionMessages,
                "a committed transaction destination lost a payload");
        require(spsc == mpsc, "SPSC and MPSC transaction streams diverged");
        for (uint64_t sequence = 0; sequence < kTransactionMessages; ++sequence) {
            require(spsc[sequence] == sequence,
                    "transaction stream contained a duplicate or out-of-order retry");
        }

        std::array<UnitEventLog*, 4> logs{&producer_->eventLog(), &peer_->eventLog(),
                                          &spsc_sink_->eventLog(), &mpsc_sink_->eventLog()};
        return canonicalizeEvents(std::span<UnitEventLog* const>(logs.data(), logs.size()));
    }

private:
    TransactionProducer* producer_ = nullptr;
    PeerProducer* peer_ = nullptr;
    TransactionSink* spsc_sink_ = nullptr;
    TransactionSink* mpsc_sink_ = nullptr;
};

void testEpochFreeDifferentialMatrix() {
    TickSimulationConfig base;
    base.max_lookahead_cycles = 8;
    base.enable_weighted_partitioning = true;
    base.partition_solver = TickSimulationConfig::PartitionSolverType::Weighted;
    base.initial_partition_sync_cost_ns = 0.0;
    base.rebalance_check_interval_cycles = 32;
    base.rebalance_imbalance_threshold = 1.01;
    base.rebalance_min_gain = 0.0;
    base.rebalance_cooldown_cycles = 0;

    std::vector<EpochFreeRunMode> modes{{.name = "sequential-reference",
                                         .kind = EpochFreeRunKind::SequentialReference,
                                         .num_threads = 1,
                                         .migrations = {}}};
    for (size_t workers = 2; workers <= 8; ++workers) {
        modes.push_back({.name = "epoch-free-static-" + std::to_string(workers),
                         .kind = EpochFreeRunKind::Static,
                         .num_threads = workers,
                         .migrations = {}});
    }
    modes.push_back({.name = "epoch-free-forced-4",
                     .kind = EpochFreeRunKind::ForcedMigration,
                     .num_threads = 4,
                     .migrations = {{.cycle = 53, .unit_name = "mpsc_sink"},
                                    {.cycle = 117, .unit_name = "transaction_producer"}}});
    modes.push_back({.name = "epoch-free-runtime-3",
                     .kind = EpochFreeRunKind::RuntimeRebalance,
                     .num_threads = 3,
                     .migrations = {}});

    const auto artifacts =
        runEpochFreeMatrix(base, kDifferentialCycles, std::span<const EpochFreeRunMode>(modes),
                           [](TickSimulation& simulation, const EpochFreeRunMode&) {
                               return std::make_unique<TransactionScenario>(simulation);
                           });
    const auto comparison = compareMatrix(artifacts);
    require(comparison.equivalent, comparison.diagnostic);

    for (const auto& artifact : artifacts) {
        if (artifact.mode_name == "sequential-reference") {
            require(artifact.epoch_free_runs == 0,
                    "sequential transaction oracle used epoch-free execution");
        } else {
            require(artifact.epoch_free_runs != 0,
                    artifact.mode_name + " fell back from epoch-free execution");
        }
        if (artifact.mode_name == "epoch-free-forced-4") {
            require(artifact.forced_migrations_applied == 2,
                    "transaction matrix did not apply both forced migrations");
        }
        if (artifact.mode_name == "epoch-free-runtime-3") {
#ifndef CHRONON_SANITIZER_BUILD
            require(artifact.rebalance_count != 0,
                    "transaction matrix did not exercise runtime rebalance");
#endif
        }
        std::cout << "  " << artifact.mode_name << ": digest=" << artifact.digest
                  << " events=" << artifact.events.size()
                  << " rebalances=" << artifact.rebalance_count << '\n';
    }
}

}  // namespace

int main() {
    try {
        testFailedReservationPublishesNothing();
        testPositionalCommitIsExactlyOnce();
        testStagedCommitSupportsRuntimeSelectedSameTypePort();
        testOutPortCapacityChangeInvalidatesClaim();
        testDestinationCapacityChangeInvalidatesClaim();
        testTopologyChangeReleasesOnlyOriginallyClaimedConnections();
        testCancelBetweenReserveAndCommitDeliversNone();
        testReceiverFlushDoesNotCancelUnpublishedTransaction();
        testCycleBoundaryAndExplicitRelease();
        testInvalidPortSetsAreRejected();
        testNoopThrowingPayloadCannotPartiallyCommit();
        testMixedDependencyOnlyEdgeParticipatesWithoutPayload();
        testSharedBoundedDestinationIsRejectedBeforeClaim();
        testOrdinarySendInvalidatesClaimedBoundedDestination();
        testIndependentTransactionsSerializeBoundedDestinationClaims();
        testRepeatedBoundedEdgeOnOnePortIsRejected();
        testSharedFiniteSPSCTransportIsRejected();
        testSharedUnboundedDestinationRemainsSupported();
        testSharedUnboundedMPSCDestinationUsesIndependentLanes();
        testTransactionSpansSharedBroadcastTransport();
        testTransactionSpansSPSCAndMPSC();
        testEpochFreeDifferentialMatrix();
    } catch (const std::exception& error) {
        std::cerr << "Port transaction test failed: " << error.what() << '\n';
        return 1;
    }
    std::cout << "Port transaction tests passed\n";
    return 0;
}
