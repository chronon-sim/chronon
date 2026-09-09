// SPDX-License-Identifier: MPL-2.0
#include <array>
#include <cassert>
#include <iostream>

#include "clock_reference.hpp"
#include "sender/schedule/ClockCalendar.hpp"

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
    assert(SimTime(2, 6) == SimTime(1, 3));
    assert(SimTime(1, 3) + SimTime(1, 6) == SimTime(1, 2));
    assert(SimTime(UINT64_MAX, UINT64_MAX - 1) > SimTime(UINT64_MAX - 1, UINT64_MAX));
    rejects([] { SimTime(1, 0); });
    rejects([] { (void)(SimTime(UINT64_MAX) + SimTime(1)); });
    rejects([] { (void)SimTime(UINT64_MAX).floorNanoseconds(); });
    rejects([] { ClockDomain::fromHz(1, "zero", 0); });
    rejects([] { ClockDomain(1, "bad/path", SimTime(1)); });
    rejects([] { ClockDomain(1, "overflow", SimTime(1, UINT64_MAX), SimTime(1, UINT64_MAX - 1)); });
    const auto a = ClockDomain::fromHz(1, "sm", 914'000'000);
    const auto b = ClockDomain::fromHz(2, "lts", 1'326'000'000, 1, SimTime::picoseconds(137));
    const auto rational_hz = ClockDomain::fromHz(3, "rational", 5, 3, SimTime(2, 7));
    assert(rational_hz.edge(7) == SimTime(157, 35));
    assert(b.edgeAtOrAfter(SimTime{}) == 0);
    assert(b.edgeAfter(SimTime{}) == 0);
    for (auto n : {0ULL, 1ULL, 2ULL, 17ULL, 999999ULL, 1000000000000ULL}) {
        for (const auto* d : {&a, &b}) {
            assert(d->edgeAtOrAfter(d->edge(n)) == n);
            assert(d->edgeAfter(d->edge(n)) == n + 1);
            const auto between = d->edge(n) + SimTime(1, 10'000'000'000'000ULL);
            assert(d->edgeAtOrAfter(between) == n + 1);
            assert(d->edgeAfter(between) == n + 1);
        }
    }
    ClockDomain huge(9, "huge", SimTime(UINT64_MAX));
    rejects([&] { (void)huge.edge(2); });
    rejects([&] { (void)huge.edgeAfter(huge.edge(1)); });
    std::array<const ClockDomain*, 2> clocks{&a, &b};
    sender::ClockCalendar calendar(clocks);
    uint64_t wc = 0, rc = 0;
    for (size_t i = 0; i < 1'000'000; ++i) {
        auto wt = clock_reference::edge(914'000'000, 0, wc);
        auto rt = clock_reference::edge(1'326'000'000, 137, rc);
        const bool w = !(rt < wt), r = !(wt < rt);
        const auto t = w ? wt : rt;
        auto batch = calendar.pop();
        assert(batch.size() == size_t(w) + size_t(r));
        const auto actual = batch.front().time;
        assert(clock_reference::Wide(actual.numerator()) * t.denominator * 1'000'000'000'000ULL ==
               t.numerator * actual.denominator());
        wc += w;
        rc += r;
    }
    std::cout << "clock time: exact queries, range checks, 10^12-edge direct mapping and 1000000 "
                 "calendar batches passed\n";
}
