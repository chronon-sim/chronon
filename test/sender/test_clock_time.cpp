// SPDX-License-Identifier: MPL-2.0
#include <array>
#include <cassert>
#include <iostream>
#include <queue>
#include <random>

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

void reductionDifferential() {
    const ClockDomain wide(1, "wide", SimTime(1, UINT64_MAX));
    const ClockDomain fixed_phase(2, "fixed-phase", SimTime(1, uint64_t{1} << 50),
                                  SimTime(1, uint64_t{1} << 51));
    for (uint64_t n : {uint64_t{0}, uint64_t{1}, uint64_t{17}, uint64_t{999999}, UINT64_MAX}) {
        const SimTime expected(n, UINT64_MAX);
        assert(wide.edge(n).numerator() == expected.numerator());
        assert(wide.edge(n).denominator() == expected.denominator());
    }
    for (uint64_t n : {uint64_t{0}, uint64_t{1}, uint64_t{999999}, fixed_phase.maxEdgeIndex()}) {
        const SimTime expected(2 * n + 1, uint64_t{1} << 51);
        assert(fixed_phase.edge(n).numerator() == expected.numerator());
        assert(fixed_phase.edge(n).denominator() == expected.denominator());
    }
    std::mt19937 random(143);
    for (size_t trial = 0; trial < 500; ++trial) {
        const SimTime period(1 + random() % 1000, 1 + random() % 1000);
        const SimTime phase(trial % 4 ? random() % 1000 : 0, 1 + random() % 1000);
        const ClockDomain clock(1, "reduction", period, phase);
        const auto denominator = std::lcm(period.denominator(), phase.denominator());
        const auto reference = [&](uint64_t cycle) {
            const auto ticks =
                clock_detail::Wide(cycle) * period.numerator() *
                    (denominator / period.denominator()) +
                clock_detail::Wide(phase.numerator()) * (denominator / phase.denominator());
            return SimTime(clock_detail::narrow(ticks), denominator);
        };
        for (uint64_t cycle : {uint64_t{0}, uint64_t{1}, uint64_t{2}, uint64_t{17},
                               uint64_t{999999}, clock.maxEdgeIndex()}) {
            const auto expected = reference(cycle), actual = clock.edge(cycle);
            assert(actual.numerator() == expected.numerator());
            assert(actual.denominator() == expected.denominator());
        }
        if (clock.maxEdgeIndex() != UINT64_MAX) {
            rejects([&] { (void)reference(clock.maxEdgeIndex() + 1); });
            rejects([&] { (void)clock.edge(clock.maxEdgeIndex() + 1); });
        }
    }
}

void calendarDifferential() {
    struct Later {
        bool operator()(const sender::ClockEdge& a, const sender::ClockEdge& b) const {
            return a.time == b.time ? a.domain->id() > b.domain->id() : a.time > b.time;
        }
    };
    std::mt19937 random(143);
    for (size_t count : {0, 1, 2, 3, 7, 16, 31, 32}) {
        std::vector<ClockDomain> domains;
        domains.reserve(count);
        for (size_t i = 0; i < count; ++i)
            domains.emplace_back(static_cast<uint32_t>(count - i), std::to_string(i),
                                 SimTime(1 + random() % 7, 21), SimTime(random() % 3, 21));
        std::vector<const ClockDomain*> clocks;
        std::priority_queue<sender::ClockEdge, std::vector<sender::ClockEdge>, Later> oracle;
        for (size_t i = 0; i < count; ++i) {
            clocks.push_back(&domains[i]);
            oracle.push({&domains[i], 0, domains[i].edge(0), i});
        }
        sender::ClockCalendar calendar(clocks), saved(clocks);
        assert(calendar.empty() == oracle.empty());
        if (!count) {
            assert(calendar.pop().empty());
            rejects([&] { (void)calendar.nextTime(); });
            continue;
        }
        for (size_t batch = 0; batch < 1000; ++batch) {
            assert(calendar.nextTime() == oracle.top().time);
            saved = calendar;
            if (batch % 11 == 0) {
                (void)calendar.pop();
                calendar = saved;
            }
            const auto actual = calendar.pop();
            const auto time = oracle.top().time;
            size_t index = 0;
            do {
                const auto edge = oracle.top();
                oracle.pop();
                assert(index < actual.size());
                assert(actual[index].domain == edge.domain && actual[index].cycle == edge.cycle);
                assert(actual[index].time == edge.time &&
                       actual[index].calendar_index == edge.calendar_index);
                ++index;
                oracle.push({edge.domain, edge.cycle + 1, edge.domain->edge(edge.cycle + 1),
                             edge.calendar_index});
            } while (oracle.top().time == time);
            assert(index == actual.size());
        }
    }
    ClockDomain huge(1, "huge", SimTime(UINT64_MAX));
    std::array<const ClockDomain*, 1> clocks{&huge};
    sender::ClockCalendar calendar(clocks);
    assert(calendar.pop().front().cycle == 0);
    rejects([&] { (void)calendar.pop(); });
    assert(calendar.nextTime() == huge.edge(1));
}
int main() {
    reductionDifferential();
    calendarDifferential();
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
