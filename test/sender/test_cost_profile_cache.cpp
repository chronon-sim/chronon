// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// test_cost_profile_cache.cpp
//
// Tests for the CostProfileCache — topology hashing, JSON round-trip,
// cache invalidation, and atomic write semantics.

#include <cassert>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "sender/core/TickableUnit.hpp"
#include "sender/port/Connection.hpp"
#include "sender/schedule/CostProfileCache.hpp"

using namespace chronon::sender;

// =============================================================================
// Test fixtures
// =============================================================================

class FixtureUnit : public TickableUnit {
public:
    explicit FixtureUnit(std::string name) : TickableUnit(std::move(name)) {}
    void tick() override {}
};

// Minimal ConnectionBase stub that only fills source/destination/delay.
// Remaining virtuals are no-ops so we can hold it in a std::vector<ConnectionBase*>.
class FixtureConnection : public ConnectionBase {
public:
    FixtureConnection(Unit* src, Unit* dst, uint32_t d) : src_(src), dst_(dst), delay_(d) {}

    uint32_t delay() const noexcept override { return delay_; }
    Unit* source() const noexcept override { return src_; }
    Unit* destination() const noexcept override { return dst_; }
    void* destPortPtr() const noexcept override { return nullptr; }
    void optimizeForSameThread() override {}
    void optimizeForSPSC() override {}
    void optimizeForMPSC() override {}
    size_t registerProducerThread(size_t /*thread_id*/) override { return 0; }
    void setThreadQueueId(size_t /*queue_id*/) override {}
    bool hasThreadQueueId() const noexcept override { return false; }
    void setConnId(uint32_t id) noexcept override { conn_id_ = id; }
    uint32_t connId() const noexcept override { return conn_id_; }
    void cancelInFlight() noexcept override {}
    size_t arbitrateAdmitErased(size_t /*budget*/) override { return 0; }
    size_t arbitrateAdmitBoundedErased(size_t /*budget*/, uint64_t /*max_send_cycle*/) override {
        return 0;
    }
    IArbitratablePort* registerOnDestMPSC() override { return nullptr; }

private:
    Unit* src_;
    Unit* dst_;
    uint32_t delay_;
    uint32_t conn_id_ = 0;
};

struct Topology {
    std::vector<std::unique_ptr<FixtureUnit>> units;
    std::vector<std::unique_ptr<FixtureConnection>> conns;
    std::vector<TickableUnit*> unit_ptrs;
    std::vector<ConnectionBase*> conn_ptrs;

    FixtureUnit* addUnit(const std::string& name) {
        units.push_back(std::make_unique<FixtureUnit>(name));
        unit_ptrs.push_back(units.back().get());
        return units.back().get();
    }

    void addEdge(Unit* src, Unit* dst, uint32_t delay) {
        conns.push_back(std::make_unique<FixtureConnection>(src, dst, delay));
        conn_ptrs.push_back(conns.back().get());
    }
};

static std::filesystem::path makeTempPath(const std::string& suffix) {
    auto dir = std::filesystem::temp_directory_path() / "chronon_cost_cache_test";
    std::filesystem::create_directories(dir);
    return dir / suffix;
}

// =============================================================================
// hashTopology: stability + sensitivity
// =============================================================================

static void test_hash_stable_across_instances() {
    std::cout << "test_hash_stable_across_instances... ";
    Topology a;
    auto* u0 = a.addUnit("fetch");
    auto* u1 = a.addUnit("decode");
    auto* u2 = a.addUnit("execute");
    a.addEdge(u0, u1, 1);
    a.addEdge(u1, u2, 2);

    Topology b;
    auto* v0 = b.addUnit("fetch");
    auto* v1 = b.addUnit("decode");
    auto* v2 = b.addUnit("execute");
    b.addEdge(v0, v1, 1);
    b.addEdge(v1, v2, 2);

    [[maybe_unused]] uint64_t ha = CostProfileCache::hashTopology(a.unit_ptrs, a.conn_ptrs);
    [[maybe_unused]] uint64_t hb = CostProfileCache::hashTopology(b.unit_ptrs, b.conn_ptrs);
    assert(ha == hb);
    std::cout << "PASSED\n";
}

static void test_hash_order_insensitive() {
    std::cout << "test_hash_order_insensitive... ";
    Topology a;
    auto* au = a.addUnit("fetch");
    auto* av = a.addUnit("decode");
    a.addEdge(au, av, 1);
    a.addEdge(av, au, 3);

    Topology b;
    auto* bv = b.addUnit("decode");
    auto* bu = b.addUnit("fetch");
    b.addEdge(bv, bu, 3);
    b.addEdge(bu, bv, 1);

    [[maybe_unused]] uint64_t ha = CostProfileCache::hashTopology(a.unit_ptrs, a.conn_ptrs);
    [[maybe_unused]] uint64_t hb = CostProfileCache::hashTopology(b.unit_ptrs, b.conn_ptrs);
    assert(ha == hb);
    std::cout << "PASSED\n";
}

static void test_hash_differs_on_added_unit() {
    std::cout << "test_hash_differs_on_added_unit... ";
    Topology base;
    auto* u0 = base.addUnit("fetch");
    auto* u1 = base.addUnit("decode");
    base.addEdge(u0, u1, 1);

    Topology grown;
    auto* v0 = grown.addUnit("fetch");
    auto* v1 = grown.addUnit("decode");
    auto* v2 = grown.addUnit("execute");
    grown.addEdge(v0, v1, 1);
    grown.addEdge(v1, v2, 1);

    assert(CostProfileCache::hashTopology(base.unit_ptrs, base.conn_ptrs) !=
           CostProfileCache::hashTopology(grown.unit_ptrs, grown.conn_ptrs));
    std::cout << "PASSED\n";
}

static void test_hash_differs_on_delay_change() {
    std::cout << "test_hash_differs_on_delay_change... ";
    Topology a;
    auto* au = a.addUnit("fetch");
    auto* av = a.addUnit("decode");
    a.addEdge(au, av, 1);

    Topology b;
    auto* bu = b.addUnit("fetch");
    auto* bv = b.addUnit("decode");
    b.addEdge(bu, bv, 2);

    assert(CostProfileCache::hashTopology(a.unit_ptrs, a.conn_ptrs) !=
           CostProfileCache::hashTopology(b.unit_ptrs, b.conn_ptrs));
    std::cout << "PASSED\n";
}

// =============================================================================
// save / load round-trip
// =============================================================================

static void test_save_load_roundtrip() {
    std::cout << "test_save_load_roundtrip... ";
    auto path = makeTempPath("roundtrip.json");
    std::filesystem::remove(path);

    std::vector<UnitTickProfile> profiles = {
        {123.5, 100.0, 1024},
        {456.25, 400.0, 1024},
        {0.125, 0.0, 1024},
    };
    uint64_t hash = 0xdeadbeef12345678ULL;
    assert(CostProfileCache::save(path.string(), hash, profiles, {"fetch", "decode", "exec"}));

    auto loaded = CostProfileCache::load(path.string(), hash);
    assert(loaded.has_value());
    assert(loaded->size() == profiles.size());
    for (size_t i = 0; i < profiles.size(); ++i) {
        assert((*loaded)[i].mean_ns == profiles[i].mean_ns);
        assert((*loaded)[i].median_ns == profiles[i].median_ns);
        assert((*loaded)[i].sample_count == profiles[i].sample_count);
    }
    std::cout << "PASSED\n";
}

static void test_load_rejects_stale_hash() {
    std::cout << "test_load_rejects_stale_hash... ";
    auto path = makeTempPath("stale.json");
    std::filesystem::remove(path);

    std::vector<UnitTickProfile> profiles = {{10.0, 10.0, 100}};
    assert(CostProfileCache::save(path.string(), 0xAAAAAAAAAAAAAAAAULL, profiles));

    auto hit = CostProfileCache::load(path.string(), 0xAAAAAAAAAAAAAAAAULL);
    assert(hit.has_value());

    auto miss = CostProfileCache::load(path.string(), 0xBBBBBBBBBBBBBBBBULL);
    assert(!miss.has_value());
    std::cout << "PASSED\n";
}

static void test_load_missing_file() {
    std::cout << "test_load_missing_file... ";
    auto path = makeTempPath("does_not_exist.json");
    std::filesystem::remove(path);
    auto miss = CostProfileCache::load(path.string(), 0x1);
    assert(!miss.has_value());
    std::cout << "PASSED\n";
}

static void test_load_rejects_corrupt_file() {
    std::cout << "test_load_rejects_corrupt_file... ";
    auto path = makeTempPath("corrupt.json");
    {
        std::ofstream out(path);
        out << "{not really json at all";
    }
    auto miss = CostProfileCache::load(path.string(), 0x1);
    assert(!miss.has_value());
    std::cout << "PASSED\n";
}

static void test_load_rejects_unknown_version() {
    std::cout << "test_load_rejects_unknown_version... ";
    auto path = makeTempPath("future_version.json");
    {
        std::ofstream out(path);
        out << "{\n  \"version\": 42,\n"
            << "  \"topology_hash\": \"0x0000000000000001\",\n"
            << "  \"profiles\": []\n}\n";
    }
    auto miss = CostProfileCache::load(path.string(), 0x1);
    assert(!miss.has_value());
    std::cout << "PASSED\n";
}

// Simulate a crash that leaves only <path>.tmp on disk. The real file
// should remain absent and `load` should return nullopt, while a
// subsequent `save` completes cleanly and supersedes the leftover tmp.
static void test_save_atomicity_leftover_tmp() {
    std::cout << "test_save_atomicity_leftover_tmp... ";
    auto path = makeTempPath("atomic.json");
    auto tmp = std::filesystem::path(path.string() + ".tmp");
    std::filesystem::remove(path);
    std::filesystem::remove(tmp);

    {
        std::ofstream out(tmp);
        out << "partially written garbage ";
    }
    // The loader only inspects <path>; a leftover tmp must not confuse it.
    assert(!CostProfileCache::load(path.string(), 0x1).has_value());

    std::vector<UnitTickProfile> profiles = {{1.0, 1.0, 1}};
    assert(CostProfileCache::save(path.string(), 0x1, profiles));
    auto loaded = CostProfileCache::load(path.string(), 0x1);
    assert(loaded.has_value());
    assert(loaded->size() == 1);
    // The successful save should have consumed the tmp via rename.
    std::filesystem::remove(tmp);
    std::cout << "PASSED\n";
}

static void test_save_overwrites_existing_atomically() {
    std::cout << "test_save_overwrites_existing_atomically... ";
    auto path = makeTempPath("overwrite.json");
    std::filesystem::remove(path);

    std::vector<UnitTickProfile> v1 = {{1.0, 1.0, 1}};
    assert(CostProfileCache::save(path.string(), 0x1, v1));
    auto loaded1 = CostProfileCache::load(path.string(), 0x1);
    assert(loaded1.has_value() && loaded1->size() == 1);

    std::vector<UnitTickProfile> v2 = {{2.0, 2.0, 2}, {3.0, 3.0, 3}};
    assert(CostProfileCache::save(path.string(), 0x2, v2));
    auto loaded2 = CostProfileCache::load(path.string(), 0x2);
    assert(loaded2.has_value() && loaded2->size() == 2);

    // The old hash should no longer satisfy load.
    assert(!CostProfileCache::load(path.string(), 0x1).has_value());
    std::cout << "PASSED\n";
}

// =============================================================================
// Integration: two successive save/load cycles produce identical data.
// =============================================================================

static void test_integration_two_runs() {
    std::cout << "test_integration_two_runs... ";
    Topology topo;
    auto* u0 = topo.addUnit("fetch");
    auto* u1 = topo.addUnit("decode");
    auto* u2 = topo.addUnit("rob");
    topo.addEdge(u0, u1, 1);
    topo.addEdge(u1, u2, 1);
    topo.addEdge(u2, u0, 4);

    auto path = makeTempPath("integration.json");
    std::filesystem::remove(path);

    uint64_t hash = CostProfileCache::hashTopology(topo.unit_ptrs, topo.conn_ptrs);

    // First run: cache miss, then save.
    assert(!CostProfileCache::load(path.string(), hash).has_value());
    std::vector<UnitTickProfile> profiles = {
        {321.0, 300.0, 1024},
        {145.75, 140.0, 1024},
        {87.5, 80.0, 1024},
    };
    assert(CostProfileCache::save(path.string(), hash, profiles, {"fetch", "decode", "rob"}));

    // Second run: cache hit returns the same profile data.
    auto hit = CostProfileCache::load(path.string(), hash);
    assert(hit.has_value());
    assert(hit->size() == profiles.size());
    for (size_t i = 0; i < profiles.size(); ++i) {
        assert((*hit)[i].mean_ns == profiles[i].mean_ns);
        assert((*hit)[i].median_ns == profiles[i].median_ns);
        assert((*hit)[i].sample_count == profiles[i].sample_count);
    }

    // And a different topology produces a different hash -> miss on same path.
    topo.addEdge(u0, u2, 1);
    [[maybe_unused]] uint64_t new_hash =
        CostProfileCache::hashTopology(topo.unit_ptrs, topo.conn_ptrs);
    assert(new_hash != hash);
    assert(!CostProfileCache::load(path.string(), new_hash).has_value());
    std::cout << "PASSED\n";
}

int main() {
    test_hash_stable_across_instances();
    test_hash_order_insensitive();
    test_hash_differs_on_added_unit();
    test_hash_differs_on_delay_change();

    test_save_load_roundtrip();
    test_load_rejects_stale_hash();
    test_load_missing_file();
    test_load_rejects_corrupt_file();
    test_load_rejects_unknown_version();

    test_save_atomicity_leftover_tmp();
    test_save_overwrites_existing_atomically();

    test_integration_two_runs();

    std::cout << "\nAll CostProfileCache tests PASSED\n";
    return 0;
}
