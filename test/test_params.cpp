// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#include <cassert>
#include <cmath>
#include <iostream>
#include <string>

#include "params/Parameter.hpp"
#include "params/ParameterSet.hpp"
#include "params/UnitTypes.hpp"
#include "params/YAMLSerialization.hpp"

using namespace chronon::params;

// =======================
// StandaloneParameter<T> Tests
// =======================

void test_parameter_basic() {
    std::cout << "Test: Parameter basic functionality" << std::endl;

    StandaloneParameter<uint32_t> param("test_param", 42, "Test parameter");

    assert(param.name() == "test_param");
    assert(param.description() == "Test parameter");
    assert(param.value() == 42);
    assert(param.defaultValue() == 42);
    assert(!param.isModified());

    param.setValue(100);
    assert(param.value() == 100);
    assert(param.isModified());

    param.reset();
    assert(param.value() == 42);
    assert(!param.isModified());

    std::cout << "  PASS" << std::endl;
}

void test_parameter_string_conversion() {
    std::cout << "Test: Parameter string conversion" << std::endl;

    StandaloneParameter<uint32_t> int_param("int", 42, "Integer param");
    assert(int_param.valueAsString() == "42");
    int_param.setValueFromString("100");
    assert(int_param.value() == 100);

    StandaloneParameter<bool> bool_param("bool", false, "Boolean param");
    assert(bool_param.valueAsString() == "false");
    bool_param.setValueFromString("true");
    assert(bool_param.value() == true);

    StandaloneParameter<std::string> str_param("str", "hello", "String param");
    assert(str_param.value() == "hello");
    str_param.setValue("world");
    assert(str_param.value() == "world");

    std::cout << "  PASS" << std::endl;
}

void test_parameter_validation() {
    std::cout << "Test: Parameter validation" << std::endl;

    StandaloneParameter<uint32_t> param("test", 4, "Test param");

    // Set validator: value must be power of 2
    param.setValidator([](uint32_t v) { return v > 0 && (v & (v - 1)) == 0; });

    assert(param.validate());  // 4 is valid

    // Valid values
    param.setValue(8);
    assert(param.validate());

    // Invalid value should throw
    bool threw = false;
    try {
        param.setValue(7);  // Not a power of 2
    } catch (const std::invalid_argument&) {
        threw = true;
    }
    assert(threw);
    (void)threw;  // Suppress unused warning in release builds

    std::cout << "  PASS" << std::endl;
}

// =======================
// Frequency Tests
// =======================

void test_frequency_parsing() {
    std::cout << "Test: Frequency parsing" << std::endl;

    Frequency f1("1GHz");
    assert(std::abs(f1.ghz() - 1.0) < 1e-6);
    assert(std::abs(f1.hz() - 1e9) < 1e-3);

    Frequency f2("500MHz");
    assert(std::abs(f2.mhz() - 500.0) < 1e-6);
    assert(std::abs(f2.ghz() - 0.5) < 1e-6);

    Frequency f3("1000000Hz");
    assert(std::abs(f3.mhz() - 1.0) < 1e-6);

    Frequency f4(3e9);  // 3 GHz
    assert(std::abs(f4.ghz() - 3.0) < 1e-6);

    std::cout << "  PASS" << std::endl;
}

void test_frequency_arithmetic() {
    std::cout << "Test: Frequency arithmetic" << std::endl;

    Frequency f1("1GHz");
    Frequency f2("500MHz");

    Frequency f3 = f1 * 2.0;
    assert(std::abs(f3.ghz() - 2.0) < 1e-6);

    Frequency f4 = f1 / 2.0;
    assert(std::abs(f4.mhz() - 500.0) < 1e-6);

    double ratio = f1 / f2;
    assert(std::abs(ratio - 2.0) < 1e-6);

    Frequency f5 = f1 + f2;
    assert(std::abs(f5.ghz() - 1.5) < 1e-6);

    Frequency f6 = f1 - f2;
    assert(std::abs(f6.mhz() - 500.0) < 1e-6);

    // Suppress unused warnings
    (void)f3;
    (void)f4;
    (void)ratio;
    (void)f5;
    (void)f6;

    std::cout << "  PASS" << std::endl;
}

void test_frequency_comparison() {
    std::cout << "Test: Frequency comparison" << std::endl;

    Frequency f1("1GHz");
    Frequency f2("1000MHz");
    Frequency f3("500MHz");

    assert(f1 == f2);
    assert(f1 != f3);
    assert(f1 > f3);
    assert(f3 < f1);

    std::cout << "  PASS" << std::endl;
}

void test_frequency_with_parameter() {
    std::cout << "Test: Frequency with StandaloneParameter<T>" << std::endl;

    StandaloneParameter<Frequency> freq_param("clock", Frequency("3GHz"), "Clock frequency");

    assert(std::abs(freq_param.value().ghz() - 3.0) < 1e-6);

    freq_param.setValueFromString("2.5GHz");
    assert(std::abs(freq_param.value().ghz() - 2.5) < 1e-6);

    std::string str = freq_param.valueAsString();
    assert(!str.empty());  // Should produce valid string

    std::cout << "  PASS" << std::endl;
}

// =======================
// Latency Tests
// =======================

void test_latency_parsing() {
    std::cout << "Test: Latency parsing" << std::endl;

    Latency l1("10ns");
    assert(std::abs(l1.ns() - 10.0) < 1e-6);

    Latency l2("1us");
    assert(std::abs(l2.ns() - 1000.0) < 1e-6);

    Latency l3("5ms");
    assert(std::abs(l3.ms() - 5.0) < 1e-6);

    Latency l4("2s");
    assert(std::abs(l4.s() - 2.0) < 1e-6);

    std::cout << "  PASS" << std::endl;
}

void test_latency_arithmetic() {
    std::cout << "Test: Latency arithmetic" << std::endl;

    Latency l1("100ns");
    Latency l2("50ns");

    Latency l3 = l1 + l2;
    assert(std::abs(l3.ns() - 150.0) < 1e-6);

    Latency l4 = l1 - l2;
    assert(std::abs(l4.ns() - 50.0) < 1e-6);

    Latency l5 = l1 * 2.0;
    assert(std::abs(l5.ns() - 200.0) < 1e-6);

    double ratio = l1 / l2;
    assert(std::abs(ratio - 2.0) < 1e-6);

    // Suppress unused warnings
    (void)l3;
    (void)l4;
    (void)l5;
    (void)ratio;

    std::cout << "  PASS" << std::endl;
}

// =======================
// Size Tests
// =======================

void test_size_parsing() {
    std::cout << "Test: Size parsing" << std::endl;

    Size s1("64B");
    assert(s1.bytes() == 64);

    Size s2("1KiB");
    assert(s2.bytes() == 1024);

    Size s3("4MiB");
    assert(s3.bytes() == 4 * 1024 * 1024);

    Size s4("1GiB");
    assert(s4.bytes() == 1024ULL * 1024 * 1024);

    // Decimal units
    Size s5("1KB");
    assert(s5.bytes() == 1000);

    Size s6("1MB");
    assert(s6.bytes() == 1000000);

    std::cout << "  PASS" << std::endl;
}

void test_size_arithmetic() {
    std::cout << "Test: Size arithmetic" << std::endl;

    Size s1("1KiB");
    Size s2("512B");

    Size s3 = s1 + s2;
    assert(s3.bytes() == 1536);

    Size s4 = s1 - s2;
    assert(s4.bytes() == 512);

    Size s5 = s1 * 2;
    assert(s5.bytes() == 2048);

    uint64_t ratio = s1 / s2;
    assert(ratio == 2);

    // Suppress unused warnings
    (void)s3;
    (void)s4;
    (void)s5;
    (void)ratio;

    std::cout << "  PASS" << std::endl;
}

void test_size_comparison() {
    std::cout << "Test: Size comparison" << std::endl;

    Size s1("1KiB");
    Size s2("1024B");
    Size s3("512B");

    assert(s1 == s2);
    assert(s1 != s3);
    assert(s1 > s3);
    assert(s3 < s1);

    std::cout << "  PASS" << std::endl;
}

// =======================
// Bandwidth Tests
// =======================

void test_bandwidth_parsing() {
    std::cout << "Test: Bandwidth parsing" << std::endl;

    Bandwidth b1("10GB/s");
    assert(std::abs(b1.bytesPerSecond() - 10e9) < 1e-3);

    Bandwidth b2("100MiB/s");
    assert(std::abs(b2.mibPerSecond() - 100.0) < 1e-6);

    Bandwidth b3("1KiB/s");
    assert(std::abs(b3.kibPerSecond() - 1.0) < 1e-6);

    std::cout << "  PASS" << std::endl;
}

// =======================
// ParameterSet Tests
// =======================

struct TestParameterSet : public ParameterSet {
    Param<uint32_t> retire_width{this, "retire_width", 4, "Number of instructions to retire"};
    Param<uint32_t> rob_size{this, "rob_size", 192, "ROB size"};
    Param<Frequency> clock_freq{this, "clock_freq", Frequency("3GHz"), "Clock frequency"};
    Param<Latency> retire_latency{this, "retire_latency", Latency("1ns"), "Retirement latency"};
    Param<Size> cache_size{this, "cache_size", Size("64KiB"), "Cache size"};
};

void test_parameter_set_basic() {
    std::cout << "Test: ParameterSet basic functionality" << std::endl;

    TestParameterSet params;

    assert(static_cast<uint32_t>(params.retire_width) == 4);
    assert(static_cast<uint32_t>(params.rob_size) == 192);

    params.retire_width = 8;
    assert(static_cast<uint32_t>(params.retire_width) == 8);

    params.rob_size = 256;
    assert(static_cast<uint32_t>(params.rob_size) == 256);

    std::cout << "  PASS" << std::endl;
}

void test_parameter_set_unit_types() {
    std::cout << "Test: ParameterSet with unit types" << std::endl;

    TestParameterSet params;

    assert(std::abs(static_cast<const Frequency&>(params.clock_freq).ghz() - 3.0) < 1e-6);
    assert(std::abs(static_cast<const Latency&>(params.retire_latency).ns() - 1.0) < 1e-6);
    assert(static_cast<const Size&>(params.cache_size).bytes() == 64 * 1024);

    params.clock_freq = Frequency("2.5GHz");
    assert(std::abs(static_cast<const Frequency&>(params.clock_freq).ghz() - 2.5) < 1e-6);

    params.cache_size = Size("128KiB");
    assert(static_cast<const Size&>(params.cache_size).bytes() == 128 * 1024);

    std::cout << "  PASS" << std::endl;
}

void test_parameter_set_validation() {
    std::cout << "Test: ParameterSet validation" << std::endl;

    TestParameterSet params;

    assert(params.validate());

    params.retire_width = 8;
    assert(params.validate());

    std::cout << "  PASS" << std::endl;
}

// =======================
// Main Test Runner
// =======================

int main() {
    std::cout << "\n=== Parameter System Tests ===" << std::endl;

    // StandaloneParameter<T> tests
    test_parameter_basic();
    test_parameter_string_conversion();
    test_parameter_validation();

    // Frequency tests
    test_frequency_parsing();
    test_frequency_arithmetic();
    test_frequency_comparison();
    test_frequency_with_parameter();

    // Latency tests
    test_latency_parsing();
    test_latency_arithmetic();

    // Size tests
    test_size_parsing();
    test_size_arithmetic();
    test_size_comparison();

    // Bandwidth tests
    test_bandwidth_parsing();

    // ParameterSet tests
    test_parameter_set_basic();
    test_parameter_set_unit_types();
    test_parameter_set_validation();

    std::cout << "\n=== All Tests Passed ===" << std::endl;
    return 0;
}
