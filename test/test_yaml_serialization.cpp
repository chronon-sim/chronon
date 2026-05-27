// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#include <yaml-cpp/yaml.h>

#include <cassert>
#include <cmath>
#include <fstream>
#include <iostream>
#include <sstream>

#include "params/Parameter.hpp"
#include "params/ParameterSet.hpp"
#include "params/UnitTypes.hpp"
#include "params/YAMLSerialization.hpp"

using namespace chronon::params;

class YAMLTestParameterSet : public ParameterSet {
public:
    Param<uint32_t> retire_width{this, "retire_width", 4, "Number of instructions to retire"};
    Param<uint32_t> rob_size{this, "rob_size", 192, "ROB size"};
    Param<Frequency> clock_freq{this, "clock_freq", Frequency("3GHz"), "Clock frequency"};
    Param<Latency> retire_latency{this, "retire_latency", Latency("1ns"), "Retirement latency"};
    Param<Size> cache_size{this, "cache_size", Size("64KiB"), "Cache size"};
    Param<bool> enable_feature{this, "enable_feature", false, "Enable feature flag"};

    // Helper method for serialization (optional - for compatibility with existing tests)
    void serializeYAML(YAML::Emitter& out) const {
        out << YAML::BeginMap;
        serializeParamValue(out, retire_width);
        serializeParamValue(out, rob_size);
        serializeParamValue(out, clock_freq);
        serializeParamValue(out, retire_latency);
        serializeParamValue(out, cache_size);
        serializeParamValue(out, enable_feature);
        out << YAML::EndMap;
    }
};

void test_yaml_basic_serialization() {
    std::cout << "Test: YAML basic serialization" << std::endl;

    YAMLTestParameterSet params;

    // Serialize to YAML
    YAML::Emitter out;
    params.serializeYAML(out);

    std::string yaml_str = out.c_str();
    std::cout << "  Generated YAML:" << std::endl;
    std::cout << yaml_str << std::endl;

    // Check that YAML contains expected keys
    assert(yaml_str.find("retire_width") != std::string::npos);
    assert(yaml_str.find("rob_size") != std::string::npos);
    assert(yaml_str.find("clock_freq") != std::string::npos);

    std::cout << "  PASS" << std::endl;
}

void test_yaml_deserialization() {
    std::cout << "Test: YAML deserialization" << std::endl;

    // Create YAML with custom values
    std::string yaml_content = R"(
retire_width: 8
rob_size: 256
clock_freq: 2.5GHz
retire_latency: 2ns
cache_size: 128KiB
enable_feature: true
)";

    YAML::Node node = YAML::Load(yaml_content);

    // Deserialize into parameter set
    YAMLTestParameterSet params;
    params.deserializeYAML(&node);

    // Verify values were loaded
    assert(static_cast<uint32_t>(params.retire_width) == 8);
    assert(static_cast<uint32_t>(params.rob_size) == 256);
    assert(std::abs(static_cast<Frequency>(params.clock_freq).ghz() - 2.5) < 1e-6);
    assert(std::abs(static_cast<Latency>(params.retire_latency).ns() - 2.0) < 1e-6);
    assert(static_cast<Size>(params.cache_size).bytes() == 128 * 1024);
    assert(static_cast<bool>(params.enable_feature) == true);

    std::cout << "  PASS" << std::endl;
}

void test_yaml_roundtrip() {
    std::cout << "Test: YAML roundtrip (serialize + deserialize)" << std::endl;

    // Create parameter set with custom values
    YAMLTestParameterSet params1;
    params1.retire_width = 16;
    params1.rob_size = 512;
    params1.clock_freq = Frequency("4GHz");
    params1.retire_latency = Latency("0.5ns");  // 0.5ns = 500ps
    params1.cache_size = Size("256KiB");
    params1.enable_feature = true;

    // Serialize to YAML
    YAML::Emitter out;
    params1.serializeYAML(out);
    std::string yaml_str = out.c_str();

    // Deserialize into new parameter set
    YAML::Node node = YAML::Load(yaml_str);
    YAMLTestParameterSet params2;
    params2.deserializeYAML(&node);

    // Verify values match
    assert(static_cast<uint32_t>(params2.retire_width) == 16);
    assert(static_cast<uint32_t>(params2.rob_size) == 512);
    assert(std::abs(static_cast<Frequency>(params2.clock_freq).ghz() - 4.0) < 1e-6);
    // Note: 500ps = 0.5ns
    assert(std::abs(static_cast<Latency>(params2.retire_latency).ns() - 0.5) < 1e-6);
    assert(static_cast<Size>(params2.cache_size).bytes() == 256 * 1024);
    assert(static_cast<bool>(params2.enable_feature) == true);

    std::cout << "  PASS" << std::endl;
}

void test_yaml_file_io() {
    std::cout << "Test: YAML file I/O" << std::endl;

    // Create parameter set
    YAMLTestParameterSet params1;
    params1.retire_width = 12;
    params1.rob_size = 384;
    params1.clock_freq = Frequency("3.5GHz");

    // Serialize to YAML emitter
    YAML::Emitter out;
    params1.serializeYAML(out);

    // Write to file
    std::ofstream file("test_params.yaml");
    file << out.c_str();
    file.close();

    // Read from file
    YAML::Node node = YAML::LoadFile("test_params.yaml");

    // Deserialize
    YAMLTestParameterSet params2;
    params2.deserializeYAML(&node);

    // Verify
    assert(static_cast<uint32_t>(params2.retire_width) == 12);
    assert(static_cast<uint32_t>(params2.rob_size) == 384);
    assert(std::abs(static_cast<Frequency>(params2.clock_freq).ghz() - 3.5) < 1e-6);

    // Cleanup
    std::remove("test_params.yaml");

    std::cout << "  PASS" << std::endl;
}

void test_yaml_partial_deserialization() {
    std::cout << "Test: YAML partial deserialization (missing fields use defaults)" << std::endl;

    // Create YAML with only some fields
    std::string yaml_content = R"(
retire_width: 10
clock_freq: 2GHz
)";

    YAML::Node node = YAML::Load(yaml_content);

    // Deserialize into parameter set
    YAMLTestParameterSet params;
    params.deserializeYAML(&node);

    // Verify specified values were loaded
    assert(static_cast<uint32_t>(params.retire_width) == 10);
    assert(std::abs(static_cast<Frequency>(params.clock_freq).ghz() - 2.0) < 1e-6);

    // Verify unspecified values kept their defaults
    assert(static_cast<uint32_t>(params.rob_size) == 192);                            // Default
    assert(std::abs(static_cast<Latency>(params.retire_latency).ns() - 1.0) < 1e-6);  // Default
    assert(static_cast<Size>(params.cache_size).bytes() == 64 * 1024);                // Default
    assert(static_cast<bool>(params.enable_feature) == false);                        // Default

    std::cout << "  PASS" << std::endl;
}

void test_new_style_access() {
    std::cout << "Test: New style parameter access" << std::endl;

    YAMLTestParameterSet params;

    // Test implicit conversion for reading
    [[maybe_unused]] uint32_t w = params.retire_width;  // implicit conversion
    assert(w == 4);

    // Test assignment
    params.retire_width = 16;
    assert(static_cast<uint32_t>(params.retire_width) == 16);

    // Test explicit value() method
    assert(params.retire_width.value() == 16);

    // Test isModified()
    assert(params.retire_width.isModified());
    assert(!params.rob_size.isModified());

    // Test reset()
    params.retire_width.reset();
    assert(static_cast<uint32_t>(params.retire_width) == 4);
    assert(!params.retire_width.isModified());

    std::cout << "  PASS" << std::endl;
}

void test_foreach_param() {
    std::cout << "Test: forEachParam iteration" << std::endl;

    YAMLTestParameterSet params;
    params.retire_width = 8;
    params.rob_size = 256;

    int count = 0;
    params.forEachParam([&count](const std::string& name, const std::string& value) {
        std::cout << "  " << name << " = " << value << std::endl;
        count++;
    });

    // Should iterate over all 6 registered params
    assert(count == 6);

    std::cout << "  PASS" << std::endl;
}

void test_get_set_param_string() {
    std::cout << "Test: getParamString / setParamString" << std::endl;

    YAMLTestParameterSet params;

    // Get by name
    assert(params.getParamString("retire_width") == "4");
    assert(params.getParamString("rob_size") == "192");

    // Set by name
    params.setParamString("retire_width", "32");
    assert(static_cast<uint32_t>(params.retire_width) == 32);

    // Test error for unknown parameter
    [[maybe_unused]] bool threw = false;
    try {
        params.getParamString("nonexistent");
    } catch (const std::runtime_error&) {
        threw = true;
    }
    assert(threw);

    std::cout << "  PASS" << std::endl;
}

int main() {
    std::cout << "\n=== YAML Serialization Tests ===" << std::endl;

    test_yaml_basic_serialization();
    test_yaml_deserialization();
    test_yaml_roundtrip();
    test_yaml_file_io();
    test_yaml_partial_deserialization();
    test_new_style_access();
    test_foreach_param();
    test_get_set_param_string();

    std::cout << "\n=== All YAML Tests Passed ===" << std::endl;
    return 0;
}
