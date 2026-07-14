// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#include <cassert>
#include <cstdint>
#include <iostream>
#include <memory>
#include <string>

#include "tree/TreeNode.hpp"

using namespace chronon::tree;

struct MockResource {
    struct ParameterSet {
        uint32_t value1 = 10;
        uint32_t value2 = 20;
        std::string name = "mock";
    };

    TreeNode* node_ptr;
    const ParameterSet* params_ptr;

    MockResource(TreeNode* node, const ParameterSet* params) : node_ptr(node), params_ptr(params) {
        assert(node != nullptr && "TreeNode pointer should not be null");
        assert(params != nullptr && "ParameterSet pointer should not be null");
    }
};

void test_constructor_basic() {
    std::cout << "Test: Constructor basic" << std::endl;

    TreeNode node("test");
    assert(node.name() == "test");
    assert(node.isRoot());
    assert(node.parent() == nullptr);
    assert(node.phase() == TreeNode::Phase::BUILDING);

    std::cout << "  PASS" << std::endl;
}

void test_constructor_with_parent() {
    std::cout << "Test: Constructor with parent" << std::endl;

    TreeNode root("root");
    TreeNode child("child", &root);

    assert(child.name() == "child");
    assert(!child.isRoot());
    assert(child.parent() == &root);

    std::cout << "  PASS" << std::endl;
}

void test_constructor_invalid_name() {
    std::cout << "Test: Constructor invalid name" << std::endl;

    [[maybe_unused]] bool threw_empty = false;
    try {
        TreeNode empty("");
    } catch (const std::invalid_argument&) {
        threw_empty = true;
    }
    assert(threw_empty && "Empty name should throw");

    [[maybe_unused]] bool threw_dot = false;
    try {
        TreeNode invalid("invalid.name");
    } catch (const std::invalid_argument&) {
        threw_dot = true;
    }
    assert(threw_dot && "Name with dot should throw");

    std::cout << "  PASS" << std::endl;
}

void test_add_child() {
    std::cout << "Test: Add child" << std::endl;

    TreeNode root("root");

    auto child = std::make_unique<TreeNode>("child");
    [[maybe_unused]] TreeNode* child_ptr = child.get();

    root.addChild("child", std::move(child));

    assert(root.children().size() == 1);
    assert(root.getChildByRelativePath("child") == child_ptr);
    assert(child_ptr->parent() == &root);

    std::cout << "  PASS" << std::endl;
}

void test_add_child_duplicate_name() {
    std::cout << "Test: Add child duplicate name" << std::endl;

    TreeNode root("root");
    root.addChild("child", std::make_unique<TreeNode>("child"));

    [[maybe_unused]] bool threw = false;
    try {
        root.addChild("child", std::make_unique<TreeNode>("child"));
    } catch (const std::runtime_error&) {
        threw = true;
    }
    assert(threw && "Duplicate child name should throw");

    std::cout << "  PASS" << std::endl;
}

void test_add_child_wrong_phase() {
    std::cout << "Test: Add child wrong phase" << std::endl;

    TreeNode root("root");
    root.transitionPhase(TreeNode::Phase::CONFIGURING);

    [[maybe_unused]] bool threw = false;
    try {
        root.addChild("child", std::make_unique<TreeNode>("child"));
    } catch (const std::runtime_error&) {
        threw = true;
    }
    assert(threw && "Cannot add children in CONFIGURING phase");

    std::cout << "  PASS" << std::endl;
}

void test_path_single_node() {
    std::cout << "Test: Path single node" << std::endl;

    TreeNode root("root");
    assert(root.path() == "root");

    std::cout << "  PASS" << std::endl;
}

void test_path_with_parent() {
    std::cout << "Test: Path with parent" << std::endl;

    TreeNode root("cpu");
    auto core = std::make_unique<TreeNode>("core0", &root);
    [[maybe_unused]] TreeNode* core_ptr = core.get();
    root.addChild("core0", std::move(core));

    assert(core_ptr->path() == "cpu.core0");

    std::cout << "  PASS" << std::endl;
}

void test_path_deep_hierarchy() {
    std::cout << "Test: Path deep hierarchy" << std::endl;

    TreeNode root("cpu");

    auto core = std::make_unique<TreeNode>("core0", &root);
    [[maybe_unused]] TreeNode* core_ptr = core.get();
    root.addChild("core0", std::move(core));

    auto rob = std::make_unique<TreeNode>("rob", core_ptr);
    [[maybe_unused]] TreeNode* rob_ptr = rob.get();
    core_ptr->addChild("rob", std::move(rob));

    assert(rob_ptr->path() == "cpu.core0.rob");

    std::cout << "  PASS" << std::endl;
}

void test_get_child_nested() {
    std::cout << "Test: Get child nested" << std::endl;

    TreeNode root("cpu");

    auto core = std::make_unique<TreeNode>("core0");
    [[maybe_unused]] TreeNode* core_ptr = core.get();
    root.addChild("core0", std::move(core));

    auto rob = std::make_unique<TreeNode>("rob");
    [[maybe_unused]] TreeNode* rob_ptr = rob.get();
    core_ptr->addChild("rob", std::move(rob));

    assert(root.getChildByRelativePath("core0.rob") == rob_ptr);
    assert(root.getChildByRelativePath("core0.rob") == rob_ptr);

    std::cout << "  PASS" << std::endl;
}

void test_absolute_path_navigation() {
    std::cout << "Test: Absolute path navigation" << std::endl;

    TreeNode root("cpu");

    auto core = std::make_unique<TreeNode>("core0");
    [[maybe_unused]] TreeNode* core_ptr = core.get();
    root.addChild("core0", std::move(core));

    auto rob = std::make_unique<TreeNode>("rob");
    [[maybe_unused]] TreeNode* rob_ptr = rob.get();
    core_ptr->addChild("rob", std::move(rob));

    assert(root.findByAbsolutePath("cpu") == &root);
    assert(root.findByAbsolutePath("cpu.core0.rob") == rob_ptr);
    assert(core_ptr->findByAbsolutePath("cpu.core0.rob") == rob_ptr);

    // Backward-compatible root-relative spelling remains accepted.
    assert(core_ptr->findByAbsolutePath("core0.rob") == rob_ptr);

    std::cout << "  PASS" << std::endl;
}

void test_get_child_not_found() {
    std::cout << "Test: Get child not found" << std::endl;

    TreeNode root("root");

    assert(root.getChildByRelativePath("nonexistent") == nullptr);
    assert(root.getChildByRelativePath("a.b.c") == nullptr);

    std::cout << "  PASS" << std::endl;
}

void test_root_navigation() {
    std::cout << "Test: Root navigation" << std::endl;

    TreeNode root("cpu");

    auto core = std::make_unique<TreeNode>("core0");
    [[maybe_unused]] TreeNode* core_ptr = core.get();
    root.addChild("core0", std::move(core));

    auto rob = std::make_unique<TreeNode>("rob");
    [[maybe_unused]] TreeNode* rob_ptr = rob.get();
    core_ptr->addChild("rob", std::move(rob));

    assert(root.root() == &root);
    assert(core_ptr->root() == &root);
    assert(rob_ptr->root() == &root);

    std::cout << "  PASS" << std::endl;
}

void test_phase_transitions_valid() {
    std::cout << "Test: Phase transitions valid" << std::endl;

    TreeNode node("test");

    assert(node.phase() == TreeNode::Phase::BUILDING);

    node.transitionPhase(TreeNode::Phase::CONFIGURING);
    assert(node.phase() == TreeNode::Phase::CONFIGURING);

    node.transitionPhase(TreeNode::Phase::BINDING);
    assert(node.phase() == TreeNode::Phase::BINDING);

    node.transitionPhase(TreeNode::Phase::FINALIZED);
    assert(node.phase() == TreeNode::Phase::FINALIZED);

    std::cout << "  PASS" << std::endl;
}

void test_phase_transition_invalid() {
    std::cout << "Test: Phase transition invalid" << std::endl;

    TreeNode node("test");

    // Cannot jump phases
    [[maybe_unused]] bool threw1 = false;
    try {
        node.transitionPhase(TreeNode::Phase::BINDING);
    } catch (const std::runtime_error&) {
        threw1 = true;
    }
    assert(threw1 && "Cannot jump phases");

    // Cannot go backwards
    node.transitionPhase(TreeNode::Phase::CONFIGURING);
    [[maybe_unused]] bool threw2 = false;
    try {
        node.transitionPhase(TreeNode::Phase::BUILDING);
    } catch (const std::runtime_error&) {
        threw2 = true;
    }
    assert(threw2 && "Cannot go backwards");

    // Cannot transition from FINALIZED
    node.transitionPhase(TreeNode::Phase::BINDING);
    node.transitionPhase(TreeNode::Phase::FINALIZED);
    [[maybe_unused]] bool threw3 = false;
    try {
        node.transitionPhase(TreeNode::Phase::BUILDING);
    } catch (const std::runtime_error&) {
        threw3 = true;
    }
    assert(threw3 && "Cannot transition from FINALIZED");

    std::cout << "  PASS" << std::endl;
}

void test_phase_transition_recursive() {
    std::cout << "Test: Phase transition recursive" << std::endl;

    TreeNode root("root");

    auto child1 = std::make_unique<TreeNode>("child1");
    [[maybe_unused]] TreeNode* child1_ptr = child1.get();
    root.addChild("child1", std::move(child1));

    auto child2 = std::make_unique<TreeNode>("child2");
    [[maybe_unused]] TreeNode* child2_ptr = child2.get();
    root.addChild("child2", std::move(child2));

    root.transitionPhaseRecursive(TreeNode::Phase::CONFIGURING);

    assert(root.phase() == TreeNode::Phase::CONFIGURING);
    assert(child1_ptr->phase() == TreeNode::Phase::CONFIGURING);
    assert(child2_ptr->phase() == TreeNode::Phase::CONFIGURING);

    std::cout << "  PASS" << std::endl;
}

void test_resource_node_constructor() {
    std::cout << "Test: ResourceNode constructor" << std::endl;

    MockResource::ParameterSet params;
    params.value1 = 42;
    params.value2 = 100;
    params.name = "test_resource";

    ResourceNode<MockResource> node("test", params);

    assert(node.name() == "test");
    assert(node.params().value1 == 42);
    assert(node.params().value2 == 100);
    assert(node.params().name == "test_resource");
    assert(!node.isInstantiated());

    std::cout << "  PASS" << std::endl;
}

void test_resource_node_lazy_instantiation() {
    std::cout << "Test: ResourceNode lazy instantiation" << std::endl;

    MockResource::ParameterSet params;
    ResourceNode<MockResource> node("test", params);

    // Transition to BINDING phase
    node.transitionPhase(TreeNode::Phase::CONFIGURING);
    node.transitionPhase(TreeNode::Phase::BINDING);

    assert(!node.isInstantiated());

    // Access resource (should trigger instantiation)
    [[maybe_unused]] MockResource* resource = node.resource();

    assert(resource != nullptr);
    assert(node.isInstantiated());
    assert(resource->node_ptr == &node);
    // Use const params() to avoid triggering the exception
    [[maybe_unused]] const auto& const_node = node;
    assert(resource->params_ptr == &const_node.params());

    // Second access should return same instance
    [[maybe_unused]] MockResource* resource2 = node.resource();
    assert(resource == resource2);

    std::cout << "  PASS" << std::endl;
}

void test_resource_node_access_wrong_phase() {
    std::cout << "Test: ResourceNode access wrong phase" << std::endl;

    MockResource::ParameterSet params;
    ResourceNode<MockResource> node("test", params);

    // Cannot access resource in BUILDING phase
    [[maybe_unused]] bool threw1 = false;
    try {
        node.resource();
    } catch (const std::runtime_error&) {
        threw1 = true;
    }
    assert(threw1 && "Cannot access resource in BUILDING phase");

    // Can access in BINDING phase
    node.transitionPhase(TreeNode::Phase::CONFIGURING);
    node.transitionPhase(TreeNode::Phase::BINDING);

    [[maybe_unused]] MockResource* res = nullptr;
    [[maybe_unused]] bool no_throw = true;
    try {
        res = node.resource();
    } catch (...) {
        no_throw = false;
    }
    assert(no_throw && "Should access resource in BINDING phase");
    assert(res != nullptr);

    std::cout << "  PASS" << std::endl;
}

void test_resource_node_parameter_modification() {
    std::cout << "Test: ResourceNode parameter modification" << std::endl;

    MockResource::ParameterSet params;
    params.value1 = 10;

    ResourceNode<MockResource> node("test", params);

    // Can modify in BUILDING phase
    [[maybe_unused]] bool no_throw1 = true;
    try {
        node.params().value1 = 20;
    } catch (...) {
        no_throw1 = false;
    }
    assert(no_throw1 && "Should modify params in BUILDING phase");
    assert(node.params().value1 == 20);

    // Cannot modify in BINDING phase
    node.transitionPhase(TreeNode::Phase::CONFIGURING);
    node.transitionPhase(TreeNode::Phase::BINDING);

    [[maybe_unused]] bool threw = false;
    try {
        node.params().value1 = 40;
    } catch (const std::runtime_error&) {
        threw = true;
    }
    assert(threw && "Cannot modify params in BINDING phase");

    std::cout << "  PASS" << std::endl;
}

void test_complete_hierarchy() {
    std::cout << "Test: Complete hierarchy example" << std::endl;

    // Build a CPU hierarchy
    auto cpu = std::make_unique<TreeNode>("cpu");

    auto core0 = std::make_unique<TreeNode>("core0", cpu.get());
    TreeNode* core0_ptr = core0.get();
    cpu->addChild("core0", std::move(core0));

    auto rob = std::make_unique<TreeNode>("rob", core0_ptr);
    [[maybe_unused]] TreeNode* rob_ptr = rob.get();
    core0_ptr->addChild("rob", std::move(rob));

    auto lsu = std::make_unique<TreeNode>("lsu", core0_ptr);
    [[maybe_unused]] TreeNode* lsu_ptr = lsu.get();
    core0_ptr->addChild("lsu", std::move(lsu));

    // Test path navigation
    assert(rob_ptr->path() == "cpu.core0.rob");
    assert(lsu_ptr->path() == "cpu.core0.lsu");

    // Test getChild
    assert(cpu->getChildByRelativePath("core0") == core0_ptr);
    assert(cpu->getChildByRelativePath("core0.rob") == rob_ptr);
    assert(cpu->getChildByRelativePath("core0.lsu") == lsu_ptr);

    // Test root
    assert(rob_ptr->root() == cpu.get());
    assert(lsu_ptr->root() == cpu.get());

    // Test phase transitions
    cpu->transitionPhaseRecursive(TreeNode::Phase::CONFIGURING);
    assert(cpu->phase() == TreeNode::Phase::CONFIGURING);
    assert(core0_ptr->phase() == TreeNode::Phase::CONFIGURING);
    assert(rob_ptr->phase() == TreeNode::Phase::CONFIGURING);
    assert(lsu_ptr->phase() == TreeNode::Phase::CONFIGURING);

    std::cout << "  PASS" << std::endl;
}

int main() {
    std::cout << "\n=== TreeNode Tests ===" << std::endl;

    // Basic tests
    test_constructor_basic();
    test_constructor_with_parent();
    test_constructor_invalid_name();
    test_add_child();
    test_add_child_duplicate_name();
    test_add_child_wrong_phase();

    // Path navigation tests
    test_path_single_node();
    test_path_with_parent();
    test_path_deep_hierarchy();
    test_get_child_nested();
    test_absolute_path_navigation();
    test_get_child_not_found();
    test_root_navigation();

    // Lifecycle phase tests
    test_phase_transitions_valid();
    test_phase_transition_invalid();
    test_phase_transition_recursive();

    // ResourceNode tests
    test_resource_node_constructor();
    test_resource_node_lazy_instantiation();
    test_resource_node_access_wrong_phase();
    test_resource_node_parameter_modification();

    // Integration test
    test_complete_hierarchy();

    std::cout << "\n=== All Tests Passed ===" << std::endl;
    return 0;
}
