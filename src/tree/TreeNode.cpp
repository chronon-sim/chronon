// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#include "TreeNode.hpp"

#include <algorithm>

namespace chronon::tree {

TreeNode::TreeNode(const std::string& name, TreeNode* parent)
    : name_(name), parent_(parent), phase_(Phase::BUILDING) {
    if (name.empty()) {
        throw std::invalid_argument("TreeNode name cannot be empty");
    }

    if (name.find('.') != std::string::npos) {
        throw std::invalid_argument("TreeNode name cannot contain '.' character: " + name);
    }
}

void TreeNode::addChild(const std::string& name, std::unique_ptr<TreeNode> child) {
    if (phase_ != Phase::BUILDING) {
        throw std::runtime_error("Cannot add child in phase " + phaseToString(phase_) +
                                 " (must be BUILDING)");
    }

    if (!child) {
        throw std::invalid_argument("Cannot add null child");
    }

    if (children_.contains(name)) {
        throw std::runtime_error("Child with name '" + name + "' already exists under node '" +
                                 name_ + "'");
    }

    if (child->name() != name) {
        throw std::invalid_argument("Child name mismatch: expected '" + name + "', got '" +
                                    child->name() + "'");
    }

    child->parent_ = this;
    children_[name] = std::move(child);
}

TreeNode* TreeNode::getChild(const std::string& path) { return getChildByRelativePath(path); }

const TreeNode* TreeNode::getChild(const std::string& path) const {
    return getChildByRelativePath(path);
}

TreeNode* TreeNode::getChildByRelativePath(const std::string& path) {
    if (path.empty()) {
        return this;
    }

    auto segments = splitPath(path);
    if (segments.empty()) {
        return this;
    }

    TreeNode* current = this;
    for (const auto& segment : segments) {
        auto it = current->children_.find(segment);
        if (it == current->children_.end()) {
            return nullptr;
        }
        current = it->second.get();
    }

    return current;
}

const TreeNode* TreeNode::getChildByRelativePath(const std::string& path) const {
    if (path.empty()) {
        return this;
    }

    auto segments = splitPath(path);
    if (segments.empty()) {
        return this;
    }

    const TreeNode* current = this;
    for (const auto& segment : segments) {
        auto it = current->children_.find(segment);
        if (it == current->children_.end()) {
            return nullptr;
        }
        current = it->second.get();
    }

    return current;
}

std::string TreeNode::path() const {
    if (isRoot()) {
        return name_;
    }

    std::vector<std::string> segments;
    const TreeNode* current = this;

    while (current != nullptr) {
        segments.push_back(current->name_);
        current = current->parent_;
    }

    std::reverse(segments.begin(), segments.end());

    std::ostringstream oss;
    for (size_t i = 0; i < segments.size(); ++i) {
        if (i > 0) {
            oss << '.';
        }
        oss << segments[i];
    }

    return oss.str();
}

void TreeNode::transitionPhase(Phase new_phase) {
    if (!isValidTransition(phase_, new_phase)) {
        throw std::runtime_error("Invalid phase transition: " + phaseToString(phase_) + " -> " +
                                 phaseToString(new_phase));
    }

    phase_ = new_phase;
}

void TreeNode::transitionPhaseRecursive(Phase new_phase) {
    transitionPhase(new_phase);

    for (auto& [name, child] : children_) {
        child->transitionPhaseRecursive(new_phase);
    }
}

TreeNode* TreeNode::root() {
    TreeNode* current = this;
    while (current->parent_ != nullptr) {
        current = current->parent_;
    }
    return current;
}

const TreeNode* TreeNode::root() const {
    const TreeNode* current = this;
    while (current->parent_ != nullptr) {
        current = current->parent_;
    }
    return current;
}

chronon::Scheduler* TreeNode::getScheduler() const {
    const TreeNode* current = this;
    while (current != nullptr) {
        if (current->scheduler_ != nullptr) {
            return current->scheduler_;
        }
        current = current->parent_;
    }
    return nullptr;
}

TreeNode* TreeNode::findByPath(const std::string& absolute_path) {
    return findByAbsolutePath(absolute_path);
}

const TreeNode* TreeNode::findByPath(const std::string& absolute_path) const {
    return findByAbsolutePath(absolute_path);
}

TreeNode* TreeNode::findByAbsolutePath(const std::string& absolute_path) {
    TreeNode* root_node = root();
    if (absolute_path == root_node->name()) {
        return root_node;
    }

    const std::string root_prefix = root_node->name() + ".";
    if (absolute_path.rfind(root_prefix, 0) == 0) {
        return root_node->getChildByRelativePath(absolute_path.substr(root_prefix.size()));
    }

    // Backward-compatible fallback for existing callers that passed a
    // root-relative path without the root node name.
    return root_node->getChildByRelativePath(absolute_path);
}

const TreeNode* TreeNode::findByAbsolutePath(const std::string& absolute_path) const {
    const TreeNode* root_node = root();
    if (absolute_path == root_node->name()) {
        return root_node;
    }

    const std::string root_prefix = root_node->name() + ".";
    if (absolute_path.rfind(root_prefix, 0) == 0) {
        return root_node->getChildByRelativePath(absolute_path.substr(root_prefix.size()));
    }

    // Backward-compatible fallback for existing callers that passed a
    // root-relative path without the root node name.
    return root_node->getChildByRelativePath(absolute_path);
}

std::string TreeNode::phaseToString(Phase p) {
    static constexpr const char* phase_names[] = {"BUILDING", "CONFIGURING", "BINDING",
                                                  "FINALIZED"};

    auto index = static_cast<size_t>(p);
    if (index < std::size(phase_names)) {
        return phase_names[index];
    }
    return "UNKNOWN";
}

std::vector<std::string> TreeNode::splitPath(const std::string& path) {
    std::vector<std::string> segments;
    std::istringstream iss(path);
    std::string segment;

    while (std::getline(iss, segment, '.')) {
        if (!segment.empty()) {
            segments.push_back(segment);
        }
    }

    return segments;
}

bool TreeNode::isValidTransition(Phase from, Phase to) {
    if (from == to) {
        return true;
    }

    switch (from) {
        case Phase::BUILDING:
            return to == Phase::CONFIGURING;
        case Phase::CONFIGURING:
            return to == Phase::BINDING;
        case Phase::BINDING:
            return to == Phase::FINALIZED;
        case Phase::FINALIZED:
            return false;
        default:
            return false;
    }
}

}  // namespace chronon::tree
