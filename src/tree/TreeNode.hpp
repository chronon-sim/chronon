// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#pragma once

#include <functional>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

namespace chronon {
class Scheduler;
}

namespace chronon::tree {

/**
 * @brief Hierarchical tree node with path-based addressing and parameter inheritance.
 *
 * Foundation for the resource hierarchy system.
 *
 * @code
 * auto root = std::make_unique<TreeNode>("cpu");
 * auto core0 = std::make_unique<TreeNode>("core0", root.get());
 * root->addChild("core0", std::move(core0));
 * std::string path = root->getChildByRelativePath("core0")->path();  // "cpu.core0"
 * @endcode
 */
class TreeNode {
public:
    /**
     * @brief Lifecycle phases for tree construction.
     *
     * Progression: BUILDING → CONFIGURING → BINDING → FINALIZED.
     */
    enum class Phase {
        BUILDING,     ///< Tree construction (addChild allowed)
        CONFIGURING,  ///< Parameter application (setParam allowed)
        BINDING,      ///< Port and connection setup
        FINALIZED     ///< Ready for simulation (immutable)
    };

    /**
     * @param name Node name (must be unique among siblings).
     * @param parent Parent node (nullptr for root).
     */
    explicit TreeNode(const std::string& name, TreeNode* parent = nullptr);

    virtual ~TreeNode() = default;

    TreeNode(const TreeNode&) = delete;
    TreeNode& operator=(const TreeNode&) = delete;
    TreeNode(TreeNode&&) = default;
    TreeNode& operator=(TreeNode&&) = default;

    /**
     * @brief Add a child node, taking ownership.
     * @throws std::runtime_error if phase is not BUILDING or a sibling has the same name.
     */
    void addChild(const std::string& name, std::unique_ptr<TreeNode> child);

    /**
     * @brief Look up a descendant by relative dotted path (e.g. "core0.rob").
     * @return Pointer to the node, or nullptr if not found.
     */
    TreeNode* getChild(const std::string& path);
    const TreeNode* getChild(const std::string& path) const;

    /**
     * @brief Look up a descendant by relative dotted path (e.g. "core0.rob").
     *
     * Preferred semantic name for getChild().
     */
    TreeNode* getChildByRelativePath(const std::string& path);
    const TreeNode* getChildByRelativePath(const std::string& path) const;

    TreeNode* parent() const { return parent_; }
    const std::string& name() const { return name_; }

    /// Full dotted path from root (e.g. "cpu.core0.rob").
    std::string path() const;

    Phase phase() const { return phase_; }

    /**
     * @brief Transition to a new lifecycle phase.
     * @throws std::runtime_error if the transition is invalid.
     */
    void transitionPhase(Phase new_phase);

    /// Recursively transition this node and all descendants.
    void transitionPhaseRecursive(Phase new_phase);

    const std::unordered_map<std::string, std::unique_ptr<TreeNode>>& children() const {
        return children_;
    }

    bool isRoot() const { return parent_ == nullptr; }

    TreeNode* root();
    const TreeNode* root() const;

    /**
     * @brief Find a node by absolute path from root (e.g. "cpu.core0.rob").
     * @return Pointer to node, or nullptr if not found.
     */
    TreeNode* findByPath(const std::string& absolute_path);
    const TreeNode* findByPath(const std::string& absolute_path) const;

    /**
     * @brief Find a node by absolute path from root (e.g. "cpu.core0.rob").
     *
     * Preferred semantic name for findByPath().
     */
    TreeNode* findByAbsolutePath(const std::string& absolute_path);
    const TreeNode* findByAbsolutePath(const std::string& absolute_path) const;

    static std::string phaseToString(Phase p);

    /// Set scheduler reference (typically on root during SimulationBuilder Phase 2).
    void setScheduler(chronon::Scheduler* scheduler) { scheduler_ = scheduler; }

    /// Get scheduler, traversing up to root.
    chronon::Scheduler* getScheduler() const;

    /// Store a type-erased shared resource pointer.
    void setResource(std::shared_ptr<void> resource) { resource_ = std::move(resource); }

    /**
     * @brief Get resource cast to T.
     *
     * No type checking is performed — caller must ensure the type matches.
     */
    template <typename T>
    T* getResource() const {
        return static_cast<T*>(resource_.get());
    }

    bool hasResource() const { return resource_ != nullptr; }

protected:
    std::string name_;
    TreeNode* parent_;
    std::unordered_map<std::string, std::unique_ptr<TreeNode>> children_;
    Phase phase_ = Phase::BUILDING;
    chronon::Scheduler* scheduler_ = nullptr;  ///< Not owned; typically set on root only.
    std::shared_ptr<void> resource_;

private:
    static std::vector<std::string> splitPath(const std::string& path);
    static bool isValidTransition(Phase from, Phase to);
};

/**
 * @brief Tree node that owns a typed resource with lazy instantiation.
 *
 * @tparam ResourceT Resource type with a nested ParameterSet.
 *
 * @code
 * ROB::ParameterSet params;
 * params.retire_width = 8;
 * auto node = std::make_unique<ResourceNode<ROB>>("rob", params);
 * ROB* rob = node->resource();  // Lazy instantiation
 * @endcode
 */
template <typename ResourceT>
class ResourceNode : public TreeNode {
public:
    using ParameterSetT = typename ResourceT::ParameterSet;

    ResourceNode(const std::string& name, const ParameterSetT& params, TreeNode* parent = nullptr)
        : TreeNode(name, parent), params_(params), resource_(nullptr) {}

    /**
     * @brief Get or lazily create the resource.
     * @throws std::runtime_error if phase is not BINDING or FINALIZED.
     */
    ResourceT* resource() {
        if (phase_ != Phase::BINDING && phase_ != Phase::FINALIZED) {
            throw std::runtime_error("Cannot access resource in phase " + phaseToString(phase_) +
                                     " (must be BINDING or FINALIZED)");
        }

        if (!resource_) {
            resource_ = std::make_unique<ResourceT>(this, &params_);
        }
        return resource_.get();
    }

    /// @return Resource pointer, or nullptr if not yet instantiated.
    const ResourceT* resource() const { return resource_.get(); }

    const ParameterSetT& params() const { return params_; }

    /**
     * @brief Mutable parameter access.
     * @throws std::runtime_error if phase is not BUILDING or CONFIGURING.
     */
    ParameterSetT& params() {
        if (phase_ != Phase::BUILDING && phase_ != Phase::CONFIGURING) {
            throw std::runtime_error("Cannot modify parameters in phase " + phaseToString(phase_) +
                                     " (must be BUILDING or CONFIGURING)");
        }
        return params_;
    }

    bool isInstantiated() const { return resource_ != nullptr; }

private:
    ParameterSetT params_;
    std::unique_ptr<ResourceT> resource_;
};

}  // namespace chronon::tree
