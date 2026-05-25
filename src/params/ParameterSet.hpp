// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#pragma once

#include <functional>
#include <stdexcept>
#include <string>
#include <vector>

#include "ParamBase.hpp"

namespace chronon::params {

/**
 * @brief Base for parameter sets backed by self-registering Param<T> members.
 *
 * Provides validation, iteration, and YAML serialization that walk every
 * registered parameter without manual listing.
 *
 * @code
 * class CPUParams : public ParameterSet {
 * public:
 *     Param<uint32_t> num_requests{this, "num_requests", 100, "Number of requests"};
 *     Param<uint32_t> request_delay{this, "request_delay", 1, "Request delay"};
 * };
 *
 * uint32_t n = params.num_requests;
 * params.num_requests = 50;
 * @endcode
 */
class ParameterSet {
public:
    virtual ~ParameterSet() = default;

    /**
     * @brief Register a parameter (called from Param<T>'s constructor).
     *
     * @p param must outlive this ParameterSet.
     */
    void registerParam(ParamBase* param) { params_.push_back(param); }

    const std::vector<ParamBase*>& registeredParams() const { return params_; }

    /// @return true if every registered parameter validates.
    virtual bool validate() const {
        for (const auto* p : params_) {
            if (!p->validate()) {
                return false;
            }
        }
        return true;
    }

    /// Invoke @p callback with (name, value-as-string) for each registered parameter.
    virtual void forEachParam(
        std::function<void(const std::string&, const std::string&)> callback) const {
        for (const auto* p : params_) {
            callback(p->name(), p->valueAsString());
        }
    }

    /**
     * @brief Look up a parameter's value by name.
     * @throws std::runtime_error if no parameter has the given name.
     */
    virtual std::string getParamString(const std::string& name) const {
        for (const auto* p : params_) {
            if (p->name() == name) {
                return p->valueAsString();
            }
        }
        throw std::runtime_error("Parameter not found: " + name);
    }

    /**
     * @brief Parse and assign a parameter's value by name.
     * @throws std::runtime_error if no parameter has the given name or parsing fails.
     */
    virtual void setParamString(const std::string& name, const std::string& value) {
        for (auto* p : params_) {
            if (p->name() == name) {
                p->setFromString(value);
                return;
            }
        }
        throw std::runtime_error("Parameter not found: " + name);
    }

    /**
     * @brief Load every registered parameter from a YAML node (type-erased).
     * @param yaml_node Pointer to const YAML::Node; nullptr is a no-op.
     */
    virtual void deserializeYAML(const void* yaml_node) {
        if (!yaml_node) return;
        for (auto* p : params_) {
            p->loadFromYAML(yaml_node);
        }
    }

protected:
    std::vector<ParamBase*> params_;  ///< Non-owning pointers to registered params.
};

}  // namespace chronon::params
