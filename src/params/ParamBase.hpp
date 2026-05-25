// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#pragma once

#include <string>

namespace chronon::params {

/**
 * @brief Type-erased interface for self-registering parameters.
 *
 * Lets ParameterSet iterate every parameter without knowing the concrete
 * Param<T> type, which is what drives YAML serialization/deserialization.
 */
class ParamBase {
public:
    virtual ~ParamBase() = default;

    virtual const std::string& name() const = 0;
    virtual const std::string& description() const = 0;

    virtual std::string valueAsString() const = 0;
    virtual std::string defaultAsString() const = 0;

    /**
     * @brief Parse @p s and assign it as the current value.
     * @throws std::invalid_argument if parsing or validation fails.
     */
    virtual void setFromString(const std::string& s) = 0;

    /**
     * @brief Load from a YAML node passed type-erased as @c const YAML::Node*.
     *
     * Type-erased so headers don't drag in yaml-cpp. Implementations look up
     * a key matching @c name() on the node and assign if present.
     */
    virtual void loadFromYAML(const void* yaml_node) = 0;

    virtual bool validate() const = 0;

    /// @return true if the current value differs from the default.
    virtual bool isModified() const = 0;

    virtual void reset() = 0;
};

}  // namespace chronon::params
