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

#include "ParamBase.hpp"
#include "Parameter.hpp"  // For ParameterType concept, toString, fromString
#include "ParameterSet.hpp"

namespace chronon::params {

/**
 * @brief Self-registering parameter that hooks into its owning ParameterSet.
 *
 * Registration in the constructor lets the owning set iterate every parameter
 * without manual listing, which drives YAML serialization/deserialization.
 *
 * @tparam T Parameter value type satisfying ParameterType.
 *
 * @code
 * class CPUParams : public ParameterSet {
 * public:
 *     Param<uint32_t> num_requests{this, "num_requests", 100, "Number of requests"};
 *     Param<uint32_t> request_delay{this, "request_delay", 1, "Request delay"};
 * };
 *
 * uint32_t n = params.num_requests;   // implicit conversion
 * params.num_requests = 50;           // assignment
 * @endcode
 */
template <ParameterType T>
class Param : public ParamBase {
public:
    /**
     * @brief Construct and self-register with @p owner.
     * @param owner Owning ParameterSet (non-null).
     */
    Param(ParameterSet* owner, const std::string& name, T default_val, const std::string& desc);

    // Non-copyable, non-movable: the owner stores this address.
    Param(const Param&) = delete;
    Param& operator=(const Param&) = delete;
    Param(Param&&) = delete;
    Param& operator=(Param&&) = delete;

    /// Implicit read access: `uint32_t n = params.num_requests;`
    operator const T&() const { return value_; }

    /// Assignment shorthand: `params.num_requests = 50;`
    Param& operator=(const T& v) {
        setValue(v);
        return *this;
    }

    const T& value() const { return value_; }

    /**
     * @brief Set value, applying validator if installed.
     * @throws std::invalid_argument if validation fails.
     */
    void setValue(const T& v) {
        if (validator_ && !validator_(v)) {
            throw std::invalid_argument("Validation failed for parameter '" + name_ + "'");
        }
        value_ = v;
    }

    const T& defaultValue() const { return default_; }

    void setValidator(std::function<bool(const T&)> validator) {
        validator_ = std::move(validator);
    }

    const std::string& name() const override { return name_; }
    const std::string& description() const override { return description_; }

    std::string valueAsString() const override { return toString(value_); }

    std::string defaultAsString() const override { return toString(default_); }

    void setFromString(const std::string& s) override {
        T new_value = fromString(s, std::type_identity<T>{});
        setValue(new_value);
    }

    void loadFromYAML(const void* yaml_node) override;  ///< Defined in YAMLSerialization.hpp.

    bool validate() const override {
        if (validator_) {
            return validator_(value_);
        }
        return true;
    }

    bool isModified() const override { return value_ != default_; }

    void reset() override { value_ = default_; }

private:
    std::string name_;
    T value_;
    T default_;
    std::string description_;
    std::function<bool(const T&)> validator_;
};

/**
 * @brief Convenience macro for declaring parameters.
 *
 * @code
 * class CPUParams : public ParameterSet {
 * public:
 *     CHRONON_PARAM(uint32_t, num_requests, 100, "Number of requests");
 *     CHRONON_PARAM(uint32_t, request_delay, 1, "Request delay");
 * };
 * @endcode
 */
#define CHRONON_PARAM(Type, Name, Default, Desc) \
    ::chronon::params::Param<Type> Name { this, #Name, Default, Desc }

template <ParameterType T>
Param<T>::Param(ParameterSet* owner, const std::string& name, T default_val,
                const std::string& desc)
    : name_(name),
      value_(default_val),
      default_(default_val),
      description_(desc),
      validator_(nullptr) {
    if (owner) {
        owner->registerParam(this);
    }
}

}  // namespace chronon::params
