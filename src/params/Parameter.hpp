// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#pragma once

#include <concepts>
#include <functional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <type_traits>

namespace chronon::params {

class Frequency;
class Latency;
class Size;
class Bandwidth;

template <typename T>
    requires(std::integral<T> || std::floating_point<T>) && (!std::is_same_v<T, bool>)
inline std::string toString(T value) {
    return std::to_string(value);
}

inline std::string toString(bool value) { return value ? "true" : "false"; }

inline std::string toString(const std::string& value) { return value; }

/// Parse string to integer with tag-dispatched return type.
template <typename T>
    requires std::integral<T>
inline T fromString(const std::string& s, std::type_identity<T> = {}) {
    try {
        if constexpr (std::is_same_v<T, bool>) {
            if (s == "true" || s == "1" || s == "True" || s == "TRUE") {
                return true;
            } else if (s == "false" || s == "0" || s == "False" || s == "FALSE") {
                return false;
            }
            throw std::invalid_argument("Invalid boolean string: " + s);
        } else if constexpr (std::is_signed_v<T>) {
            if constexpr (sizeof(T) <= sizeof(long)) {
                return static_cast<T>(std::stol(s));
            } else {
                return static_cast<T>(std::stoll(s));
            }
        } else {
            if constexpr (sizeof(T) <= sizeof(unsigned long)) {
                return static_cast<T>(std::stoul(s));
            } else {
                return static_cast<T>(std::stoull(s));
            }
        }
    } catch (const std::exception& e) {
        throw std::invalid_argument("Failed to parse '" + s + "' as integer: " + e.what());
    }
}

/// Parse string to floating-point value with tag-dispatched return type.
template <typename T>
    requires std::floating_point<T>
inline T fromString(const std::string& s, std::type_identity<T> = {}) {
    try {
        if constexpr (std::is_same_v<T, float>) {
            return std::stof(s);
        } else if constexpr (std::is_same_v<T, double>) {
            return std::stod(s);
        } else {
            return std::stold(s);
        }
    } catch (const std::exception& e) {
        throw std::invalid_argument("Failed to parse '" + s + "' as floating point: " + e.what());
    }
}

inline std::string fromString(const std::string& s, std::type_identity<std::string>) { return s; }

/**
 * @brief Concept for valid parameter types: copy-constructible and equality-comparable.
 *
 * Serialization (toString/fromString) is checked at use sites, not in the concept.
 */
template <typename T>
concept ParameterType = std::copy_constructible<T> && std::equality_comparable<T>;

/**
 * @brief Generic parameter descriptor with default, validation, and string serialization.
 *
 * @tparam T Parameter value type satisfying ParameterType.
 *
 * @code
 * StandaloneParameter<uint32_t> retire_width("retire_width", 4,
 *                                           "Instructions retired per cycle");
 * retire_width.setValueFromString("16");
 * uint32_t value = retire_width.value();
 * @endcode
 */
template <ParameterType T>
class StandaloneParameter {
public:
    StandaloneParameter(const std::string& name, T default_value, const std::string& description)
        : name_(name),
          value_(default_value),
          default_(default_value),
          description_(description),
          validator_(nullptr) {}

    const T& value() const { return value_; }

    /**
     * @brief Set value, applying validator if set.
     * @throws std::invalid_argument if validation fails.
     */
    void setValue(const T& v) {
        if (validator_ && !validator_(v)) {
            throw std::invalid_argument("Validation failed for parameter '" + name_ + "'");
        }
        value_ = v;
    }

    /**
     * @brief Parse and set value from string.
     * @throws std::invalid_argument if parsing or validation fails.
     */
    void setValueFromString(const std::string& s) {
        // Unqualified call enables both the templates above and ADL-found overloads.
        T new_value = fromString(s, std::type_identity<T>{});
        setValue(new_value);
    }

    std::string valueAsString() const { return toString(value_); }

    const std::string& name() const { return name_; }
    const std::string& description() const { return description_; }
    const T& defaultValue() const { return default_; }

    void reset() { value_ = default_; }

    bool isModified() const { return value_ != default_; }

    /**
     * @brief Install a validation callback.
     *
     * @code
     * param.setValidator([](uint32_t v) { return v > 0 && v <= 16; });
     * @endcode
     */
    void setValidator(std::function<bool(const T&)> validator) {
        validator_ = std::move(validator);
    }

    /// @return true if the current value passes the validator, or no validator is set.
    bool validate() const {
        if (validator_) {
            return validator_(value_);
        }
        return true;
    }

private:
    std::string name_;
    T value_;
    T default_;
    std::string description_;
    std::function<bool(const T&)> validator_;
};

/// Backward-compatible name for standalone, non-self-registering parameters.
/// Use Param<T> for ParameterSet members that must self-register.
template <ParameterType T>
using Parameter = StandaloneParameter<T>;

}  // namespace chronon::params
