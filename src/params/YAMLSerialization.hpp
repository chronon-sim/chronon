// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#pragma once

#include <yaml-cpp/yaml.h>

#include <string>

#include "Param.hpp"
#include "Parameter.hpp"
#include "ParameterSet.hpp"
#include "UnitTypes.hpp"

namespace chronon::params {

template <typename T>
    requires std::integral<T> || std::floating_point<T>
inline void toYAML(YAML::Emitter& out, const T& value) {
    out << value;
}

inline void toYAML(YAML::Emitter& out, const std::string& value) { out << value; }

inline void toYAML(YAML::Emitter& out, bool value) { out << (value ? "true" : "false"); }

template <typename T>
    requires std::integral<T>
inline T fromYAML(const YAML::Node& node, std::type_identity<T> = {}) {
    if constexpr (std::is_same_v<T, bool>) {
        return node.as<bool>();
    } else {
        return node.as<T>();
    }
}

template <typename T>
    requires std::floating_point<T>
inline T fromYAML(const YAML::Node& node, std::type_identity<T> = {}) {
    return node.as<T>();
}

inline std::string fromYAML(const YAML::Node& node, std::type_identity<std::string>) {
    return node.as<std::string>();
}

inline void toYAML(YAML::Emitter& out, const Frequency& freq) { out << freq.toString(); }
inline void toYAML(YAML::Emitter& out, const Latency& lat) { out << lat.toString(); }
inline void toYAML(YAML::Emitter& out, const Size& size) { out << size.toString(); }
inline void toYAML(YAML::Emitter& out, const Bandwidth& bw) { out << bw.toString(); }

inline Frequency fromYAML(const YAML::Node& node, std::type_identity<Frequency>) {
    return Frequency(node.as<std::string>());
}
inline Latency fromYAML(const YAML::Node& node, std::type_identity<Latency>) {
    return Latency(node.as<std::string>());
}
inline Size fromYAML(const YAML::Node& node, std::type_identity<Size>) {
    return Size(node.as<std::string>());
}
inline Bandwidth fromYAML(const YAML::Node& node, std::type_identity<Bandwidth>) {
    return Bandwidth(node.as<std::string>());
}

template <ParameterType T>
void serializeStandaloneParameter(YAML::Emitter& out, const StandaloneParameter<T>& param) {
    out << YAML::BeginMap;
    out << YAML::Key << "name" << YAML::Value << param.name();
    out << YAML::Key << "value" << YAML::Value;
    toYAML(out, param.value());
    out << YAML::Key << "default" << YAML::Value;
    toYAML(out, param.defaultValue());
    out << YAML::Key << "description" << YAML::Value << param.description();
    out << YAML::EndMap;
}

template <ParameterType T>
void deserializeStandaloneParameter(const YAML::Node& node, StandaloneParameter<T>& param) {
    if (node["value"]) {
        T value = fromYAML(node["value"], std::type_identity<T>{});
        param.setValue(value);
    }
}

template <ParameterType T>
void serializeParameter(YAML::Emitter& out, const StandaloneParameter<T>& param) {
    serializeStandaloneParameter(out, param);
}

template <ParameterType T>
void deserializeParameter(const YAML::Node& node, StandaloneParameter<T>& param) {
    deserializeStandaloneParameter(node, param);
}

template <typename T>
inline void serializeParamValue(YAML::Emitter& out, const StandaloneParameter<T>& param) {
    out << YAML::Key << param.name();
    out << YAML::Value;
    toYAML(out, param.value());
}

template <typename T>
inline void deserializeParamValue(const YAML::Node& node, const std::string& name,
                                  StandaloneParameter<T>& param) {
    if (node[name]) {
        T value = fromYAML(node[name], std::type_identity<T>{});
        param.setValue(value);
    }
}

template <ParameterType T>
void Param<T>::loadFromYAML(const void* yaml_node_ptr) {
    if (!yaml_node_ptr) return;
    const YAML::Node& node = *static_cast<const YAML::Node*>(yaml_node_ptr);
    if (node[name_]) {
        T value = fromYAML(node[name_], std::type_identity<T>{});
        setValue(value);
    }
}

template <ParameterType T>
inline void serializeParamValue(YAML::Emitter& out, const Param<T>& param) {
    out << YAML::Key << param.name();
    out << YAML::Value;
    toYAML(out, param.value());
}

inline void serializeParameterSet(YAML::Emitter& out, const ParameterSet& params) {
    out << YAML::BeginMap;
    for (const auto* p : params.registeredParams()) {
        out << YAML::Key << p->name();
        out << YAML::Value << p->valueAsString();
    }
    out << YAML::EndMap;
}

}  // namespace chronon::params
