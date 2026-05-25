// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#pragma once

#include <yaml-cpp/yaml.h>

#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace chronon::sender::config {

/** @brief Thrown when a YAML override path or format is invalid. */
class YAMLOverrideError : public std::runtime_error {
public:
    explicit YAMLOverrideError(const std::string& message) : std::runtime_error(message) {}
};

/**
 * @brief Applies dot-notation key=value overrides to a YAML configuration.
 *
 * Example paths: "simulation.observation.logging.min_level=warn",
 * "simulation.unit.fetch.params.icache_lines=20".
 */
class YAMLOverride {
public:
    /** @brief Parsed override: path and raw value string. */
    struct Override {
        std::string path;
        std::string value;

        Override(const std::string& p, const std::string& v) : path(p), value(v) {}
    };

    static Override parseOverrideString(const std::string& override_str) {
        size_t eq_pos = override_str.find('=');
        if (eq_pos == std::string::npos) {
            throw YAMLOverrideError("Invalid override format (missing '='): " + override_str);
        }

        std::string path = override_str.substr(0, eq_pos);
        std::string value = override_str.substr(eq_pos + 1);

        if (path.empty()) {
            throw YAMLOverrideError("Invalid override format (empty path): " + override_str);
        }

        return Override(path, value);
    }

    static void applyOverride(YAML::Node& root, const Override& override) {
        std::vector<std::string> path_parts = splitPath(override.path);

        if (path_parts.empty()) {
            throw YAMLOverrideError("Invalid override path (empty): " + override.path);
        }

        // Recursion avoids yaml-cpp's surprising node-assignment semantics.
        applyOverrideRecursive(root, path_parts, 0, override.value, override.path);
    }

private:
    static void applyOverrideRecursive(YAML::Node node, const std::vector<std::string>& path_parts,
                                       size_t index, const std::string& value,
                                       const std::string& full_path) {
        const std::string& key = path_parts[index];

        if (index == path_parts.size() - 1) {
            node[key] = convertValue(value);
            return;
        }

        if (!node[key]) {
            node[key] = YAML::Node(YAML::NodeType::Map);
        }

        if (!node[key].IsMap()) {
            throw YAMLOverrideError("Cannot override: intermediate path element is not a map: " +
                                    full_path + " (at " + key + ")");
        }

        applyOverrideRecursive(node[key], path_parts, index + 1, value, full_path);
    }

public:
    static void applyOverride(YAML::Node& root, const std::string& override_str) {
        Override override = parseOverrideString(override_str);
        applyOverride(root, override);
    }

    static void applyOverrides(YAML::Node& root, const std::vector<Override>& overrides) {
        for (const auto& override : overrides) {
            applyOverride(root, override);
        }
    }

    static void applyOverrides(YAML::Node& root, const std::vector<std::string>& override_strings) {
        for (const auto& override_str : override_strings) {
            applyOverride(root, override_str);
        }
    }

private:
    static std::vector<std::string> splitPath(const std::string& path) {
        std::vector<std::string> parts;
        size_t start = 0;
        size_t end = 0;

        while ((end = path.find('.', start)) != std::string::npos) {
            if (end > start) {
                parts.push_back(path.substr(start, end - start));
            }
            start = end + 1;
        }

        if (start < path.length()) {
            parts.push_back(path.substr(start));
        }

        return parts;
    }

    /// Heuristic: "true"/"false" → bool; digits → int; digits with '.' → double; else string.
    static YAML::Node convertValue(const std::string& value) {
        if (value == "true") {
            return YAML::Node(true);
        }
        if (value == "false") {
            return YAML::Node(false);
        }

        bool is_int = !value.empty() && (value[0] == '-' || std::isdigit(value[0]));
        if (is_int) {
            for (size_t i = 1; i < value.length(); ++i) {
                if (!std::isdigit(value[i])) {
                    is_int = false;
                    break;
                }
            }

            if (is_int) {
                try {
                    return YAML::Node(std::stoll(value));
                } catch (...) {
                }
            }
        }

        bool is_double =
            !value.empty() && (value[0] == '-' || std::isdigit(value[0]) || value[0] == '.');
        if (is_double) {
            bool has_dot = false;
            for (size_t i = 1; i < value.length(); ++i) {
                if (value[i] == '.') {
                    if (has_dot) {
                        is_double = false;
                        break;
                    }
                    has_dot = true;
                } else if (!std::isdigit(value[i])) {
                    is_double = false;
                    break;
                }
            }

            if (is_double && has_dot) {
                try {
                    return YAML::Node(std::stod(value));
                } catch (...) {
                }
            }
        }

        return YAML::Node(value);
    }
};

}  // namespace chronon::sender::config
