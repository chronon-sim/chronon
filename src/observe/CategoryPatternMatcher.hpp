// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// CategoryPatternMatcher.hpp
//
// Glob-style pattern matching for trace categories.
// Used by ObservationManager to resolve YAML patterns to CategoryMasks.

#pragma once

#include <string>
#include <string_view>
#include <vector>

#include "ObservationApi.hpp"
#include "Types.hpp"

namespace chronon::observe {

/**
 * CategoryPatternMatcher - Matches category names against glob patterns.
 *
 * Supports the following pattern types:
 * - "*" : Match all categories
 * - "prefix*" or "prefix_*" : Match categories starting with prefix
 * - "*suffix" : Match categories ending with suffix
 * - "exact" : Match exactly this name
 *
 * Examples:
 *   matches("*", "icache_hit")        -> true
 *   matches("icache_*", "icache_hit") -> true
 *   matches("icache_*", "dcache_hit") -> false
 *   matches("*_hit", "icache_hit")    -> true
 *   matches("branch_pred", "branch_pred") -> true
 */
class CategoryPatternMatcher {
public:
    /**
     * Check if a category name matches a pattern.
     *
     * @param pattern The glob pattern (e.g., "icache_*", "*", "exact")
     * @param name The category name to test
     * @return true if name matches pattern
     */
    static bool matches(std::string_view pattern, std::string_view name) {
        if (pattern.empty()) {
            return false;
        }

        // "*" matches everything
        if (pattern == "*") {
            return true;
        }

        // Check for wildcard position
        size_t wildcard_pos = pattern.find('*');

        if (wildcard_pos == std::string_view::npos) {
            // No wildcard - exact match
            return pattern == name;
        }

        if (wildcard_pos == 0) {
            // "*suffix" - match ending
            std::string_view suffix = pattern.substr(1);
            if (suffix.empty()) {
                return true;  // Just "*"
            }
            return name.size() >= suffix.size() &&
                   name.substr(name.size() - suffix.size()) == suffix;
        }

        if (wildcard_pos == pattern.size() - 1) {
            // "prefix*" - match beginning
            std::string_view prefix = pattern.substr(0, wildcard_pos);
            return name.size() >= prefix.size() && name.substr(0, prefix.size()) == prefix;
        }

        // "prefix*suffix" - match both
        std::string_view prefix = pattern.substr(0, wildcard_pos);
        std::string_view suffix = pattern.substr(wildcard_pos + 1);

        if (name.size() < prefix.size() + suffix.size()) {
            return false;
        }

        return name.substr(0, prefix.size()) == prefix &&
               name.substr(name.size() - suffix.size()) == suffix;
    }

    /**
     * Resolve a pattern to a combined CategoryMask using the CategoryRegistry.
     *
     * @param pattern The glob pattern
     * @return Combined CategoryMask of all matching categories (0 if none match)
     */
    static CategoryMask resolvePattern(std::string_view pattern) {
        CategoryMask combined = 0;
        const auto& entries = CategoryRegistry::instance().entries();

        for (const auto& entry : entries) {
            if (matches(pattern, entry.name)) {
                combined |= entry.mask;
            }
        }

        return combined;
    }

    /**
     * Get all matching category entries for a pattern.
     *
     * @param pattern The glob pattern
     * @return Vector of matching CategoryEntry references
     */
    static std::vector<const CategoryRegistry::CategoryEntry*> getMatchingCategories(
        std::string_view pattern) {
        std::vector<const CategoryRegistry::CategoryEntry*> result;
        const auto& entries = CategoryRegistry::instance().entries();

        for (const auto& entry : entries) {
            if (matches(pattern, entry.name)) {
                result.push_back(&entry);
            }
        }

        return result;
    }

    /**
     * Check if a pattern would match any registered category.
     *
     * @param pattern The glob pattern
     * @return true if at least one category matches
     */
    static bool hasMatchingCategories(std::string_view pattern) {
        const auto& entries = CategoryRegistry::instance().entries();

        for (const auto& entry : entries) {
            if (matches(pattern, entry.name)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Resolve multiple patterns to a combined CategoryMask.
     *
     * @param patterns Vector of glob patterns
     * @return Combined CategoryMask of all matching categories
     */
    static CategoryMask resolvePatterns(const std::vector<std::string>& patterns) {
        CategoryMask combined = 0;
        for (const auto& pattern : patterns) {
            combined |= resolvePattern(pattern);
        }
        return combined;
    }
};

}  // namespace chronon::observe
