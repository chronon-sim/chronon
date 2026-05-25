// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#pragma once

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <typeinfo>
#include <vector>

#include "../core/TickableUnit.hpp"
#include "../core/Unit.hpp"
#include "../port/Connection.hpp"
#include "TickCostProfiler.hpp"

namespace chronon::sender {

/**
 * @brief Persistent cache of TickCostProfiler results keyed on a topology hash.
 *
 * Eliminates rdtsc-measurement noise across successive runs of the same simulation: once
 * cached, the partitioner receives bit-identical unit costs on every run, making thread
 * assignment a pure function of topology rather than wall-clock variance. Cache key is
 * derived from sorted (unit_name, unit_type) and (src, dst, delay) — any topology change
 * (including delays) forces a fresh profile.
 */
class CostProfileCache {
public:
    static uint64_t hashTopology(const std::vector<TickableUnit*>& units,
                                 const std::vector<ConnectionBase*>& connections) {
        std::vector<std::string> unit_entries;
        unit_entries.reserve(units.size());
        for (auto* unit : units) {
            if (unit == nullptr) continue;
            std::string entry = unit->name();
            entry.push_back('|');
            entry.append(typeid(*unit).name());
            unit_entries.push_back(std::move(entry));
        }
        std::sort(unit_entries.begin(), unit_entries.end());

        std::vector<std::string> conn_entries;
        conn_entries.reserve(connections.size());
        for (auto* conn : connections) {
            if (conn == nullptr) continue;
            Unit* src = conn->source();
            Unit* dst = conn->destination();
            if (src == nullptr || dst == nullptr) continue;
            std::string entry = src->name();
            entry.push_back('>');
            entry.append(dst->name());
            entry.push_back('@');
            entry.append(std::to_string(conn->delay()));
            conn_entries.push_back(std::move(entry));
        }
        std::sort(conn_entries.begin(), conn_entries.end());

        uint64_t h = 0xcbf29ce484222325ULL;
        auto mix = [&h](std::string_view sv) {
            for (unsigned char c : sv) {
                h ^= c;
                h *= 0x100000001b3ULL;
            }
            h ^= '\n';
            h *= 0x100000001b3ULL;
        };
        mix("units:");
        for (const auto& e : unit_entries) mix(e);
        mix("conns:");
        for (const auto& e : conn_entries) mix(e);
        return h;
    }

    static std::optional<std::vector<UnitTickProfile>> load(const std::string& path,
                                                            uint64_t topology_hash) {
        if (path.empty()) return std::nullopt;
        std::error_code ec;
        if (!std::filesystem::exists(path, ec) || ec) return std::nullopt;

        std::ifstream in(path, std::ios::binary);
        if (!in) return std::nullopt;
        std::stringstream buf;
        buf << in.rdbuf();
        const std::string text = buf.str();

        uint64_t file_hash = 0;
        int version = 0;
        if (!extractUintField_(text, "version", version)) return std::nullopt;
        if (version != 1) return std::nullopt;
        if (!extractHexField_(text, "topology_hash", file_hash)) return std::nullopt;
        if (file_hash != topology_hash) return std::nullopt;

        std::vector<UnitTickProfile> profiles;
        if (!extractProfiles_(text, profiles)) return std::nullopt;
        return profiles;
    }

    static bool save(const std::string& path, uint64_t topology_hash,
                     const std::vector<UnitTickProfile>& profiles,
                     const std::vector<std::string>& unit_names = {}) {
        if (path.empty()) return false;

        std::filesystem::path out_path(path);
        std::error_code ec;
        if (out_path.has_parent_path()) {
            std::filesystem::create_directories(out_path.parent_path(), ec);
        }
        std::filesystem::path tmp_path = out_path;
        tmp_path += ".tmp";

        {
            std::ofstream out(tmp_path, std::ios::binary | std::ios::trunc);
            if (!out) return false;
            out << "{\n";
            out << "  \"version\": 1,\n";
            out << "  \"topology_hash\": \"" << formatHex_(topology_hash) << "\",\n";
            out << "  \"profiles\": [";
            for (size_t i = 0; i < profiles.size(); ++i) {
                out << (i == 0 ? "\n" : ",\n");
                out << "    {";
                if (i < unit_names.size()) {
                    out << "\"unit_name\": \"" << escapeJson_(unit_names[i]) << "\", ";
                }
                out << "\"mean_ns\": " << formatDouble_(profiles[i].mean_ns) << ", ";
                out << "\"median_ns\": " << formatDouble_(profiles[i].median_ns) << ", ";
                out << "\"sample_count\": " << profiles[i].sample_count;
                out << "}";
            }
            out << (profiles.empty() ? "]\n" : "\n  ]\n");
            out << "}\n";
            out.flush();
            if (!out) return false;
        }

        std::filesystem::rename(tmp_path, out_path, ec);
        if (ec) {
            std::filesystem::remove(tmp_path, ec);
            return false;
        }
        return true;
    }

private:
    static std::string formatHex_(uint64_t value) {
        char buf[32];
        std::snprintf(buf, sizeof(buf), "0x%016llx", static_cast<unsigned long long>(value));
        return buf;
    }

    static std::string formatDouble_(double value) {
        char buf[64];
        std::snprintf(buf, sizeof(buf), "%.17g", value);
        return buf;
    }

    static std::string escapeJson_(const std::string& s) {
        std::string out;
        out.reserve(s.size());
        for (char c : s) {
            switch (c) {
                case '"':
                    out.append("\\\"");
                    break;
                case '\\':
                    out.append("\\\\");
                    break;
                case '\n':
                    out.append("\\n");
                    break;
                case '\r':
                    out.append("\\r");
                    break;
                case '\t':
                    out.append("\\t");
                    break;
                default:
                    if (static_cast<unsigned char>(c) < 0x20) {
                        char ebuf[8];
                        std::snprintf(ebuf, sizeof(ebuf), "\\u%04x", static_cast<unsigned int>(c));
                        out.append(ebuf);
                    } else {
                        out.push_back(c);
                    }
            }
        }
        return out;
    }

    static size_t findField_(const std::string& text, const std::string& key) {
        const std::string needle = "\"" + key + "\"";
        size_t pos = 0;
        while ((pos = text.find(needle, pos)) != std::string::npos) {
            size_t colon = text.find(':', pos + needle.size());
            if (colon == std::string::npos) return std::string::npos;
            return colon + 1;
        }
        return std::string::npos;
    }

    static bool extractUintField_(const std::string& text, const std::string& key, int& out) {
        size_t start = findField_(text, key);
        if (start == std::string::npos) return false;
        while (start < text.size() &&
               (std::isspace(static_cast<unsigned char>(text[start])) != 0)) {
            ++start;
        }
        char* end = nullptr;
        long val = std::strtol(text.c_str() + start, &end, 10);
        if (end == text.c_str() + start) return false;
        out = static_cast<int>(val);
        return true;
    }

    static bool extractHexField_(const std::string& text, const std::string& key, uint64_t& out) {
        size_t start = findField_(text, key);
        if (start == std::string::npos) return false;
        size_t quote = text.find('"', start);
        if (quote == std::string::npos) return false;
        size_t end = text.find('"', quote + 1);
        if (end == std::string::npos) return false;
        std::string hex = text.substr(quote + 1, end - quote - 1);
        if (hex.rfind("0x", 0) == 0 || hex.rfind("0X", 0) == 0) hex.erase(0, 2);
        if (hex.empty()) return false;
        char* stop = nullptr;
        out = std::strtoull(hex.c_str(), &stop, 16);
        return stop != hex.c_str();
    }

    static bool extractProfiles_(const std::string& text, std::vector<UnitTickProfile>& out) {
        size_t arr_start = findField_(text, "profiles");
        if (arr_start == std::string::npos) return false;
        size_t lb = text.find('[', arr_start);
        if (lb == std::string::npos) return false;
        size_t rb = text.find(']', lb);
        if (rb == std::string::npos) return false;

        size_t cursor = lb + 1;
        while (cursor < rb) {
            size_t obj_start = text.find('{', cursor);
            if (obj_start == std::string::npos || obj_start >= rb) break;
            size_t obj_end = text.find('}', obj_start);
            if (obj_end == std::string::npos || obj_end > rb) return false;
            std::string obj = text.substr(obj_start, obj_end - obj_start + 1);

            UnitTickProfile p{};
            if (!extractDoubleInObject_(obj, "mean_ns", p.mean_ns)) return false;
            if (!extractDoubleInObject_(obj, "median_ns", p.median_ns)) return false;
            uint64_t sample_count = 0;
            if (!extractUint64InObject_(obj, "sample_count", sample_count)) return false;
            p.sample_count = sample_count;
            out.push_back(p);
            cursor = obj_end + 1;
        }
        return true;
    }

    static bool extractDoubleInObject_(const std::string& obj, const std::string& key,
                                       double& out) {
        size_t start = findField_(obj, key);
        if (start == std::string::npos) return false;
        while (start < obj.size() && (std::isspace(static_cast<unsigned char>(obj[start])) != 0)) {
            ++start;
        }
        char* end = nullptr;
        out = std::strtod(obj.c_str() + start, &end);
        return end != obj.c_str() + start;
    }

    static bool extractUint64InObject_(const std::string& obj, const std::string& key,
                                       uint64_t& out) {
        size_t start = findField_(obj, key);
        if (start == std::string::npos) return false;
        while (start < obj.size() && (std::isspace(static_cast<unsigned char>(obj[start])) != 0)) {
            ++start;
        }
        char* end = nullptr;
        out = std::strtoull(obj.c_str() + start, &end, 10);
        return end != obj.c_str() + start;
    }
};

}  // namespace chronon::sender
