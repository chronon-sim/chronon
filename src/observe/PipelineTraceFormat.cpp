// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#include "PipelineTraceFormat.hpp"

#include <fmt/format.h>

namespace chronon::observe {

namespace {

bool parseFlowId(std::string_view id, uint64_t& out) {
    int base = 10;
    size_t pos = 0;
    if (id.size() > 2 && id[0] == '0' && (id[1] == 'x' || id[1] == 'X')) {
        base = 16;
        pos = 2;
    }
    if (pos == id.size()) {
        return false;
    }

    uint64_t value = 0;
    for (; pos < id.size(); ++pos) {
        const unsigned char c = static_cast<unsigned char>(id[pos]);
        uint8_t digit = 0;
        if (c >= '0' && c <= '9') {
            digit = static_cast<uint8_t>(c - '0');
        } else if (base == 16 && c >= 'a' && c <= 'f') {
            digit = static_cast<uint8_t>(10 + c - 'a');
        } else if (base == 16 && c >= 'A' && c <= 'F') {
            digit = static_cast<uint8_t>(10 + c - 'A');
        } else {
            return false;
        }
        if (digit >= base) {
            return false;
        }
        value = value * static_cast<uint64_t>(base) + digit;
    }

    out = value;
    return true;
}

uint64_t mixColorKey(uint64_t value) {
    value += 0x9e3779b97f4a7c15ULL;
    value = (value ^ (value >> 30U)) * 0xbf58476d1ce4e5b9ULL;
    value = (value ^ (value >> 27U)) * 0x94d049bb133111ebULL;
    return value ^ (value >> 31U);
}

uint64_t hashColorKey(std::string_view key) {
    uint64_t parsed = 0;
    if (parseFlowId(key, parsed)) {
        return mixColorKey(parsed);
    }

    uint64_t hash = 1469598103934665603ULL;
    for (unsigned char ch : key) {
        hash ^= ch;
        hash *= 1099511628211ULL;
    }
    return mixColorKey(hash);
}

void appendInvisibleColorSuffix(std::string& out, uint64_t hash) {
    static constexpr std::string_view kInvisible[4] = {
        "\xE2\x80\x8B",  // U+200B ZERO WIDTH SPACE
        "\xE2\x80\x8C",  // U+200C ZERO WIDTH NON-JOINER
        "\xE2\x80\x8D",  // U+200D ZERO WIDTH JOINER
        "\xE2\x81\xA0",  // U+2060 WORD JOINER
    };
    for (int i = 0; i < 16; ++i) {
        out.append(kInvisible[hash & 0x3ULL]);
        hash >>= 2U;
    }
}

}  // namespace

uint64_t pipelineColorHash(uint64_t id) { return mixColorKey(id); }

uint64_t pipelineColorHash(std::string_view key) { return hashColorKey(key); }

std::string pipelineColorCategory(uint64_t color_hash) {
    return fmt::format("c{:016x}", color_hash);
}

std::string pipelineColoredEventName(std::string_view visible_name, uint64_t color_hash) {
    std::string name(visible_name);
    appendInvisibleColorSuffix(name, color_hash);
    return name;
}

}  // namespace chronon::observe
