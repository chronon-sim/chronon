// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#include "PipelineTraceFormat.hpp"

#include <fmt/format.h>

#include <bit>
#include <cctype>

#include "ObserveApi.hpp"

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

std::string_view pipelineColorKey(std::string_view /*source_name*/, std::string_view id,
                                  std::string_view /*note*/) {
    return id;
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

bool isPipeCategory(CategoryMask category) {
    CategoryMask remaining = category;
    while (remaining != 0) {
        const uint32_t bit = static_cast<uint32_t>(std::countr_zero(remaining));
        if (CategoryRegistry::instance().nameForBit(bit) == "pipe") {
            return true;
        }
        remaining &= remaining - 1;
    }
    return false;
}

bool parsePipelineTraceMessage(std::string_view source_name, std::string_view message,
                               PipelineTraceFields& out) {
    const size_t hash = message.find('#');
    if (hash == std::string_view::npos || hash == 0) {
        return false;
    }

    const size_t semi = message.find(';', hash + 1);
    const std::string_view header = message.substr(0, hash);
    out.id = message.substr(
        hash + 1, semi == std::string_view::npos ? std::string_view::npos : semi - hash - 1);
    out.note = semi == std::string_view::npos ? std::string_view{} : message.substr(semi + 1);
    if (out.id.empty()) {
        return false;
    }

    size_t stage_start = 0;
    std::string_view pipe_id;
    const unsigned char first = static_cast<unsigned char>(header.front());
    if (std::isdigit(first)) {
        while (stage_start < header.size() &&
               std::isdigit(static_cast<unsigned char>(header[stage_start]))) {
            ++stage_start;
        }
        pipe_id = header.substr(0, stage_start);
    } else if (header.front() == '-') {
        stage_start = 1;
    }
    const std::string_view stage = header.substr(stage_start);
    if (stage.empty()) {
        return false;
    }
    out.track_path.clear();
    std::string_view source = source_name.empty() ? std::string_view("unknown") : source_name;
    out.track_path.reserve(source.size() + stage.size() + pipe_id.size() + 8);
    out.track_path.append(source);
    out.track_path.push_back('.');
    out.track_path.append(stage);
    if (!pipe_id.empty()) {
        out.track_path.append(" pipe");
        out.track_path.append(pipe_id);
    }

    const std::string_view color_key = pipelineColorKey(source_name, out.id, out.note);
    const uint64_t color_hash = pipelineColorHash(color_key);
    out.category = pipelineColorCategory(color_hash);
    out.event_name = pipelineColoredEventName(out.id, color_hash);

    out.flow_id = 0;
    (void)parseFlowId(out.id, out.flow_id);
    return true;
}

}  // namespace chronon::observe
