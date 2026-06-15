// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#include "PipelineTraceFormat.hpp"

#include <fmt/format.h>

#include <cctype>
#include <limits>

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

std::string colorCategory(std::string_view key) {
    return fmt::format("c{:016x}", hashColorKey(key));
}

void appendInvisibleColorSuffix(std::string& out, std::string_view key) {
    uint64_t hash = hashColorKey(key);
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

std::string coloredEventName(std::string_view visible_name, std::string_view color_key) {
    std::string name(visible_name);
    appendInvisibleColorSuffix(name, color_key);
    return name;
}

std::string_view findNoteValue(std::string_view note, std::string_view key) {
    const size_t start = note.find(key);
    if (start == std::string_view::npos) {
        return {};
    }

    const size_t value_start = start + key.size();
    size_t value_end = value_start;
    while (value_end < note.size() && !std::isspace(static_cast<unsigned char>(note[value_end]))) {
        ++value_end;
    }
    return note.substr(value_start, value_end - value_start);
}

std::string_view pipelineColorKey(std::string_view source_name, std::string_view id,
                                  std::string_view note) {
    if (source_name.find(".lsu") != std::string_view::npos) {
        if (auto pa = findNoteValue(note, "pa="); !pa.empty()) {
            return pa;
        }
        if (auto va = findNoteValue(note, "va="); !va.empty()) {
            return va;
        }
        if (auto pc = findNoteValue(note, "pc="); !pc.empty()) {
            return pc;
        }
    }
    return id;
}

uint32_t stageGroupOrder(std::string_view stage_name) {
    struct StageOrder {
        std::string_view name;
        uint32_t order;
    };
    static constexpr StageOrder kStageOrders[] = {
        {"BP", 0},    {"IC", 100},  {"IF", 200},  {"DEC", 300}, {"REN", 400},
        {"DIS", 500}, {"IQE", 600}, {"ISS", 700}, {"EXE", 800}, {"CMP", 900},
        {"LI", 1000}, {"SI", 1100}, {"SA", 1200}, {"SR", 1300}, {"RET", 1400},
    };

    for (const auto& entry : kStageOrders) {
        if (stage_name == entry.name) {
            return entry.order;
        }
    }

    uint32_t fallback = 2000;
    for (unsigned char ch : stage_name) {
        fallback = fallback * 33U + ch;
    }
    return fallback;
}

uint32_t parseStageOrder(std::string_view stage, char pipe_digit) {
    std::string_view stage_name = stage;
    uint32_t stage_num = 0;
    uint32_t multiplier = 1;
    while (!stage_name.empty() && std::isdigit(static_cast<unsigned char>(stage_name.back()))) {
        stage_num += static_cast<uint32_t>(stage_name.back() - '0') * multiplier;
        multiplier *= 10U;
        stage_name.remove_suffix(1);
    }

    uint32_t pipe = 0;
    if (pipe_digit >= '0' && pipe_digit <= '9') {
        pipe = static_cast<uint32_t>(pipe_digit - '0');
    }

    uint64_t order = static_cast<uint64_t>(stageGroupOrder(stage_name)) + stage_num * 10ULL + pipe;
    if (order > std::numeric_limits<uint32_t>::max()) {
        return std::numeric_limits<uint32_t>::max();
    }
    return static_cast<uint32_t>(order);
}

}  // namespace

bool isPipeCategory(uint32_t category) {
    for (const auto& entry : CategoryRegistry::instance().entries()) {
        if ((category & entry.mask) != 0 && entry.name == "pipe") {
            return true;
        }
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
    char pipe_digit = '\0';
    const unsigned char first = static_cast<unsigned char>(header.front());
    if (std::isdigit(first)) {
        pipe_digit = static_cast<char>(first);
        stage_start = 1;
    } else if (header.front() == '-') {
        stage_start = 1;
    }
    const std::string_view stage = header.substr(stage_start);
    if (stage.empty()) {
        return false;
    }
    out.stage_order = parseStageOrder(stage, pipe_digit);

    out.track_path.clear();
    out.track_path.reserve(source_name.size() + stage.size() + 8);
    std::string_view source = source_name.empty() ? std::string_view("unknown") : source_name;
    for (char ch : source) {
        out.track_path.push_back(ch == '.' ? '/' : ch);
    }
    out.track_path.push_back(' ');
    out.track_path.append(stage);
    if (pipe_digit != '\0') {
        out.track_path.append(" pipe");
        out.track_path.push_back(pipe_digit);
    }

    const std::string_view color_key = pipelineColorKey(source_name, out.id, out.note);
    out.category = colorCategory(color_key);
    out.event_name = coloredEventName(out.id, color_key);

    out.flow_id = 0;
    (void)parseFlowId(out.id, out.flow_id);
    return true;
}

}  // namespace chronon::observe
