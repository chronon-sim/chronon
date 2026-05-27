// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#include "CounterRegistry.hpp"

#include <cstring>
#include <thread>

#include "ObservationContext.hpp"
#include "ObservationQueue.hpp"
#include "ThreadContextManager.hpp"

namespace chronon::observe {

void CounterRegistry::reregisterAll(
    const std::vector<std::unique_ptr<ObservationContext>>& contexts) {
    counters_.clear();
    derived_defs_.clear();
    for (const auto& ctx : contexts) {
        if (ctx) {
            ctx->registerAllCounters(this);
            for (const auto& def : ctx->derivedCounterDefs()) {
                derived_defs_.push_back(def);
            }
        }
    }
}

void CounterRegistry::dumpSnapshots(
    uint64_t cycle, ObservationQueue* queue,
    const std::vector<std::unique_ptr<ObservationContext>>& contexts) {
    if (!queue) {
        return;
    }

    struct SnapshotEntry {
        const std::string* unit_name;
        std::string counter_name;
        uint64_t value;
        size_t aligned_size;
    };

    auto alignedRecordSize = [](size_t unit_len, size_t ctr_len) -> size_t {
        size_t data_size = 8 + 2 + unit_len + 2 + ctr_len + 8;
        size_t record_size = sizeof(ObservationQueue::RecordHeader) + data_size;
        return (record_size + 7) & ~7;
    };

    std::vector<SnapshotEntry> entries;
    entries.reserve(counters_.size());
    size_t total_size = 0;

    for (const auto& [key, counter_ptr] : counters_) {
        if (key.counter_name.empty()) {
            continue;
        }
        size_t aligned = alignedRecordSize(key.unit_name.length(), key.counter_name.length());
        entries.push_back({&key.unit_name, key.counter_name, counter_ptr->get(), aligned});
        total_size += aligned;
    }

    constexpr size_t num_ch = static_cast<size_t>(ObservationChannel::NumChannels);
    for (const auto& ctx : contexts) {
        if (!ctx) {
            continue;
        }
        const auto& stats = ctx->observationStats();
        if (stats.totalEmitted() == 0 && stats.totalDropped() == 0) {
            continue;
        }
        const std::string& unit_name = ctx->unitName();
        for (size_t i = 0; i < num_ch; ++i) {
            auto ch = static_cast<ObservationChannel>(i);
            const auto& ch_stats = stats.get(ch);
            const char* ch_name = ObservationStats::channelName(ch);

            if (ch_stats.emitted > 0) {
                std::string ctr_name = std::string("obs_") + ch_name + "_emitted";
                size_t aligned = alignedRecordSize(unit_name.length(), ctr_name.length());
                entries.push_back({&unit_name, std::move(ctr_name), ch_stats.emitted, aligned});
                total_size += aligned;
            }
            if (ch_stats.dropped > 0) {
                std::string ctr_name = std::string("obs_") + ch_name + "_dropped";
                size_t aligned = alignedRecordSize(unit_name.length(), ctr_name.length());
                entries.push_back({&unit_name, std::move(ctr_name), ch_stats.dropped, aligned});
                total_size += aligned;
            }
        }
    }

    if (entries.empty()) {
        return;
    }

    auto* ptr = queue->prepareWrite(total_size);
    if (ptr == nullptr) {
        if (total_size > queue->capacity()) {
            queue->incrementDropped();
            return;
        }
        constexpr uint32_t max_spins = 4096;
        uint32_t spins = 0;
        do {
            if (++spins > 64) {
                std::this_thread::yield();
            } else {
#if defined(__x86_64__) || defined(_M_X64)
                __builtin_ia32_pause();
#elif defined(__aarch64__)
                asm volatile("yield" ::: "memory");
#else
                std::this_thread::yield();
#endif
            }
            if ((spins & 0xFFu) == 0u) {
                (void)ThreadContextManager::instance().wakeBackend();
            }
            ptr = queue->prepareWrite(total_size);
        } while (ptr == nullptr && spins < max_spins);

        if (ptr == nullptr) {
            queue->incrementDropped();
            return;
        }
    }

    std::byte* write_pos = ptr;
    for (const auto& entry : entries) {
        auto* header = reinterpret_cast<ObservationQueue::RecordHeader*>(write_pos);
        header->total_size = static_cast<uint16_t>(entry.aligned_size);
        header->type = ObservationQueue::EventType::COUNTER_SNAPSHOT;
        header->flags = 0;
        header->padding = 0;

        std::byte* data = write_pos + sizeof(ObservationQueue::RecordHeader);
        size_t offset = 0;

        std::memcpy(data + offset, &cycle, 8);
        offset += 8;

        uint16_t unit_len = static_cast<uint16_t>(entry.unit_name->length());
        std::memcpy(data + offset, &unit_len, 2);
        offset += 2;
        std::memcpy(data + offset, entry.unit_name->data(), unit_len);
        offset += unit_len;

        uint16_t ctr_len = static_cast<uint16_t>(entry.counter_name.length());
        std::memcpy(data + offset, &ctr_len, 2);
        offset += 2;
        std::memcpy(data + offset, entry.counter_name.data(), ctr_len);
        offset += ctr_len;

        std::memcpy(data + offset, &entry.value, 8);

        write_pos += entry.aligned_size;
    }

    queue->finishAndCommitWrite(total_size);

    for (const auto& [key, counter_ptr] : counters_) {
        counter_ptr->reset();
    }
}

}  // namespace chronon::observe
