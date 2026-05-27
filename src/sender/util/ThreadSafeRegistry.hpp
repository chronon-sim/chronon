// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace chronon {

template <typename V>
class ThreadSafeRegistry {
public:
    void insert(const std::string& key, std::unique_ptr<V> value) {
        std::lock_guard<std::mutex> lock(mutex_);
        entries_[key] = std::move(value);
    }

    V* find(const std::string& key) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = entries_.find(key);
        return it != entries_.end() ? it->second.get() : nullptr;
    }

    bool has(const std::string& key) const {
        std::lock_guard<std::mutex> lock(mutex_);
        return entries_.count(key) > 0;
    }

    size_t size() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return entries_.size();
    }

    std::vector<std::string> keys() const {
        std::lock_guard<std::mutex> lock(mutex_);
        std::vector<std::string> result;
        result.reserve(entries_.size());
        for (const auto& [k, _] : entries_) {
            result.push_back(k);
        }
        return result;
    }

    template <typename Func>
    void forEach(Func&& fn) const {
        std::lock_guard<std::mutex> lock(mutex_);
        for (const auto& [k, v] : entries_) {
            fn(k, *v);
        }
    }

    void clear() {
        std::lock_guard<std::mutex> lock(mutex_);
        entries_.clear();
    }

protected:
    ThreadSafeRegistry() = default;
    ~ThreadSafeRegistry() = default;
    ThreadSafeRegistry(const ThreadSafeRegistry&) = delete;
    ThreadSafeRegistry& operator=(const ThreadSafeRegistry&) = delete;

    mutable std::mutex mutex_;
    std::unordered_map<std::string, std::unique_ptr<V>> entries_;
};

}  // namespace chronon
