// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace chronon::observe {

class SourceNameRegistry {
public:
    uint16_t registerName(const std::string& name) {
        uint16_t id = static_cast<uint16_t>(names_.size() + 1);
        names_.push_back(name);
        return id;
    }

    std::string_view getName(uint16_t source_id) const noexcept {
        if (source_id > 0 && source_id <= names_.size()) {
            return names_[source_id - 1];
        }
        return "";
    }

    void freeze() noexcept { frozen_ = true; }
    void unfreeze() noexcept { frozen_ = false; }
    bool isFrozen() const noexcept { return frozen_; }

    void clear() {
        names_.clear();
        frozen_ = false;
    }

private:
    std::vector<std::string> names_;
    bool frozen_ = false;
};

}  // namespace chronon::observe
