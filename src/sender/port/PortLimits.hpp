// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// SPDX-License-Identifier: MPL-2.0
#pragma once

#include <cstddef>

namespace chronon::sender {
/// Model-visible send limit per owning unit cycle. Zero retains legacy unlimited semantics.
struct SendRate {
    size_t entries_per_cycle;
};
/// Model-visible destination FIFO depth; independent of physical adapter storage.
struct QueueDepth {
    size_t entries;
};
}  // namespace chronon::sender
