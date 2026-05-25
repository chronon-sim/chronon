// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// CompressionBuffer.hpp
//
// Helper class for block-level zstd compression.
// Provides optional compression with graceful fallback when zstd is unavailable.

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

namespace chronon::observe {

/**
 * CompressionBuffer - Block-level compression using zstd.
 *
 * Compresses data blocks for efficient trace storage.
 * Falls back to uncompressed output if zstd is unavailable.
 */
class CompressionBuffer {
public:
    /**
     * Construct a compression buffer.
     *
     * @param level Compression level (1-22, 3 = fast default)
     * @param max_input_size Maximum expected input size for buffer pre-allocation
     */
    explicit CompressionBuffer(int level = 3, size_t max_input_size = 64 * 1024);

    ~CompressionBuffer();

    // Non-copyable, non-movable
    CompressionBuffer(const CompressionBuffer&) = delete;
    CompressionBuffer& operator=(const CompressionBuffer&) = delete;

    /**
     * Check if compression is available.
     *
     * @return true if zstd is available and working
     */
    [[nodiscard]] bool isAvailable() const noexcept { return available_; }

    /**
     * Compress data.
     *
     * @param input Input data to compress
     * @param input_size Size of input data
     * @return Compressed data (or copy of input if compression unavailable/ineffective)
     */
    std::vector<std::byte> compress(const std::byte* input, size_t input_size);

    /**
     * Decompress data.
     *
     * @param input Compressed data
     * @param input_size Size of compressed data
     * @param output_size Expected decompressed size
     * @return Decompressed data
     */
    std::vector<std::byte> decompress(const std::byte* input, size_t input_size,
                                      size_t output_size);

    /**
     * Get compression level.
     */
    [[nodiscard]] int level() const noexcept { return level_; }

    /**
     * Get last compression ratio (compressed/uncompressed).
     */
    [[nodiscard]] double lastRatio() const noexcept { return last_ratio_; }

private:
    int level_;
    bool available_;
    double last_ratio_ = 1.0;

    // Internal compression context (opaque pointer for zstd)
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace chronon::observe
