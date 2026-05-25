// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// CompressionBuffer.cpp

#include "CompressionBuffer.hpp"

#include <algorithm>
#include <cstring>

// Check if zstd is available
#if __has_include(<zstd.h>)
#define CHRONON_HAS_ZSTD 1
#include <zstd.h>
#else
#define CHRONON_HAS_ZSTD 0
#endif

namespace chronon::observe {

#if CHRONON_HAS_ZSTD

struct CompressionBuffer::Impl {
    ZSTD_CCtx* cctx = nullptr;
    ZSTD_DCtx* dctx = nullptr;
    std::vector<std::byte> output_buffer;

    Impl(size_t max_input_size) {
        cctx = ZSTD_createCCtx();
        dctx = ZSTD_createDCtx();
        // Pre-allocate output buffer for worst-case compression
        output_buffer.resize(ZSTD_compressBound(max_input_size));
    }

    ~Impl() {
        if (cctx) {
            ZSTD_freeCCtx(cctx);
        }
        if (dctx) {
            ZSTD_freeDCtx(dctx);
        }
    }
};

CompressionBuffer::CompressionBuffer(int level, size_t max_input_size)
    : level_(std::clamp(level, 1, 22)),
      available_(true),
      impl_(std::make_unique<Impl>(max_input_size)) {
    if (!impl_->cctx || !impl_->dctx) {
        available_ = false;
    }
}

CompressionBuffer::~CompressionBuffer() = default;

std::vector<std::byte> CompressionBuffer::compress(const std::byte* input, size_t input_size) {
    if (!available_ || input_size == 0) {
        // Fallback: return uncompressed copy
        last_ratio_ = 1.0;
        return std::vector<std::byte>(input, input + input_size);
    }

    // Ensure output buffer is large enough
    size_t max_compressed = ZSTD_compressBound(input_size);
    if (impl_->output_buffer.size() < max_compressed) {
        impl_->output_buffer.resize(max_compressed);
    }

    // Compress
    size_t compressed_size =
        ZSTD_compressCCtx(impl_->cctx, impl_->output_buffer.data(), impl_->output_buffer.size(),
                          input, input_size, level_);

    if (ZSTD_isError(compressed_size)) {
        // Compression failed - return uncompressed
        last_ratio_ = 1.0;
        return std::vector<std::byte>(input, input + input_size);
    }

    // Check if compression was beneficial
    last_ratio_ = static_cast<double>(compressed_size) / static_cast<double>(input_size);
    if (compressed_size >= input_size) {
        // Compression didn't help - return uncompressed
        last_ratio_ = 1.0;
        return std::vector<std::byte>(input, input + input_size);
    }

    // Return compressed data
    return std::vector<std::byte>(impl_->output_buffer.data(),
                                  impl_->output_buffer.data() + compressed_size);
}

std::vector<std::byte> CompressionBuffer::decompress(const std::byte* input, size_t input_size,
                                                     size_t output_size) {
    if (!available_ || input_size == 0) {
        // Assume data is uncompressed
        return std::vector<std::byte>(input, input + input_size);
    }

    std::vector<std::byte> output(output_size);

    size_t decompressed_size =
        ZSTD_decompressDCtx(impl_->dctx, output.data(), output.size(), input, input_size);

    if (ZSTD_isError(decompressed_size) || decompressed_size != output_size) {
        // Decompression failed - data might be uncompressed
        if (input_size == output_size) {
            return std::vector<std::byte>(input, input + input_size);
        }
        // Return empty on error
        return {};
    }

    return output;
}

#else  // !CHRONON_HAS_ZSTD

// Fallback implementation when zstd is not available

struct CompressionBuffer::Impl {};

CompressionBuffer::CompressionBuffer(int level, size_t /* max_input_size */)
    : level_(level), available_(false), impl_(nullptr) {}

CompressionBuffer::~CompressionBuffer() = default;

std::vector<std::byte> CompressionBuffer::compress(const std::byte* input, size_t input_size) {
    last_ratio_ = 1.0;
    return std::vector<std::byte>(input, input + input_size);
}

std::vector<std::byte> CompressionBuffer::decompress(const std::byte* input, size_t input_size,
                                                     size_t /* output_size */) {
    return std::vector<std::byte>(input, input + input_size);
}

#endif  // CHRONON_HAS_ZSTD

}  // namespace chronon::observe
