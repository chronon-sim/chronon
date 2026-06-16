// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Observe.hpp
//
// Umbrella include for the Chronon observation system.

#pragma once

// Core types
#include "Types.hpp"

// Counter system
#include "Counter.hpp"

// High-performance per-thread queues
#include "SPSCQueue.hpp"
#include "ThreadContext.hpp"
#include "ThreadContextManager.hpp"

// Observation context
#include "ObservationContext.hpp"

// Filtering
#include "ObservationFilter.hpp"

// Modern macro-free API
#include "ObserveApi.hpp"
#include "PipelineApi.hpp"

// Unit mixin
#include "ObservableUnit.hpp"

// Per-instance counters
#include "LocalCounter.hpp"

// Derived (computed) counters
#include "DerivedCounter.hpp"

// Timeline lanes and push-model counter tracks
#include "TimelineApi.hpp"
#include "TimelineTrack.hpp"

/// @file Observe.hpp
/// @brief Umbrella header for the Chronon observation system.
