// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

/// @file
/// Forward declarations for the sender-based simulation framework.

#pragma once

#include <cstdint>

namespace chronon::sender {

class Unit;
class TickableUnit;
class TickSimulation;

enum class TerminationReason : uint8_t;
struct TerminationRequest;
class TerminationController;

class PortBase;
template <typename T>
class InPort;
template <typename T>
class OutPort;
template <typename T>
class Connection;
class ConnectionBase;

class DependencyGraph;
class CycleAnalyzer;

template <typename T>
class MessageQueue;
template <typename T>
class LockFreeMessageQueue;

}  // namespace chronon::sender
