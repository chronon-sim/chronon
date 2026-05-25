// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

/// @file Re-exports TickSimulation as the chronon:: Simulation type.

#pragma once

#include "../sender/core/TickSimulation.hpp"

namespace chronon {

using sender::TickSimulation;
using sender::TickSimulationConfig;

using Simulation = sender::TickSimulation;
using SimulationConfig = sender::TickSimulationConfig;

}  // namespace chronon
