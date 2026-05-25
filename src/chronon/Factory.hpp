// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

/// @file Re-exports for the unified chronon:: namespace.

#pragma once

#include "../sender/config/SenderSimulationBuilder.hpp"
#include "../sender/core/PhasedTickableUnit.hpp"
#include "../sender/factory/SenderFactory.hpp"

namespace chronon {

using sender::PhasedTickableUnit;
using sender::factory::AutoRegisteredUnit;
using sender::factory::ISenderFactory;
using sender::factory::PhasedAutoRegisteredUnit;
using sender::factory::SenderFactoryRegistry;

using sender::config::SenderSimulationBuilder;

using SimulationBuilder = sender::config::SenderSimulationBuilder;
using FactoryRegistry = sender::factory::SenderFactoryRegistry;

}  // namespace chronon
