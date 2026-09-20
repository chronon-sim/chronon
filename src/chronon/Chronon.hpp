// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

/// @file
/// Full Chronon umbrella, including compatibility names and modeling utilities.

#pragma once

// Full compatibility umbrella. Prefer a focused header in new model code.
#include "../sender/Sender.hpp"
#include "Application.hpp"
#include "Observation.hpp"
#include "Simulation.hpp"

namespace chronon {

// --- Tick Simulation ---

using Simulation = sender::TickSimulation;
using SimulationConfig = sender::TickSimulationConfig;

// --- Factory ---
using sender::factory::ISenderFactory;
using sender::factory::SenderFactoryRegistry;

using sender::config::SenderSimulationBuilder;

using FactoryRegistry = sender::factory::SenderFactoryRegistry;

// --- Port ---
using sender::DelayOneBroadcastFabric;
using sender::FlushRange;
using sender::PortBase;
using sender::PortBindingRegistry;
using sender::PortDirectory;
using sender::PortPolicy;
using sender::PortTransaction;
using sender::ReliablePortSender;
using sender::reserve;

// --- Params ---
using params::ParamBase;

// --- Util ---
using sender::Phase0;
using sender::Phase1;
using sender::ValidPhase;

using sender::ArbLoser;
using sender::ArbRequest;
using sender::ArbResult;
using sender::ArbWinner;
using sender::BankConflictPriority;
using sender::LoseReason;
using sender::PriorityArbiter;

using sender::SingleStageReg;
using sender::StagePipeline;
using sender::StageReg;

using sender::convertForward;
using sender::processForward;
using sender::simpleForward;
using sender::simpleForwardAll;

using sender::VersionedRegister;

}  // namespace chronon
