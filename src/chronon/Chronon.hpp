// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

/// @file Umbrella header exposing the full Chronon API in the chronon:: namespace.

#pragma once

// --- Subsystem includes ---
#include "../observe/Observe.hpp"
#include "../params/Param.hpp"
#include "../params/ParameterSet.hpp"
#include "../params/UnitConstructorMacros.hpp"
#include "../params/YAMLSerialization.hpp"
#include "../sender/Sender.hpp"
#include "../sender/app/SimulationApp.hpp"
#include "../sender/config/SenderSimulationBuilder.hpp"
#include "../sender/core/PhasedTickableUnit.hpp"
#include "../sender/core/TerminationRequest.hpp"
#include "../sender/core/TickSimulation.hpp"
#include "../sender/core/TickableUnit.hpp"
#include "../sender/core/Unit.hpp"
#include "../sender/factory/SenderFactory.hpp"
#include "../sender/port/Connection.hpp"
#include "../sender/port/Port.hpp"
#include "../sender/port/PortDirectory.hpp"
#include "../sender/util/PipelinePhase.hpp"
#include "../sender/util/PriorityArbiter.hpp"
#include "../sender/util/StageForward.hpp"
#include "../sender/util/StagePipeline.hpp"
#include "../sender/util/StageReg.hpp"
#include "../sender/util/VersionedRegister.hpp"
#include "../tree/TreeNode.hpp"

namespace chronon {

// --- Tree ---
using tree::TreeNode;

// --- Unit ---
using sender::Unit;
using sender::UnitState;

// --- Tick Simulation ---
using sender::TerminationController;
using sender::TerminationReason;
using sender::TerminationRequest;
using sender::TickableUnit;
using sender::TickSimulation;
using sender::TickSimulationConfig;

using Simulation = sender::TickSimulation;
using SimulationConfig = sender::TickSimulationConfig;

// --- Factory ---
using sender::PhasedTickableUnit;
using sender::factory::AutoRegisteredUnit;
using sender::factory::ISenderFactory;
using sender::factory::PhasedAutoRegisteredUnit;
using sender::factory::SenderFactoryRegistry;

using sender::config::SenderSimulationBuilder;

using SimulationBuilder = sender::config::SenderSimulationBuilder;
using FactoryRegistry = sender::factory::SenderFactoryRegistry;

// --- Port ---
using sender::Connection;
using sender::InPort;
using sender::OutPort;
using sender::PortBase;
using sender::PortBindingRegistry;
using sender::PortDirectory;
using sender::PortPolicy;

// --- Params ---
using params::Param;
using params::ParamBase;
using params::ParameterSet;

// --- Observe ---
using observe::Category;
using observe::CategoryRegistry;
using observe::ComputeFn;
using observe::Counter;
using observe::CounterId;
using observe::DerivedCounter;
using observe::DerivedCounterDef;
using observe::LogLevel;
using observe::ObservableUnit;
using observe::ObservationChannel;
using observe::ObservationChannelStats;
using observe::ObservationContext;
using observe::ObservationStats;
using observe::toIndex;
namespace DerivedFormula = observe::DerivedFormula;

// Timeline lanes: spans/instants with flows and typed args, counter samples.
using observe::arg;
using observe::EventNameRef;
using observe::Flow;
using observe::flow;
using observe::pipe;
using observe::PipelinePipe;
using observe::pipeStage;
using observe::pipeStageHex;
using observe::TimelineCounter;
using observe::TimelineLane;
using observe::operator""_ev;

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
