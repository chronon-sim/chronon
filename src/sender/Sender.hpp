// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

/// @file Umbrella header for the chronon::sender API.

#pragma once

#include "app/SimulationApp.hpp"
#include "config/SenderConfigLoader.hpp"
#include "config/SenderSimulationBuilder.hpp"
#include "config/SenderUnitConfig.hpp"
#include "config/YAMLOverride.hpp"
#include "core/CrashHandler.hpp"
#include "core/PhasedTickableUnit.hpp"
#include "core/TerminationRequest.hpp"
#include "core/TickSimulation.hpp"
#include "core/TickSimulationConfig.hpp"
#include "core/TickableUnit.hpp"
#include "core/Unit.hpp"
#include "factory/SenderFactory.hpp"
#include "port/Connection.hpp"
#include "port/DelayOneBroadcastFabric.hpp"
#include "port/InPort.hpp"
#include "port/OutPort.hpp"
#include "port/Port.hpp"
#include "port/PortDirectory.hpp"
#include "port/PortTransaction.hpp"
#include "schedule/DependencyGraph.hpp"
#include "schedule/SchedulerTimelineTrace.hpp"
#include "schedule/SimulatedAnnealingPartitioner.hpp"
#include "schedule/WeightedPartitioner.hpp"
#include "util/Graph.hpp"
#include "util/PipelinePhase.hpp"
#include "util/PriorityArbiter.hpp"
#include "util/StageForward.hpp"
#include "util/StagePipeline.hpp"
#include "util/StageReg.hpp"
#include "util/VersionedRegister.hpp"
