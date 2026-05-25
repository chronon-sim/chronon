// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#pragma once

/// @file Macros for the dual-constructor pattern used by YAML-instantiable units.

namespace chronon::params {

/**
 * @brief Generate the factory-side constructor that delegates to a direct constructor.
 *
 * The factory system requires `UnitT(const ParameterSet*)`. This macro produces it
 * by forwarding extracted parameter values to a direct constructor that is written
 * right after the macro invocation — keeping initialization logic in one place.
 *
 * @param UnitClass     Unit class name (e.g. `FetchUnit`).
 * @param ParamSetType  ParameterSet type (must match the `using ParameterSet = ...`).
 * @param ...           Expressions extracting fields from `params` (e.g.
 *                      `params->max_instructions, params->icache_lines`).
 *
 * @code
 * class FetchUnit : public AutoRegisteredUnit<FetchUnit> {
 * public:
 *     using ParameterSet = FetchParams;
 *
 *     CHRONON_UNIT_CONSTRUCTOR(FetchUnit, ParameterSet,
 *         params->max_instructions, params->icache_lines)
 *     (uint64_t max_instr = 1000000, uint32_t icache_lines = 50)
 *         : AutoRegisteredUnit("fetch"), max_instructions_(max_instr) {
 *         for (uint64_t line = 0; line < icache_lines; ++line) {
 *             icache_lines_.insert(line);
 *         }
 *     }
 * };
 * @endcode
 *
 * Use an empty extraction list for units with no parameters:
 * @code
 * CHRONON_UNIT_CONSTRUCTOR(DecodeUnit, ParameterSet, )
 * ()
 *     : AutoRegisteredUnit("decode") { ... }
 * @endcode
 *
 * @note The extraction expressions must match the direct constructor's parameter
 *       types in order. Implemented via constructor delegation (C++11).
 */
#define CHRONON_UNIT_CONSTRUCTOR(UnitClass, ParamSetType, ...)                                  \
    explicit UnitClass([[maybe_unused]] const ParamSetType* params) : UnitClass(__VA_ARGS__) {} \
    explicit UnitClass

/// Semantic alias for CHRONON_UNIT_CONSTRUCTOR.
#define CHRONON_DUAL_CONSTRUCTOR(UnitClass, ParamSetType, ...) \
    CHRONON_UNIT_CONSTRUCTOR(UnitClass, ParamSetType, __VA_ARGS__)

}  // namespace chronon::params
