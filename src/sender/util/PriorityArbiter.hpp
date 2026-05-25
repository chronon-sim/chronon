// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#pragma once

#include <array>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <initializer_list>

namespace chronon::sender {

/** @brief A single arbitration request for one source on one pipe. */
template <typename SourceEnum>
struct ArbRequest {
    SourceEnum source{};
    uint8_t entry_id = 0xFF;  ///< Buffer-specific ID (LID, STID, etc.).
    uint64_t addr = 0;        ///< For bank conflict detection.
    bool valid = false;
    uint64_t tag = 0;  ///< Opaque attribute carried through for observe.
};

/** @brief Winning request on a pipe; bank_conflict marks a priority win blocked by a conflict. */
template <typename SourceEnum>
struct ArbWinner {
    SourceEnum source{};
    uint8_t entry_id = 0xFF;
    uint8_t pipe_id = 0;
    uint64_t tag = 0;
    bool valid = false;
    bool bank_conflict = false;
};

enum class LoseReason : uint8_t { LOWER_PRIORITY, BANK_CONFLICT, OVERFLOW_FAILED };

enum class BankConflictPriority : uint8_t {
    LowerPipeIndex,
    HigherPipeIndex,
};

/** @brief A losing request with the reason it was rejected. */
template <typename SourceEnum>
struct ArbLoser {
    SourceEnum source{};
    uint8_t entry_id = 0xFF;
    uint8_t target_pipe = 0;
    uint64_t tag = 0;
    LoseReason reason{};
};

/** @brief Per-tick arbitration result: winners by pipe, losers, and request counts. */
template <typename SourceEnum, std::size_t NumPipes, std::size_t MaxLosers = 16>
struct ArbResult {
    std::array<ArbWinner<SourceEnum>, NumPipes> winners{};
    std::array<ArbLoser<SourceEnum>, MaxLosers> losers{};
    uint8_t loser_count = 0;
    std::array<uint8_t, NumPipes> contention{};  ///< Number of requests per pipe.

    const ArbWinner<SourceEnum>& operator[](std::size_t pipe) const {
        assert(pipe < NumPipes);
        return winners[pipe];
    }

    void clear() noexcept {
        for (auto& w : winners) {
            w = ArbWinner<SourceEnum>{};
        }
        for (auto& l : losers) {
            l = ArbLoser<SourceEnum>{};
        }
        loser_count = 0;
        contention.fill(0);
    }
};

/**
 * @brief Declarative N-pipe priority arbiter with overflow, bank-conflict, and idle-fill.
 *
 * Replaces hand-unrolled priority chains with a configured data structure. Configuration
 * (setPipePriority / setOverflow / setBankConflictFn / addPassRule / addDynamicOverride /
 * addIdleFillSource) is done once; per-tick callers use clearRequests / addRequest /
 * arbitrate.
 */
template <typename SourceEnum, std::size_t NumPipes, std::size_t MaxSourcesPerPipe = 8,
          std::size_t MaxLosers = 16, std::size_t MaxOverflows = 8, std::size_t MaxPassRules = 4,
          std::size_t MaxDynOverrides = 4, std::size_t MaxIdleFillSources = 4,
          std::size_t MaxIdleFillRequests = 8>
class PriorityArbiter {
public:
    using Request = ArbRequest<SourceEnum>;
    using Result = ArbResult<SourceEnum, NumPipes, MaxLosers>;

    /// First source in @p priority is highest.
    void setPipePriority(std::size_t pipe, std::initializer_list<SourceEnum> priority) {
        assert(pipe < NumPipes);
        assert(priority.size() <= MaxSourcesPerPipe);
        auto& chain = priority_chains_[pipe];
        chain.count = 0;
        for (auto src : priority) {
            chain.sources[chain.count++] = src;
        }
    }

    /// Overflow: if `source` loses on `from_pipe`, try `to_pipe`.
    void setOverflow(SourceEnum source, std::size_t from_pipe, std::size_t to_pipe) {
        assert(overflow_count_ < MaxOverflows);
        overflows_[overflow_count_++] = {source, static_cast<uint8_t>(from_pipe),
                                         static_cast<uint8_t>(to_pipe)};
    }

    /// Bank conflict predicate: returns true if two addresses conflict.
    void setBankConflictFn(bool (*fn)(uint64_t, uint64_t)) { bank_conflict_fn_ = fn; }

    /// Select which pipe wins when two accepted winners conflict on bank.
    void setBankConflictPriority(BankConflictPriority priority) noexcept {
        bank_conflict_priority_ = priority;
    }

    /// Pass rule: `lower` may proceed despite conflict with `higher`.
    void addPassRule(SourceEnum higher, SourceEnum lower) {
        assert(pass_rule_count_ < MaxPassRules);
        pass_rules_[pass_rule_count_++] = {higher, lower};
    }

    /// Dynamic override: when `*cond` is true, `promoted` beats `demoted`.
    void addDynamicOverride(SourceEnum promoted, SourceEnum demoted, const bool* cond) {
        assert(dyn_override_count_ < MaxDynOverrides);
        dyn_overrides_[dyn_override_count_++] = {promoted, demoted, cond};
    }

    /// Register a source that can idle-fill empty pipes after arbitration.
    void addIdleFillSource(SourceEnum source) {
        assert(idle_fill_source_count_ < MaxIdleFillSources);
        idle_fill_sources_[idle_fill_source_count_++] = source;
    }

    void clearRequests() noexcept {
        for (auto& pipe_reqs : requests_) {
            pipe_reqs.count = 0;
        }
        idle_fill_request_count_ = 0;
        for (auto& occ : occupied_) {
            occ = OccupiedInfo{};
        }
        result_.clear();
    }

    /// Pre-occupy a pipe; no winner is assigned but @p addr still participates in conflict checks.
    void setOccupied(std::size_t pipe, uint64_t addr, SourceEnum source) {
        assert(pipe < NumPipes);
        occupied_[pipe] = {true, addr, true, source};
    }

    /// Submit request for a specific pipe.
    void addRequest(std::size_t pipe, Request req) {
        assert(pipe < NumPipes);
        auto& pipe_reqs = requests_[pipe];
        if (pipe_reqs.count < MaxSourcesPerPipe) {
            pipe_reqs.entries[pipe_reqs.count++] = req;
        }
    }

    /// Submit idle-fill request (any empty pipe after main arbitration).
    void addIdleFillRequest(Request req) {
        if (idle_fill_request_count_ < MaxIdleFillRequests) {
            idle_fill_requests_[idle_fill_request_count_++] = req;
        }
    }

    /// Returns a const ref valid until the next clearRequests().
    const Result& arbitrate() {
        result_.clear();

        PriorityChain effective[NumPipes];
        for (std::size_t p = 0; p < NumPipes; ++p) {
            effective[p] = priority_chains_[p];
        }
        applyDynamicOverrides_(effective);

        for (std::size_t pipe = 0; pipe < NumPipes; ++pipe) {
            if (occupied_[pipe].present) {
                result_.contention[pipe] = requests_[pipe].count;
                for (std::size_t r = 0; r < requests_[pipe].count; ++r) {
                    auto& req = requests_[pipe].entries[r];
                    if (req.valid) {
                        addLoser_(req, pipe, LoseReason::LOWER_PRIORITY);
                        tryOverflow_(req, pipe);
                    }
                }
                continue;
            }

            result_.contention[pipe] = requests_[pipe].count;
            selectWinner_(pipe, effective[pipe]);
        }

        resolveBankConflicts_();
        fillIdlePipes_();

        return result_;
    }

    const Result& result() const noexcept { return result_; }

private:
    struct PriorityChain {
        std::array<SourceEnum, MaxSourcesPerPipe> sources{};
        std::size_t count = 0;
    };

    struct OverflowRule {
        SourceEnum source{};
        uint8_t from_pipe = 0;
        uint8_t to_pipe = 0;
    };

    struct PassRule {
        SourceEnum higher{};
        SourceEnum lower{};
    };

    struct DynOverride {
        SourceEnum promoted{};
        SourceEnum demoted{};
        const bool* cond = nullptr;
    };

    struct RequestList {
        std::array<Request, MaxSourcesPerPipe> entries{};
        std::size_t count = 0;
    };

    struct OccupiedInfo {
        bool present = false;
        uint64_t addr = 0;
        bool has_addr = false;
        SourceEnum source{};
    };

    std::array<PriorityChain, NumPipes> priority_chains_{};

    std::array<OverflowRule, MaxOverflows> overflows_{};
    std::size_t overflow_count_ = 0;

    bool (*bank_conflict_fn_)(uint64_t, uint64_t) = nullptr;
    BankConflictPriority bank_conflict_priority_ = BankConflictPriority::LowerPipeIndex;

    std::array<PassRule, MaxPassRules> pass_rules_{};
    std::size_t pass_rule_count_ = 0;

    std::array<DynOverride, MaxDynOverrides> dyn_overrides_{};
    std::size_t dyn_override_count_ = 0;

    std::array<SourceEnum, MaxIdleFillSources> idle_fill_sources_{};
    std::size_t idle_fill_source_count_ = 0;

    std::array<RequestList, NumPipes> requests_{};
    std::array<OccupiedInfo, NumPipes> occupied_{};

    std::array<Request, MaxIdleFillRequests> idle_fill_requests_{};
    std::size_t idle_fill_request_count_ = 0;

    Result result_{};

    void applyDynamicOverrides_(PriorityChain (&chains)[NumPipes]) {
        for (std::size_t oi = 0; oi < dyn_override_count_; ++oi) {
            auto& ovr = dyn_overrides_[oi];
            if (!ovr.cond || !*ovr.cond) continue;

            for (std::size_t p = 0; p < NumPipes; ++p) {
                auto& chain = chains[p];
                std::size_t promoted_idx = chain.count;
                std::size_t demoted_idx = chain.count;

                for (std::size_t i = 0; i < chain.count; ++i) {
                    if (chain.sources[i] == ovr.promoted) promoted_idx = i;
                    if (chain.sources[i] == ovr.demoted) demoted_idx = i;
                }

                if (promoted_idx < chain.count && demoted_idx < chain.count &&
                    promoted_idx > demoted_idx) {
                    std::swap(chain.sources[promoted_idx], chain.sources[demoted_idx]);
                }
            }
        }
    }

    void selectWinner_(std::size_t pipe, const PriorityChain& chain) {
        bool found_winner = false;

        for (std::size_t pri = 0; pri < chain.count; ++pri) {
            SourceEnum src = chain.sources[pri];

            Request* match = nullptr;
            for (std::size_t r = 0; r < requests_[pipe].count; ++r) {
                auto& req = requests_[pipe].entries[r];
                if (req.valid && req.source == src) {
                    match = &req;
                    break;
                }
            }

            if (match == nullptr) continue;

            if (!found_winner) {
                auto& winner = result_.winners[pipe];
                winner.source = match->source;
                winner.entry_id = match->entry_id;
                winner.pipe_id = static_cast<uint8_t>(pipe);
                winner.tag = match->tag;
                winner.valid = true;
                winner_addrs_[pipe] = match->addr;
                match->valid = false;
                found_winner = true;
            } else {
                addLoser_(*match, pipe, LoseReason::LOWER_PRIORITY);
                tryOverflow_(*match, pipe);
                match->valid = false;
            }
        }

        // Requests with sources not in the priority chain are losers.
        for (std::size_t r = 0; r < requests_[pipe].count; ++r) {
            auto& req = requests_[pipe].entries[r];
            if (req.valid) {
                addLoser_(req, pipe, LoseReason::LOWER_PRIORITY);
                tryOverflow_(req, pipe);
                req.valid = false;
            }
        }
    }

    void addLoser_(const Request& req, std::size_t pipe, LoseReason reason) {
        if (result_.loser_count < MaxLosers) {
            auto& loser = result_.losers[result_.loser_count++];
            loser.source = req.source;
            loser.entry_id = req.entry_id;
            loser.target_pipe = static_cast<uint8_t>(pipe);
            loser.tag = req.tag;
            loser.reason = reason;
        }
    }

    void tryOverflow_(const Request& req, std::size_t from_pipe) {
        for (std::size_t i = 0; i < overflow_count_; ++i) {
            auto& ovf = overflows_[i];
            if (ovf.source == req.source && ovf.from_pipe == static_cast<uint8_t>(from_pipe)) {
                auto to = ovf.to_pipe;
                addRequest(to, req);
                return;
            }
        }
    }

    bool canPass_(SourceEnum src_higher, SourceEnum src_lower) const {
        for (std::size_t i = 0; i < pass_rule_count_; ++i) {
            if (pass_rules_[i].higher == src_higher && pass_rules_[i].lower == src_lower) {
                return true;
            }
        }
        return false;
    }

    void resolveBankConflicts_() {
        if (bank_conflict_fn_ == nullptr) return;

        struct LaneInfo {
            uint64_t addr = 0;
            bool has_addr = false;
            SourceEnum source{};
        };
        std::array<LaneInfo, NumPipes> lane_info{};

        for (std::size_t p = 0; p < NumPipes; ++p) {
            if (result_.winners[p].valid) {
                lane_info[p] = {winner_addrs_[p], true, result_.winners[p].source};
            } else if (occupied_[p].present && occupied_[p].has_addr) {
                lane_info[p] = {occupied_[p].addr, true, occupied_[p].source};
            }
        }

        auto laneActive = [&](std::size_t lane) {
            return result_.winners[lane].valid ||
                   (occupied_[lane].present && occupied_[lane].has_addr);
        };

        auto blockIfConflict = [&](std::size_t victim, std::size_t blocker) {
            if (!result_.winners[victim].valid || !lane_info[victim].has_addr) {
                return false;
            }
            if (!laneActive(blocker) || !lane_info[blocker].has_addr) {
                return false;
            }
            if (!bank_conflict_fn_(lane_info[victim].addr, lane_info[blocker].addr)) {
                return false;
            }

            SourceEnum src_higher = lane_info[blocker].source;
            if (canPass_(src_higher, result_.winners[victim].source)) {
                return false;
            }

            result_.winners[victim].bank_conflict = true;
            result_.winners[victim].valid = false;

            addLoser_(Request{result_.winners[victim].source, result_.winners[victim].entry_id,
                              lane_info[victim].addr, true, result_.winners[victim].tag},
                      victim, LoseReason::BANK_CONFLICT);
            return true;
        };

        if (bank_conflict_priority_ == BankConflictPriority::HigherPipeIndex) {
            for (std::size_t lane_offset = 0; lane_offset < NumPipes; ++lane_offset) {
                const std::size_t lane = NumPipes - 1 - lane_offset;
                for (std::size_t higher_offset = 0; higher_offset + lane + 1 < NumPipes;
                     ++higher_offset) {
                    const std::size_t higher = NumPipes - 1 - higher_offset;
                    if (blockIfConflict(lane, higher)) {
                        break;
                    }
                }
            }
            return;
        }

        // Default: lower-index pipe is treated as higher priority.
        for (std::size_t lane = 1; lane < NumPipes; ++lane) {
            if (!result_.winners[lane].valid || !lane_info[lane].has_addr) {
                continue;
            }

            for (std::size_t higher = 0; higher < lane; ++higher) {
                if (blockIfConflict(lane, higher)) {
                    break;
                }
            }
        }
    }

    void fillIdlePipes_() {
        std::size_t fill_idx = 0;
        for (std::size_t pipe = 0; pipe < NumPipes; ++pipe) {
            if (result_.winners[pipe].valid || occupied_[pipe].present) {
                continue;
            }

            while (fill_idx < idle_fill_request_count_) {
                auto& req = idle_fill_requests_[fill_idx];
                if (req.valid) {
                    bool is_idle_source = false;
                    for (std::size_t s = 0; s < idle_fill_source_count_; ++s) {
                        if (idle_fill_sources_[s] == req.source) {
                            is_idle_source = true;
                            break;
                        }
                    }

                    if (is_idle_source) {
                        auto& winner = result_.winners[pipe];
                        winner.source = req.source;
                        winner.entry_id = req.entry_id;
                        winner.pipe_id = static_cast<uint8_t>(pipe);
                        winner.tag = req.tag;
                        winner.valid = true;
                        winner_addrs_[pipe] = req.addr;
                        req.valid = false;
                        ++fill_idx;
                        break;
                    }
                }
                ++fill_idx;
            }
        }
    }

    std::array<uint64_t, NumPipes> winner_addrs_{};
};

}  // namespace chronon::sender
