// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// SPDX-License-Identifier: MPL-2.0
#pragma once

#include <array>
#include <bit>
#include <cassert>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <type_traits>
#include <vector>

#include "../core/TickableUnit.hpp"
#include "../schedule/ClockCalendar.hpp"

namespace chronon::sender {

/// Custom aggregate payloads must explicitly attest that all nested data is
/// owned. Raw/live pointers, references, shared mutable state and callbacks
/// are not legal CDC packets. A trait cannot inspect arbitrary C++ aggregates.
template <typename T>
struct CdcPayloadTraits : std::bool_constant<std::is_arithmetic_v<T> || std::is_enum_v<T>> {};
template <>
struct CdcPayloadTraits<std::string> : std::true_type {};
template <typename T, size_t N>
struct CdcPayloadTraits<std::array<T, N>> : CdcPayloadTraits<T> {};
template <typename T, typename A>
struct CdcPayloadTraits<std::vector<T, A>> : CdcPayloadTraits<T> {};
template <typename T>
struct CdcPayloadTraits<std::unique_ptr<T>> : CdcPayloadTraits<T> {};

template <typename T>
struct CdcPacket {
    static_assert(
        CdcPayloadTraits<T>::value && !std::is_pointer_v<T>,
        "CDC payload must own its data; explicitly specialize CdcPayloadTraits for aggregates");
    uint64_t transaction_id;
    T data;
};

struct AsyncFifoConfig {
    size_t depth = 8;                ///< Dual-port RAM entries, excluding the one output register.
    size_t synchronizer_stages = 2;  ///< Registers in EACH pointer synchronizer, >= 2.
};

/// Finite dual-clock circuit, independent of the scheduler. All state is
/// private and owned here. beginEdges snapshots pre-edge remote Gray pointers;
/// commit updates both domains from those snapshots (nonblocking-assignment semantics).
template <typename T>
class AsyncFifoCircuit {
public:
    using Packet = CdcPacket<T>;
    struct State {
        uint64_t write_binary, read_binary, write_gray, read_gray;
        uint64_t write_sync, read_sync;
        bool full, empty, output_valid;
        size_t ram_occupancy;
        uint64_t writes, reads;
    };
    struct Commit {
        bool wrote = false, read = false;
        bool full_changed = false, empty_changed = false;
        uint64_t write_transaction = 0, read_transaction = 0;
        uint64_t visible_begin = 0, visible_end = 0;
    };

    explicit AsyncFifoCircuit(AsyncFifoConfig config = {}) : config_(config) {
        if (config.depth < 2 || !std::has_single_bit(config.depth) ||
            config.depth > (size_t{1} << 30)) {
            throw std::invalid_argument("async FIFO depth must be a power of two in [2, 2^30]");
        }
        if (config.synchronizer_stages < 2 || config.synchronizer_stages > 64) {
            throw std::invalid_argument("async FIFO synchronizer stages must be in [2, 64]");
        }
        mask_ = 2 * config.depth - 1;
        ram_.resize(config.depth);
        transaction_tags_.resize(config.depth);
        write_sync_.resize(config.synchronizer_stages);
        read_sync_.resize(config.synchronizer_stages);
    }

    void beginEdges(bool write_edge, bool read_edge) {
        if (active_) throw std::logic_error("async FIFO edge already active");
        active_ = true;
        write_edge_ = write_edge;
        read_edge_ = read_edge;
        sampled_write_ = gray_(write_binary_);
        sampled_read_ = gray_(read_binary_);
        read_request_ = false;
    }
    bool canWrite() const {
        requireWrite_();
        return !full_ && !pending_write_;
    }
    bool tryWrite(Packet&& packet) {
        if (!canWrite()) return false;
        if (!packet.transaction_id)
            throw std::invalid_argument("CDC transaction ID must be nonzero");
        pending_write_.emplace(std::move(packet));
        return true;
    }
    bool canRead() const {
        requireRead_();
        return !empty_ && !read_request_ && !output_;
    }
    bool requestRead() {
        if (!canRead()) return false;
        read_request_ = true;
        return true;
    }
    bool outputValid() const {
        requireRead_();
        return output_.has_value();
    }
    std::optional<Packet> take() {
        requireRead_();
        if (!output_) return std::nullopt;
        auto result = std::move(output_);
        output_.reset();
        return result;
    }
    Commit commit(bool trace_visibility = false) {
        if (!active_) throw std::logic_error("async FIFO commit without edge sampling");
        Commit result;
        const uint64_t old_write_sync = write_sync_.back();
        const uint64_t old_read_sync = read_sync_.back();
        const bool old_full = full_, old_empty = empty_;
        if (read_edge_) {
            if (trace_visibility) {
                result.visible_begin = visible_binary_;
                result.visible_end = binary_(old_write_sync);
                visible_binary_ = result.visible_end;
            }
            if (read_request_) {
                auto& slot = ram_[read_binary_ & (config_.depth - 1)];
                if (empty_ || !slot || output_)
                    throw std::logic_error("async FIFO invalid RAM read");
                result.read = true;
                result.read_transaction = slot->transaction_id;
                output_.emplace(std::move(*slot));
                slot.reset();
                read_binary_ = (read_binary_ + 1) & mask_;
                --occupancy_;
                ++reads_;
            }
            // Local flag flop samples OLD synchronizer tail, not this edge's tail update.
            empty_ = gray_(read_binary_) == old_write_sync;
            shift_(write_sync_, sampled_write_);
        }
        if (write_edge_) {
            if (pending_write_) {
                auto& slot = ram_[write_binary_ & (config_.depth - 1)];
                if (full_ || slot) throw std::logic_error("async FIFO would overwrite unread RAM");
                result.wrote = true;
                result.write_transaction = pending_write_->transaction_id;
                transaction_tags_[write_binary_ & (config_.depth - 1)] = result.write_transaction;
                slot.emplace(std::move(*pending_write_));
                pending_write_.reset();
                write_binary_ = (write_binary_ + 1) & mask_;
                ++occupancy_;
                ++writes_;
            }
            // The top TWO Gray bits are inverted for a wrap-separated full comparison.
            const uint64_t invert = config_.depth | (config_.depth >> 1);
            full_ = gray_(write_binary_) == (old_read_sync ^ invert);
            shift_(read_sync_, sampled_read_);
        }
        result.full_changed = full_ != old_full;
        result.empty_changed = empty_ != old_empty;
        if (occupancy_ > config_.depth) throw std::logic_error("async FIFO occupancy invariant");
        active_ = false;
        return result;
    }
    State state() const noexcept {
        return {write_binary_,
                read_binary_,
                gray_(write_binary_),
                gray_(read_binary_),
                write_sync_.back(),
                read_sync_.back(),
                full_,
                empty_,
                bool(output_),
                occupancy_,
                writes_,
                reads_};
    }
    bool drained() const noexcept {
        return !active_ && occupancy_ == 0 && !output_ && !pending_write_ && !full_ && empty_ &&
               write_sync_.back() == gray_(write_binary_) &&
               read_sync_.back() == gray_(read_binary_);
    }
    size_t depth() const noexcept { return config_.depth; }
    bool edgeActive() const noexcept { return active_; }
    uint64_t pointerMask() const noexcept { return mask_; }
    /// Diagnostic identity only, never consulted by hardware control decisions.
    uint64_t transactionAt(uint64_t binary) const noexcept {
        return transaction_tags_[binary & (config_.depth - 1)];
    }

private:
    void requireWrite_() const {
        if (!active_ || !write_edge_)
            throw std::logic_error("FIFO write outside write-domain edge");
    }
    void requireRead_() const {
        if (!active_ || !read_edge_) throw std::logic_error("FIFO read outside read-domain edge");
    }
    static uint64_t gray_(uint64_t value) noexcept { return value ^ (value >> 1); }
    static uint64_t binary_(uint64_t gray) noexcept {
        uint64_t value = 0;
        for (; gray; gray >>= 1) value ^= gray;
        return value;
    }
    static void shift_(std::vector<uint64_t>& chain, uint64_t sampled) {
        for (size_t i = chain.size() - 1; i != 0; --i) chain[i] = chain[i - 1];
        chain[0] = sampled;
    }
    AsyncFifoConfig config_;
    std::vector<std::optional<Packet>> ram_;
    std::vector<uint64_t> transaction_tags_;
    std::optional<Packet> output_, pending_write_;
    std::vector<uint64_t> write_sync_, read_sync_;
    uint64_t mask_ = 0, write_binary_ = 0, read_binary_ = 0, visible_binary_ = 0;
    uint64_t sampled_write_ = 0, sampled_read_ = 0;
    uint64_t writes_ = 0, reads_ = 0;
    size_t occupancy_ = 0;  // Assertion/observation only. NEVER drives full/empty/acceptance.
    bool full_ = false, empty_ = true, active_ = false;
    bool write_edge_ = false, read_edge_ = false, read_request_ = false;
};

class CdcComponent {
public:
    virtual ~CdcComponent() = default;
    virtual uint32_t id() const noexcept = 0;
    virtual Unit* writeOwner() const noexcept = 0;
    virtual Unit* readOwner() const noexcept = 0;
    virtual void setClockTraceStreams(observe::ClockTraceStream* write,
                                      observe::ClockTraceStream* read) noexcept = 0;
    virtual void begin(std::span<const ClockEdge> edges) = 0;
    virtual void commit() = 0;
    virtual bool drained() const noexcept = 0;
};

template <typename T>
class AsyncFifo;

template <typename T>
class AsyncWritePort : public PortBase {
public:
    AsyncWritePort(TickableUnit* owner, std::string name) : PortBase(owner, std::move(name)) {}
    bool canSend() const {
        check_();
        return fifo_->circuit_.canWrite();
    }
    bool send(CdcPacket<T>&& packet) {
        check_();
        return fifo_->circuit_.tryWrite(std::move(packet));
    }

private:
    friend class AsyncFifo<T>;
    void check_() const {
        if (!fifo_ || !owner_->isClockEdgeExecuting()) {
            throw std::logic_error("async write port requires its owner's clock edge");
        }
    }
    AsyncFifo<T>* fifo_ = nullptr;
};

template <typename T>
class AsyncReadPort : public PortBase {
public:
    AsyncReadPort(TickableUnit* owner, std::string name) : PortBase(owner, std::move(name)) {}
    bool canRead() const {
        check_();
        return fifo_->circuit_.canRead();
    }
    bool requestRead() {
        check_();
        return fifo_->circuit_.requestRead();
    }
    bool outputValid() const {
        check_();
        return fifo_->circuit_.outputValid();
    }
    std::optional<CdcPacket<T>> take() {
        check_();
        auto packet = fifo_->circuit_.take();
        if (packet)
            fifo_->record_(owner_, owner_->localCycle(), observe::ClockEventKind::Consume,
                           packet->transaction_id, 0, observe::ClockEventPhase::Evaluate);
        return packet;
    }

private:
    friend class AsyncFifo<T>;
    void check_() const {
        if (!fifo_ || !owner_->isClockEdgeExecuting()) {
            throw std::logic_error("async read port requires its owner's clock edge");
        }
    }
    AsyncFifo<T>* fifo_ = nullptr;
};

/// Simulation-owned dual-port RAM and pointer logic; endpoints expose only
/// their local hardware interface, not the other Unit or live packet storage.
template <typename T>
class AsyncFifo final : public CdcComponent {
public:
    AsyncFifo(uint32_t id, AsyncWritePort<T>& write, AsyncReadPort<T>& read, AsyncFifoConfig config)
        : id_(id), write_(write), read_(read), circuit_(config) {
        if (!write.owner() || !read.owner() || write.fifo_ || read.fifo_) {
            throw std::invalid_argument("async FIFO endpoints must be owned and unbound");
        }
        write.fifo_ = this;
        read.fifo_ = this;
    }
    uint32_t id() const noexcept override { return id_; }
    Unit* writeOwner() const noexcept override { return write_.owner(); }
    Unit* readOwner() const noexcept override { return read_.owner(); }
    void setClockTraceStreams(observe::ClockTraceStream* write,
                              observe::ClockTraceStream* read) noexcept override {
        write_trace_ = write;
        read_trace_ = read;
    }
    void begin(std::span<const ClockEdge> edges) override {
        bool w = false, r = false;
        for (const auto& edge : edges) {
            if (edge.domain->id() == write_.owner()->clockDomainId()) {
                w = true;
                write_cycle_ = edge.cycle;
            }
            if (edge.domain->id() == read_.owner()->clockDomainId()) {
                r = true;
                read_cycle_ = edge.cycle;
            }
        }
        read_edge_ = r;
        circuit_.beginEdges(w, r);
    }
    void commit() override {
        const auto delta = circuit_.commit(commitTrace_(read_.owner()) != nullptr);
        if (delta.wrote)
            record_(write_.owner(), write_cycle_, observe::ClockEventKind::Write,
                    delta.write_transaction);
        if (read_edge_ && commitTrace_(read_.owner())) {
            for (auto n = delta.visible_begin; n != delta.visible_end;
                 n = (n + 1) & circuit_.pointerMask()) {
                record_(read_.owner(), read_cycle_, observe::ClockEventKind::Visible,
                        circuit_.transactionAt(n));
            }
        }
        if (delta.read) {
            record_(read_.owner(), read_cycle_, observe::ClockEventKind::Read,
                    delta.read_transaction);
            record_(read_.owner(), read_cycle_, observe::ClockEventKind::Output,
                    delta.read_transaction);
        }
        if (delta.full_changed)
            record_(write_.owner(), write_cycle_, observe::ClockEventKind::Full, 0,
                    circuit_.state().full);
        if (delta.empty_changed)
            record_(read_.owner(), read_cycle_, observe::ClockEventKind::Empty, 0,
                    circuit_.state().empty);
        if (delta.read || (delta.empty_changed && !circuit_.state().empty)) {
            wakeUnitAt(read_.owner(), read_cycle_ + 1);
        }
        if (delta.full_changed && !circuit_.state().full)
            wakeUnitAt(write_.owner(), write_cycle_ + 1);
    }
    bool drained() const noexcept override { return circuit_.drained(); }
    auto diagnostics() const {
        if (circuit_.edgeActive())
            throw std::logic_error("FIFO diagnostics are host-only between clock batches");
        return circuit_.state();
    }

private:
    friend class AsyncWritePort<T>;
    friend class AsyncReadPort<T>;
    observe::ClockTraceStream* commitTrace_(Unit* unit) const noexcept {
        auto* stream = unit == write_.owner() ? write_trace_ : read_trace_;
        return stream ? stream : unit->clockTraceStream();
    }
    void record_(Unit* unit, uint64_t cycle, observe::ClockEventKind kind, uint64_t transaction,
                 uint64_t value = 0,
                 observe::ClockEventPhase phase = observe::ClockEventPhase::Commit) {
        auto* stream = phase == observe::ClockEventPhase::Commit ? commitTrace_(unit)
                                                                 : unit->clockTraceStream();
        if (stream) {
            stream->record(cycle, kind, transaction, value, id_, phase);
        }
    }
    uint32_t id_;
    AsyncWritePort<T>& write_;
    AsyncReadPort<T>& read_;
    AsyncFifoCircuit<T> circuit_;
    uint64_t write_cycle_ = 0, read_cycle_ = 0;
    bool read_edge_ = false;
    observe::ClockTraceStream* write_trace_ = nullptr;
    observe::ClockTraceStream* read_trace_ = nullptr;
};

}  // namespace chronon::sender
