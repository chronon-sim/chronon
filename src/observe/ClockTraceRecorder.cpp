// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// SPDX-License-Identifier: MPL-2.0
#include "ClockTraceRecorder.hpp"

#include <fmt/format.h>

#include <algorithm>
#include <array>
#include <bit>
#include <chrono>
#include <exception>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <span>
#include <thread>
#include <tuple>

#include "detail/ClockEventBuffer.hpp"

namespace chronon::observe {
namespace {
// Read host time only on the slow path; stream counters follow producer ownership.
class ClockTraceStall {
public:
    ClockTraceStall(bool stalled, uint64_t& count, uint64_t& ns) : ns_(stalled ? &ns : nullptr) {
        if (ns_) {
            ++count;
            start_ = std::chrono::steady_clock::now();
        }
    }
    ~ClockTraceStall() {
        if (ns_)
            *ns_ += std::chrono::duration_cast<std::chrono::nanoseconds>(
                        std::chrono::steady_clock::now() - start_)
                        .count();
    }

private:
    uint64_t* ns_;
    std::chrono::steady_clock::time_point start_;
};
}  // namespace

ClockTraceStream::ClockTraceStream(size_t capacity, bool lossless, bool perfetto, ClockDomain clock,
                                   std::atomic<bool>* failed)
    : ring_(capacity),
      lossless_(lossless),
      failed_(failed),
      perfetto_(perfetto),
      clock_(std::move(clock)) {}

void ClockTraceStream::advance(uint64_t next_cycle) {
    if (coordinator_ || parallel_)
        throw std::logic_error("coordinated streams use recorder progress");
    if (finished_.load(std::memory_order_relaxed) || next_cycle < minimum_cycle_)
        throw std::logic_error("invalid clock stream progress after finish or backwards");
    const auto ns = perfetto_ ? clock_.edge(next_cycle).floorNanoseconds() : 0;
    minimum_cycle_ = next_cycle;
    watermark_.store(ns, std::memory_order_release);
    ClockTraceStall stall(perfetto_ && acknowledged_.load(std::memory_order_acquire) < ns, stalls_,
                          stall_ns_);
    while (perfetto_ && acknowledged_.load(std::memory_order_acquire) < ns) {
        if (failed_->load(std::memory_order_acquire))
            throw std::runtime_error("clock trace backend failed or closed");
        std::this_thread::yield();
    }
    if (failed_->load(std::memory_order_acquire))
        throw std::runtime_error("clock trace backend failed or closed");
}

void ClockTraceStream::finish() {
    if (coordinator_ || parallel_)
        throw std::logic_error("coordinated streams finish with recorder close");
    finished_.store(true, std::memory_order_release);
}

void ClockTraceStream::endEdge() {
    if (!dropped_run_.value) return;
    const auto head = head_.load(std::memory_order_relaxed);
    auto tail = tail_.load(std::memory_order_acquire);
    {
        ClockTraceStall stall(head - tail == ring_.size(), stalls_, stall_ns_);
        while (head - tail == ring_.size()) {
            if (failed_->load(std::memory_order_acquire))
                throw std::runtime_error("clock trace backend failed or closed");
            std::this_thread::yield();
            tail = tail_.load(std::memory_order_acquire);
        }
    }
    ring_[head & (ring_.size() - 1)] = dropped_run_;
    dropped_run_.value = 0;
    peak_ = std::max(peak_, head - tail + 1);
    head_.store(head + 1, std::memory_order_release);
}

void ClockTraceStream::record(uint64_t cycle, ClockEventKind kind, uint64_t transaction,
                              uint64_t value, uint32_t fifo, ClockEventPhase phase) {
    if (cycle < minimum_cycle_ || finished_.load(std::memory_order_relaxed))
        throw std::logic_error("clock record violates stream progress or finish");
    if (ordinal_ == UINT64_MAX) throw std::overflow_error("clock trace stream ordinal overflow");
    const auto ordinal = ordinal_++;
    if (dropped_run_.value &&
        (dropped_run_.local_cycle != cycle ||
         (static_cast<uint8_t>(dropped_run_.phase) & 127) != static_cast<uint8_t>(phase)))
        endEdge();
    auto head = head_.load(std::memory_order_relaxed);
    auto tail = tail_.load(std::memory_order_acquire);
    if (dropped_run_.value && head - tail != ring_.size()) {
        endEdge();
        head = head_.load(std::memory_order_relaxed);
        tail = tail_.load(std::memory_order_acquire);
    }
    {
        ClockTraceStall stall(lossless_ && head - tail == ring_.size(), stalls_, stall_ns_);
        while (head - tail == ring_.size()) {
            if (failed_->load(std::memory_order_acquire))
                throw std::runtime_error("clock trace backend failed or closed");
            if (!lossless_) {
                ++dropped_;
                if (parallel_) {
                    if (!dropped_run_.value) {
                        dropped_run_ = {
                            cycle,
                            0,
                            0,
                            ordinal,
                            0,
                            kind,
                            static_cast<ClockEventPhase>(static_cast<uint8_t>(phase) | 128)};
                    }
                    ++dropped_run_.value;
                }
                return;
            }
            // Host waiting never changes simulated time or acceptance decisions.
            std::this_thread::yield();
            tail = tail_.load(std::memory_order_acquire);
        }
        if (failed_->load(std::memory_order_acquire))
            throw std::runtime_error("clock trace backend failed or closed");
    }
    if (coordinator_) coordinator_->reserveClockRecord(record_base_bytes_, kind);
    ring_[head & (ring_.size() - 1)] = {cycle, transaction, value, ordinal, fifo, kind, phase};
    peak_ = std::max(peak_, head - tail + 1);
    head_.store(head + 1, std::memory_order_release);
}

namespace {
constexpr std::string_view ClockCategory = "clock";
constexpr std::array<std::string_view, 6> ClockAnnotations = {
    "unit_id", "fifo_id", "transaction_id", "phase", "value", "ordinal"};

void validateText(std::string_view value) {
    if (value.empty() || value.size() > 1024)
        throw std::invalid_argument("clock metadata string length outside [1,1024]");
    for (unsigned char c : value) {
        if (c < 32 || c >= 127)
            throw std::invalid_argument("clock metadata requires printable ASCII");
    }
}
}  // namespace

struct ClockTraceRecorder::Impl {
    explicit Impl(Config config_) : config(std::move(config_)) {}
    Config config;
    struct Stream {
        ClockDomain domain;
        uint32_t unit_id;
        std::string unit_name;
        std::unique_ptr<ClockTraceStream> queue;
        uint32_t sequence = 0;
        uint64_t track = 0;
        size_t logical_unit = 0;
        uint64_t producer_order = 0, encoded_ordinal = 0;
    };
    struct TextSink {
        std::ofstream file;
        std::string buffer;
    };
    std::vector<Stream> streams;
    std::map<ClockEventKind, std::string> names = {
        {ClockEventKind::Write, "fifo.write"},     {ClockEventKind::Visible, "fifo.visible"},
        {ClockEventKind::Read, "fifo.read"},       {ClockEventKind::Output, "fifo.output"},
        {ClockEventKind::Consume, "fifo.consume"}, {ClockEventKind::Full, "fifo.full"},
        {ClockEventKind::Empty, "fifo.empty"},     {ClockEventKind::User, "user"}};
    std::map<ClockDomainId, TextSink> text;
    PerfettoTraceWriter writer;
    std::thread worker;
    std::atomic<bool> stopping{false}, failed{false};
    std::atomic<uint64_t> watermark{0}, acknowledged{0};
    std::exception_ptr error;
    bool started = false, closed = false;
    Stats stats;
    // Producer-only resource reservations, not event ordering or hardware state.
    bool coordinated = false, batch_active = false;
    uint64_t bucket_ns = 0;
    size_t pending_records = 0, pending_bytes = 0;
    size_t bucket_records = 0, bucket_bytes = 0, batches = 0;
    std::vector<uint16_t> event_name_bytes;

    // Scheduler publishes slot descriptors; backend alone owns their contents.
    // Unlike native records, staged records own no strings or payloads. Reserve
    // each admitted bucket's full capacity so ingress drain never depends on a
    // different actor getting CPU time. The native writer holds ONE bucket.
    struct PendingRecord {
        ClockRecord record;
        SimTime time;
        size_t stream;
    };
    struct Bucket {
        uint64_t ns = 0;
        size_t bytes = 0;
        size_t size = 0;
        PendingRecord* records = nullptr;
    };
    // Two fixed allocations, not one allocation per bucket. Include BOTH
    // arrays in the budget; allocator bookkeeping no longer scales with slots.
    std::unique_ptr<PendingRecord[]> staging;
    std::unique_ptr<Bucket[]> bucket_storage;
    std::span<Bucket> buckets;
    alignas(64) std::atomic<uint64_t> bucket_head{0};
    alignas(64) std::atomic<uint64_t> bucket_tail{0};
    size_t staged_records = 0;
    size_t bucket_capacity = 0;

    void checkFailure() const {
        if (failed.load(std::memory_order_acquire)) {
            if (error) std::rethrow_exception(error);
            throw std::runtime_error("clock trace backend failed or closed");
        }
    }

    void stage(size_t stream_index, const ClockRecord& record) {
        auto& stream = streams[stream_index];
        const auto time = stream.domain.edge(record.local_cycle);
        const auto ns = time.floorNanoseconds();
        const auto head = bucket_head.load(std::memory_order_acquire);
        auto first = bucket_tail.load(std::memory_order_relaxed), last = head;
        while (first < last) {
            const auto mid = first + (last - first) / 2;
            if (buckets[mid % buckets.size()].ns < ns)
                first = mid + 1;
            else
                last = mid;
        }
        if (first != head && buckets[first % buckets.size()].ns == ns) {
            auto& bucket = buckets[first % buckets.size()];
            const auto bytes = stream.queue->record_base_bytes_ + names.at(record.kind).size();
            if (bucket.size == bucket_capacity ||
                bytes > detail::ClockEventBuffer::MaxPendingBytes - bucket.bytes)
                throw std::length_error(
                    "native clock single-nanosecond bucket exceeds record or 4 MiB byte budget");
            bucket.records[bucket.size++] = {record, time, stream_index};
            bucket.bytes += bytes;
            stats.peak_staging_records =
                std::max<uint64_t>(stats.peak_staging_records, ++staged_records);
            return;
        }
        throw std::logic_error("clock event outside admitted observation buckets");
    }

    void drainBuckets(uint64_t limit, bool stop) {
        auto tail = bucket_tail.load(std::memory_order_relaxed);
        const auto head = bucket_head.load(std::memory_order_acquire);
        while (tail != head) {
            auto& bucket = buckets[tail % buckets.size()];
            if (!stop && bucket.ns >= limit) break;
            const std::span records(bucket.records, bucket.size);
            std::sort(records.begin(), records.end(), [&](const auto& a, const auto& b) {
                if (a.time != b.time) return a.time < b.time;
                const auto& sa = streams[a.stream];
                const auto& sb = streams[b.stream];
                return std::tuple{static_cast<uint8_t>(a.record.phase) & 127,
                                  std::string_view(sa.unit_name), sa.producer_order,
                                  a.record.ordinal} <
                       std::tuple{static_cast<uint8_t>(b.record.phase) & 127,
                                  std::string_view(sb.unit_name), sb.producer_order,
                                  b.record.ordinal};
            });
            for (auto& pending : records) {
                auto& stream = streams[pending.stream];
                auto& ordinal = streams[stream.logical_unit].encoded_ordinal;
                if (static_cast<uint8_t>(pending.record.phase) & 128) {
                    if (pending.record.value > UINT64_MAX - ordinal)
                        throw std::overflow_error("clock unit ordinal overflow");
                    ordinal += pending.record.value;
                    continue;
                }
                if (ordinal == UINT64_MAX) throw std::overflow_error("clock unit ordinal overflow");
                pending.record.ordinal = ordinal++;
                encode(stream, pending.record);
            }
            // One closed bucket fits the native record/metadata budgets. No
            // later bucket enters the writer until this one has been emitted.
            if (config.perfetto)
                writer.advanceClockWatermark(bucket.ns == UINT64_MAX ? UINT64_MAX : bucket.ns + 1);
            staged_records -= bucket.size;
            bucket.size = 0;
            bucket.bytes = 0;
            bucket_tail.store(++tail, std::memory_order_release);
        }
    }

    void writeManifest() {
        std::ofstream file(config.output_dir / "clock-manifest.json");
        file.exceptions(std::ios::badbit | std::ios::failbit);
        file << "{\n  \"version\":1,\n  \"run_id\":" << std::quoted(config.run_id)
             << ",\n  \"time_unit\":\"rational seconds\",\n  \"perfetto_unit\":\"ns (floor)\","
             << "\n  \"lossless\":" << (config.lossless ? "true" : "false")
             << ",\n  \"stream_capacity\":" << config.stream_capacity << ",\n  \"domains\":[";
        std::map<ClockDomainId, const ClockDomain*> clocks;
        for (const auto& stream : streams) clocks.emplace(stream.domain.id(), &stream.domain);
        bool first = true;
        for (const auto& [id, clock] : clocks) {
            if (!first) file << ',';
            first = false;
            file << "\n    {\"id\":" << id << ",\"name\":" << std::quoted(clock->name())
                 << ",\"period_num\":" << clock->period().numerator()
                 << ",\"period_den\":" << clock->period().denominator()
                 << ",\"phase_num\":" << clock->phase().numerator()
                 << ",\"phase_den\":" << clock->phase().denominator() << '}';
        }
        file << "\n  ],\n  \"streams\":[";
        first = true;
        for (const auto& stream : streams) {
            if (!first) file << ',';
            first = false;
            file << "\n    {\"unit_id\":" << stream.unit_id
                 << ",\"unit\":" << std::quoted(stream.unit_name)
                 << ",\"domain_id\":" << stream.domain.id() << ",\"sequence\":" << stream.sequence
                 << ",\"track\":" << stream.track << ",\"producer_order\":" << stream.producer_order
                 << '}';
        }
        file << "\n  ],\n  \"events\":{";
        first = true;
        for (const auto& [kind, name] : names) {
            if (!first) file << ',';
            first = false;
            file << std::quoted(std::to_string(static_cast<uint16_t>(kind))) << ':'
                 << std::quoted(name);
        }
        file << "}\n}\n";
    }

    void encode(Stream& stream, const ClockRecord& record) {
        const auto& name = names.at(record.kind);
        if (config.text) {
            auto& sink = text.at(stream.domain.id());
            fmt::format_to(std::back_inserter(sink.buffer), "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
                           record.local_cycle, stream.unit_id, name,
                           static_cast<unsigned>(record.phase), record.transaction_id,
                           record.fifo_id, record.value, record.ordinal);
            if (sink.buffer.size() >= 65536) {
                sink.file.write(sink.buffer.data(),
                                static_cast<std::streamsize>(sink.buffer.size()));
                sink.buffer.clear();
            }
        }
        if (config.perfetto) {
            using A = PerfettoTraceWriter::Annotation;
            const std::array<A, 6> annotations = {
                {{ClockAnnotations[0], A::Kind::Uint, stream.unit_id},
                 {ClockAnnotations[1], A::Kind::Uint, record.fifo_id},
                 {ClockAnnotations[2], A::Kind::Uint, record.transaction_id},
                 {ClockAnnotations[3], A::Kind::Uint, static_cast<uint64_t>(record.phase)},
                 {ClockAnnotations[4], A::Kind::Uint, record.value},
                 {ClockAnnotations[5], A::Kind::Uint, record.ordinal}}};
            writer.clockInstant(stream.sequence, stream.track, ClockCategory, name,
                                record.local_cycle, record.transaction_id, annotations);
        }
        ++stats.events;
    }

    void run() noexcept {
        try {
            std::vector<uint64_t> heads(streams.size());
            unsigned idle = 0;
            for (;;) {
                const bool stop = stopping.load(std::memory_order_acquire);
                uint64_t limit = UINT64_MAX;
                // Read progress BEFORE the heads. Acquiring a promise then the
                // heads includes every record whose publication preceded it.
                if (!buckets.empty() && !stop) {
                    limit = watermark.load(std::memory_order_acquire);
                } else if (config.perfetto && !stop) {
                    for (const auto& stream : streams) {
                        const auto& queue = *stream.queue;
                        if (!queue.finished_.load(std::memory_order_acquire))
                            limit =
                                std::min(limit, queue.watermark_.load(std::memory_order_acquire));
                    }
                    limit = std::max(limit, watermark.load(std::memory_order_acquire));
                }
                for (size_t i = 0; i < streams.size(); ++i)
                    heads[i] = streams[i].queue->head_.load(std::memory_order_acquire);
                bool any = false;
                bool remaining;
                do {
                    remaining = false;
                    for (size_t i = 0; i < streams.size(); ++i) {
                        const auto index = config.reverse_drain ? streams.size() - 1 - i : i;
                        auto& stream = streams[index];
                        auto& queue = *stream.queue;
                        auto tail = queue.tail_.load(std::memory_order_relaxed);
                        const auto end =
                            tail + std::min<uint64_t>(heads[index] - tail, config.drain_batch);
                        for (; tail != end; ++tail) {
                            const auto& record = queue.ring_[tail & (queue.ring_.size() - 1)];
                            if (buckets.empty())
                                encode(stream, record);
                            else
                                stage(index, record);
                            any = true;
                        }
                        queue.tail_.store(tail, std::memory_order_release);
                        remaining |= tail != heads[index];
                    }
                } while (remaining);
                if (!buckets.empty()) {
                    drainBuckets(limit, stop);
                    acknowledged.store(limit, std::memory_order_release);
                } else if (config.perfetto) {
                    writer.advanceClockWatermark(limit);
                    acknowledged.store(limit, std::memory_order_release);
                    for (auto& stream : streams)
                        stream.queue->acknowledged_.store(limit, std::memory_order_release);
                }
                if (stop) break;
                if (any)
                    idle = 0;
                else if (config.perfetto && idle++ < 64)
                    std::this_thread::yield();
                else
                    std::this_thread::sleep_for(std::chrono::microseconds(50));
            }
            for (auto& [id, sink] : text) {
                (void)id;
                sink.file.write(sink.buffer.data(),
                                static_cast<std::streamsize>(sink.buffer.size()));
                sink.buffer.clear();
                sink.file.close();
            }
            writer.close();
        } catch (...) {
            error = std::current_exception();
            failed.store(true, std::memory_order_release);
        }
    }
};

ClockTraceRecorder::ClockTraceRecorder(Config config)
    : impl_(std::make_unique<Impl>(std::move(config))) {
    const auto& c = impl_->config;
    validateText(c.run_id);
    if (c.stream_capacity < 2 || c.stream_capacity > (1u << 20) ||
        !std::has_single_bit(c.stream_capacity) || c.drain_batch == 0 ||
        c.drain_batch > c.stream_capacity) {
        throw std::invalid_argument(
            "clock trace requires power-of-two capacity [2,2^20] and batch [1,capacity]");
    }
    if (c.perfetto && (c.perfetto_options.clock_buffer_records < 2 ||
                       c.perfetto_options.clock_buffer_records > 65536))
        throw std::invalid_argument("clock buffer records must be in [2,65536]");
}
ClockTraceRecorder::~ClockTraceRecorder() {
    try {
        close();
    } catch (const std::exception& e) {
        std::cerr << "[clock trace] " << e.what() << '\n';
    }
}
bool ClockTraceRecorder::enabled() const noexcept {
    return impl_->config.text || impl_->config.perfetto;
}

void ClockTraceRecorder::defineEvent(ClockEventKind kind, std::string name) {
    if (impl_->started) throw std::logic_error("event metadata is immutable after start");
    validateText(name);
    if (name.size() > 128 || impl_->names.size() >= 1024 || impl_->names.contains(kind)) {
        throw std::invalid_argument("duplicate/oversized clock event dictionary");
    }
    impl_->names.emplace(kind, std::move(name));
}

ClockTraceStream* ClockTraceRecorder::addStream(const ClockDomain& domain, uint32_t unit_id,
                                                std::string unit_name) {
    if (impl_->started) throw std::logic_error("clock streams must be declared before start");
    if (!enabled()) return nullptr;
    validateText(unit_name);
    for (const auto& stream : impl_->streams) {
        if (stream.unit_id == unit_id || stream.unit_name == unit_name) {
            throw std::invalid_argument("clock streams require unique unit IDs and names");
        }
        if ((stream.domain.id() == domain.id() &&
             (stream.domain.name() != domain.name() || stream.domain.period() != domain.period() ||
              stream.domain.phase() != domain.phase())) ||
            (stream.domain.name() == domain.name() && stream.domain.id() != domain.id())) {
            throw std::invalid_argument("inconsistent clock metadata");
        }
    }
    const auto bytes = impl_->config.stream_capacity * sizeof(ClockRecord);
    if (impl_->stats.allocated_buffer_bytes + bytes > 256 * 1024 * 1024) {
        throw std::invalid_argument("clock trace ingress budget exceeds 256 MiB");
    }
    auto queue = std::unique_ptr<ClockTraceStream>(
        new ClockTraceStream(impl_->config.stream_capacity, impl_->config.lossless,
                             impl_->config.perfetto, domain, &impl_->failed));
    auto* result = queue.get();
    const auto index = impl_->streams.size();
    impl_->streams.push_back(
        {domain, unit_id, std::move(unit_name), std::move(queue), 0, 0, index});
    impl_->stats.allocated_buffer_bytes += bytes;
    return result;
}

ClockTraceStream* ClockTraceRecorder::addProducerStream(ClockTraceStream* unit,
                                                        uint64_t producer_order) {
    if (impl_->started) throw std::logic_error("clock streams must be declared before start");
    if (!enabled()) return nullptr;
    const auto found = std::find_if(impl_->streams.begin(), impl_->streams.end(),
                                    [&](const auto& stream) { return stream.queue.get() == unit; });
    if (found == impl_->streams.end() || !producer_order || found->producer_order)
        throw std::invalid_argument("producer stream requires a primary unit and nonzero order");
    const auto logical = found->logical_unit;
    for (const auto& stream : impl_->streams)
        if (stream.logical_unit == logical && stream.producer_order == producer_order)
            throw std::invalid_argument("duplicate clock producer order");
    const auto bytes = impl_->config.stream_capacity * sizeof(ClockRecord);
    if (impl_->stats.allocated_buffer_bytes + bytes > 256 * 1024 * 1024)
        throw std::invalid_argument("clock trace ingress budget exceeds 256 MiB");
    auto queue = std::unique_ptr<ClockTraceStream>(
        new ClockTraceStream(impl_->config.stream_capacity, impl_->config.lossless,
                             impl_->config.perfetto, found->domain, &impl_->failed));
    auto* result = queue.get();
    // Copy metadata before push_back can invalidate found.
    Impl::Stream stream{found->domain, found->unit_id, found->unit_name, std::move(queue), 0, 0,
                        logical,       producer_order};
    impl_->streams.push_back(std::move(stream));
    impl_->stats.allocated_buffer_bytes += bytes;
    return result;
}

void ClockTraceRecorder::startParallel(size_t lookahead_batches) {
    auto& p = *impl_;
    if (p.started || !lookahead_batches) throw std::logic_error("invalid parallel clock start");
    if (!enabled()) return start();
    const auto records = std::min(
        p.config.perfetto_options.clock_buffer_records,
        detail::ClockEventBuffer::MaxPendingBytes / detail::ClockEventBuffer::BaseRecordBytes);
    if (records < 2)
        throw std::invalid_argument("parallel clock bucket needs at least two records");
    const auto slot_bytes = records * sizeof(Impl::PendingRecord) + sizeof(Impl::Bucket);
    const auto available = 256 * 1024 * 1024 - p.stats.allocated_buffer_bytes;
    const auto max_slots = available / slot_bytes;
    if (!max_slots)
        throw std::invalid_argument("parallel clock ingress/staging budget exceeds 256 MiB");
    const auto slots = std::min(lookahead_batches, max_slots - 1) + 1;
    auto buckets = std::make_unique<Impl::Bucket[]>(slots);
    auto staging = std::make_unique<Impl::PendingRecord[]>(slots * records);
    for (size_t i = 0; i < slots; ++i) buckets[i].records = staging.get() + i * records;
    p.bucket_storage = std::move(buckets);
    p.staging = std::move(staging);
    p.buckets = {p.bucket_storage.get(), slots};
    p.bucket_capacity = records;
    p.stats.allocated_staging_bytes = slots * slot_bytes;
    start();
}

bool ClockTraceRecorder::parallelActive() const noexcept {
    return impl_->started && !impl_->buckets.empty() && !impl_->closed;
}

bool ClockTraceRecorder::tryAdmitClockBatch(const SimTime& time) {
    auto& p = *impl_;
    if (!parallelActive()) throw std::logic_error("clock admission requires parallel recording");
    p.checkFailure();
    const auto ns = time.floorNanoseconds();
    if (ns < p.watermark.load(std::memory_order_relaxed))
        throw std::logic_error("clock admission precedes published progress");
    const auto head = p.bucket_head.load(std::memory_order_relaxed);
    const auto tail = p.bucket_tail.load(std::memory_order_acquire);
    // Termination may discard unused simulation grants. Their empty observation
    // slots remain reserved and can be reused after resetTermination().
    if (tail != head && ns <= p.buckets[(head - 1) % p.buckets.size()].ns) {
        for (auto n = tail; n != head; ++n)
            if (p.buckets[n % p.buckets.size()].ns == ns) return true;
        throw std::logic_error("clock observation admissions must be ordered");
    }
    if (head - tail == p.buckets.size()) {
        ++p.stats.admission_retries;
        return false;
    }
    if (head == UINT64_MAX) throw std::overflow_error("clock observation admission overflow");
    p.buckets[head % p.buckets.size()].ns = ns;
    p.bucket_head.store(head + 1, std::memory_order_release);
    return true;
}

void ClockTraceRecorder::publishClockProgress(uint64_t exclusive_ns) {
    auto& p = *impl_;
    if (!parallelActive()) throw std::logic_error("clock progress requires parallel recording");
    p.checkFailure();
    if (exclusive_ns < p.watermark.load(std::memory_order_relaxed))
        throw std::logic_error("clock recorder watermark cannot move backwards");
    p.watermark.store(exclusive_ns, std::memory_order_release);
}

void ClockTraceRecorder::start(bool serial_coordinator) {
    if (impl_->started) throw std::logic_error("clock recorder cannot be restarted");
    if (impl_->buckets.empty() &&
        std::any_of(impl_->streams.begin(), impl_->streams.end(),
                    [](const auto& stream) { return stream.producer_order != 0; }))
        throw std::logic_error("additional clock producers require startParallel");
    if (!enabled()) {
        impl_->started = true;
        return;
    }
    const auto& config = impl_->config;
    impl_->coordinated = serial_coordinator && config.perfetto;
    if (impl_->coordinated || !impl_->buckets.empty()) {
        if (impl_->coordinated) {
            impl_->event_name_bytes.resize(1u << 16);
            for (const auto& [kind, name] : impl_->names)
                impl_->event_name_bytes[static_cast<uint16_t>(kind)] = name.size();
        }
        size_t base = detail::ClockEventBuffer::BaseRecordBytes + ClockCategory.size();
        for (auto name : ClockAnnotations) base += name.size();
        for (auto& stream : impl_->streams) {
            if (impl_->coordinated) stream.queue->coordinator_ = this;
            stream.queue->parallel_ = !impl_->buckets.empty();
            stream.queue->record_base_bytes_ = base + stream.unit_name.size();
        }
    }
    if (std::filesystem::exists(config.output_dir) &&
        !std::filesystem::is_empty(config.output_dir)) {
        throw std::invalid_argument("clock trace output directory must be new or empty");
    }
    std::filesystem::create_directories(config.output_dir);
    if (config.perfetto) {
        if (!impl_->writer.open(config.output_dir / "timeline.pftrace", config.perfetto_options)) {
            throw std::runtime_error("cannot open clock Perfetto trace");
        }
        for (auto& stream : impl_->streams)
            stream.sequence = impl_->writer.addClockStream(stream.domain);
    }
    std::map<ClockDomainId, uint64_t> domains;
    for (auto& stream : impl_->streams) {
        if (config.perfetto) {
            auto [entry, inserted] = domains.try_emplace(stream.domain.id(), 0);
            if (inserted) entry->second = impl_->writer.addTrack("domain-" + stream.domain.name());
            stream.track = stream.producer_order
                               ? impl_->streams[stream.logical_unit].track
                               : impl_->writer.addTrack(stream.unit_name, entry->second);
        }
        if (config.text && !impl_->text.contains(stream.domain.id())) {
            auto& sink = impl_->text[stream.domain.id()];
            sink.file.exceptions(std::ios::badbit | std::ios::failbit);
            sink.file.open(config.output_dir / ("text-domain-" + stream.domain.name() + ".log"));
            sink.buffer.reserve(66048);
            sink.file
                << "# run=" << config.run_id << " domain=" << stream.domain.id()
                << " clock-manifest.json; per-stream order only\n"
                << "# "
                   "local_cycle\tunit_id\tevent\tphase\ttransaction_id\tfifo_id\tvalue\tordinal\n";
        }
    }
    impl_->writeManifest();
    impl_->started = true;
    impl_->worker = std::thread([this] { impl_->run(); });
}

bool ClockTraceRecorder::needsProgress() const noexcept {
    return impl_->config.perfetto && !impl_->closed;
}

void ClockTraceRecorder::advance(uint64_t exclusive_ns) {
    if (!impl_->started || impl_->closed)
        throw std::logic_error("clock progress requires a running recorder");
    if (!impl_->config.perfetto) return;
    if (exclusive_ns < impl_->watermark.load(std::memory_order_relaxed))
        throw std::invalid_argument("clock recorder watermark cannot move backwards");
    impl_->watermark.store(exclusive_ns, std::memory_order_release);
    ClockTraceStall stall(impl_->acknowledged.load(std::memory_order_acquire) < exclusive_ns,
                          impl_->stats.progress_stalls, impl_->stats.progress_stall_ns);
    while (impl_->acknowledged.load(std::memory_order_acquire) < exclusive_ns) {
        if (impl_->failed.load(std::memory_order_acquire))
            throw std::runtime_error("clock trace backend failed or closed");
        std::this_thread::yield();
    }
    if (impl_->failed.load(std::memory_order_acquire))
        throw std::runtime_error("clock trace backend failed or closed");
}

void ClockTraceRecorder::beginClockBatch(const SimTime& time) {
    auto& p = *impl_;
    if (!p.started || p.closed || !p.coordinated || p.batch_active)
        throw std::logic_error("invalid coordinated clock batch");
    const auto ns = time.floorNanoseconds();
    if (ns < p.bucket_ns || ns < p.watermark.load(std::memory_order_relaxed))
        throw std::logic_error("clock batch precedes published progress");
    if (ns != p.bucket_ns) {
        p.bucket_records = p.bucket_bytes = 0;
        p.bucket_ns = ns;
    }
    p.batch_active = true;
}

void ClockTraceRecorder::reserveClockRecord(size_t base_bytes, ClockEventKind kind) {
    auto& p = *impl_;
    if (!p.batch_active) throw std::logic_error("clock record outside coordinated batch");
    const auto name_bytes = p.event_name_bytes[static_cast<uint16_t>(kind)];
    if (!name_bytes) throw std::invalid_argument("undefined clock event kind");
    const auto bytes = base_bytes + name_bytes;
    const auto capacity = p.config.perfetto_options.clock_buffer_records;
    constexpr auto byte_limit = detail::ClockEventBuffer::MaxPendingBytes;
    if (p.bucket_records == capacity || bytes > byte_limit - p.bucket_bytes)
        throw std::length_error(
            "native clock single-nanosecond bucket exceeds record or 4 MiB byte budget");
    if (p.pending_records == capacity || bytes > byte_limit - p.pending_bytes) {
        // All older-ns events were published by this serial producer. Never
        // close the current bucket, even if this happens midway through a tick.
        advance(p.bucket_ns);
        p.pending_records = p.bucket_records;
        p.pending_bytes = p.bucket_bytes;
        p.batches = 0;
    }
    ++p.pending_records;
    ++p.bucket_records;
    p.pending_bytes += bytes;
    p.bucket_bytes += bytes;
}

void ClockTraceRecorder::endClockBatch() {
    auto& p = *impl_;
    if (!p.batch_active) throw std::logic_error("no coordinated clock batch to end");
    if (++p.batches == 64) {
        advance(p.bucket_ns);
        p.pending_records = p.bucket_records;
        p.pending_bytes = p.bucket_bytes;
        p.batches = 0;
    }
    p.batch_active = false;
}

void ClockTraceRecorder::close() {
    if (!impl_->started || impl_->closed) return;
    // All producers have stopped. Flush any gap metadata left by an interrupted
    // actor before taking the backend's final queue snapshot.
    try {
        if (!impl_->failed.load(std::memory_order_acquire))
            for (auto& stream : impl_->streams) stream.queue->endEdge();
    } catch (...) {
        // Backend failure while flushing metadata: still join below, then
        // rethrow the original backend error rather than leaving a live thread.
    }
    impl_->stopping.store(true, std::memory_order_release);
    if (impl_->worker.joinable()) impl_->worker.join();
    impl_->failed.store(true, std::memory_order_release);
    impl_->closed = true;
    for (const auto& stream : impl_->streams) {
        impl_->stats.dropped += stream.queue->dropped_;
        impl_->stats.producer_stalls += stream.queue->stalls_;
        impl_->stats.producer_stall_ns += stream.queue->stall_ns_;
        impl_->stats.peak_buffer_bytes += stream.queue->peak_ * sizeof(ClockRecord);
    }
    if (impl_->error) std::rethrow_exception(impl_->error);
    impl_->stats.native_buffer_peak_bytes = impl_->writer.clockBufferPeakBytes();
    impl_->stats.native_buffer_peak_records = impl_->writer.clockBufferPeakRecords();
    impl_->stats.first_output_ns = impl_->writer.firstClockOutputNanoseconds();
    if (enabled()) {
        std::ofstream report(impl_->config.output_dir / "clock-stats.json");
        report.exceptions(std::ios::badbit | std::ios::failbit);
        report << "{\"events\":" << impl_->stats.events
               << ",\"producer_stalls\":" << impl_->stats.producer_stalls
               << ",\"producer_stall_ns\":" << impl_->stats.producer_stall_ns
               << ",\"admission_retries\":" << impl_->stats.admission_retries
               << ",\"progress_stalls\":" << impl_->stats.progress_stalls
               << ",\"progress_stall_ns\":" << impl_->stats.progress_stall_ns
               << ",\"dropped_events\":" << impl_->stats.dropped
               << ",\"allocated_ingress_bytes\":" << impl_->stats.allocated_buffer_bytes
               << ",\"peak_ingress_bytes_upper_bound\":" << impl_->stats.peak_buffer_bytes
               << ",\"allocated_staging_bytes\":" << impl_->stats.allocated_staging_bytes
               << ",\"peak_staging_records\":" << impl_->stats.peak_staging_records
               << ",\"native_buffer_peak_bytes_upper_bound\":"
               << impl_->stats.native_buffer_peak_bytes
               << ",\"native_buffer_peak_records\":" << impl_->stats.native_buffer_peak_records
               << ",\"first_output_ns\":" << impl_->stats.first_output_ns
               << ",\"temporary_disk_bytes\":0}\n";
        report.close();
        for (const auto& entry : std::filesystem::directory_iterator(impl_->config.output_dir)) {
            if (entry.is_regular_file()) impl_->stats.file_bytes += entry.file_size();
        }
    }
}

ClockTraceRecorder::Stats ClockTraceRecorder::stats() const {
    if (!impl_->closed)
        throw std::logic_error("clock trace stats require close after producers stop");
    return impl_->stats;
}

}  // namespace chronon::observe
