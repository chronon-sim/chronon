// SPDX-License-Identifier: MPL-2.0
#include "ClockEventSpool.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <fstream>
#include <optional>
#include <tuple>
#include <vector>

namespace chronon::observe::detail {
namespace {
using Record = ClockEventSpool::Record;
constexpr auto MaxRecordBytes = ClockEventSpool::MaxRecordBytes;

bool earlier(const Record& a, const Record& b) {
    if (a.time != b.time) return a.time < b.time;
    return std::tie(a.phase, a.track_name, a.ordinal, a.stream, a.stream_order) <
           std::tie(b.phase, b.track_name, b.ordinal, b.stream, b.stream_order);
}

void putNumber(std::vector<char>& data, uint64_t number) {
    if (data.size() > MaxRecordBytes - 8)
        throw std::invalid_argument("native clock record exceeds 64 KiB");
    for (unsigned i = 0; i < 8; ++i) data.push_back(static_cast<char>(number >> (i * 8)));
}

void putString(std::vector<char>& data, const std::string& value) {
    putNumber(data, value.size());
    if (value.size() > MaxRecordBytes - data.size())
        throw std::invalid_argument("native clock record exceeds 64 KiB");
    data.insert(data.end(), value.begin(), value.end());
}

void writeRecord(std::ostream& output, const Record& record, std::vector<char>& data) {
    data.clear();
    for (auto value : {record.time.numerator(), record.time.denominator(), record.phase,
                       record.ordinal, record.stream_order, record.track, record.cycle, record.flow,
                       uint64_t(record.stream), uint64_t(record.annotation_count)})
        putNumber(data, value);
    putString(data, record.track_name);
    putString(data, record.category);
    putString(data, record.name);
    for (size_t i = 0; i < record.annotation_count; ++i) {
        const auto& annotation = record.annotations[i];
        putString(data, annotation.name);
        putNumber(data, static_cast<uint64_t>(annotation.kind));
        putNumber(data, annotation.bits);
        putString(data, annotation.string);
    }
    char size[8];
    for (unsigned i = 0; i < 8; ++i) size[i] = static_cast<char>(uint64_t(data.size()) >> (i * 8));
    output.write(size, sizeof(size));
    output.write(data.data(), static_cast<std::streamsize>(data.size()));
}

uint64_t getNumber(const std::vector<char>& data, size_t& position) {
    if (data.size() - position < 8) throw std::runtime_error("truncated clock spool record");
    uint64_t value = 0;
    for (unsigned i = 0; i < 8; ++i)
        value |= uint64_t(static_cast<unsigned char>(data[position++])) << (i * 8);
    return value;
}

std::string getString(const std::vector<char>& data, size_t& position) {
    const auto length = getNumber(data, position);
    if (length > data.size() - position) throw std::runtime_error("invalid clock spool string");
    std::string value(data.data() + position, static_cast<size_t>(length));
    position += static_cast<size_t>(length);
    return value;
}

// Zero means clean EOF. The spool is private scratch data, not a checkpoint format.
size_t readRecord(std::istream& input, Record& record, std::vector<char>& data) {
    char header[8];
    if (!input.read(header, sizeof(header))) {
        if (input.eof() && input.gcount() == 0) return 0;
        throw std::runtime_error("cannot read clock spool frame");
    }
    uint64_t length = 0;
    for (unsigned i = 0; i < 8; ++i)
        length |= uint64_t(static_cast<unsigned char>(header[i])) << (i * 8);
    if (!length || length > MaxRecordBytes) throw std::runtime_error("invalid clock spool frame");
    data.resize(static_cast<size_t>(length));
    if (!input.read(data.data(), static_cast<std::streamsize>(length)))
        throw std::runtime_error("truncated clock spool frame");
    size_t position = 0;
    const auto numerator = getNumber(data, position);
    const auto denominator = getNumber(data, position);
    record.time = SimTime(numerator, denominator);
    record.phase = getNumber(data, position);
    record.ordinal = getNumber(data, position);
    record.stream_order = getNumber(data, position);
    record.track = getNumber(data, position);
    record.cycle = getNumber(data, position);
    record.flow = getNumber(data, position);
    const auto stream = getNumber(data, position), count = getNumber(data, position);
    if (stream > UINT32_MAX || count > record.annotations.size())
        throw std::runtime_error("invalid clock spool identity");
    record.stream = static_cast<uint32_t>(stream);
    record.annotation_count = static_cast<size_t>(count);
    record.track_name = getString(data, position);
    record.category = getString(data, position);
    record.name = getString(data, position);
    for (size_t i = 0; i < record.annotation_count; ++i) {
        auto& annotation = record.annotations[i];
        annotation.name = getString(data, position);
        const auto kind = getNumber(data, position);
        if (kind > static_cast<uint64_t>(PerfettoTraceWriter::Annotation::Kind::String))
            throw std::runtime_error("invalid clock spool annotation kind");
        annotation.kind = static_cast<PerfettoTraceWriter::Annotation::Kind>(kind);
        annotation.bits = getNumber(data, position);
        annotation.string = getString(data, position);
    }
    if (position != data.size()) throw std::runtime_error("trailing clock spool data");
    return static_cast<size_t>(length) + sizeof(header);
}

std::ofstream outputFile(const std::filesystem::path& path) {
    std::ofstream output;
    output.exceptions(std::ios::badbit | std::ios::failbit);
    output.open(path, std::ios::binary | std::ios::trunc);
    return output;
}

std::ifstream inputFile(const std::filesystem::path& path) {
    std::ifstream input(path, std::ios::binary);
    if (!input.is_open()) throw std::runtime_error("cannot open clock spool input");
    return input;
}
}  // namespace

struct ClockEventSpool::Impl {
    std::filesystem::path directory;
    std::ofstream input;
    const size_t run_records;
    std::vector<char> buffer;

    explicit Impl(size_t records) : run_records(records) { buffer.reserve(MaxRecordBytes); }
    ~Impl() {
        if (input.is_open()) {
            try {
                input.close();
            } catch (...) {
            }
        }
        if (!directory.empty()) {
            std::error_code ignored;
            std::filesystem::remove_all(directory, ignored);
        }
    }

    std::filesystem::path runPath(uint64_t pass, uint64_t number) const {
        return directory / ("run-" + std::to_string(pass) + "-" + std::to_string(number));
    }

    uint64_t makeRuns() {
        auto source = inputFile(directory / "input");
        std::vector<Record> records;
        records.reserve(run_records);
        uint64_t runs = 0;
        size_t bytes = 0;
        const auto finish = [&] {
            std::sort(records.begin(), records.end(), earlier);
            auto output = outputFile(runPath(0, runs++));
            for (const auto& record : records) writeRecord(output, record, buffer);
            output.close();
            records.clear();
            bytes = 0;
        };
        Record record;
        while (const auto size = readRecord(source, record, buffer)) {
            if (!records.empty() && (records.size() == run_records || bytes + size > MaxRunBytes))
                finish();
            bytes += size;
            records.push_back(std::move(record));
            record = Record{};
        }
        if (!records.empty()) finish();
        source.close();
        std::filesystem::remove(directory / "input");
        return runs;
    }

    uint64_t mergePass(uint64_t pass, uint64_t runs) {
        uint64_t outputs = 0;
        for (uint64_t start = 0; start < runs;) {
            const auto count = static_cast<size_t>(std::min<uint64_t>(MergeFanIn, runs - start));
            std::array<std::ifstream, MergeFanIn> files;
            std::array<std::optional<Record>, MergeFanIn> heads;
            for (size_t i = 0; i < count; ++i) {
                files[i] = inputFile(runPath(pass, start + i));
                heads[i].emplace();
                if (!readRecord(files[i], *heads[i], buffer)) heads[i].reset();
            }
            auto output = outputFile(runPath(pass + 1, outputs++));
            for (;;) {
                size_t first = count;
                for (size_t i = 0; i < count; ++i)
                    if (heads[i] && (first == count || earlier(*heads[i], *heads[first])))
                        first = i;
                if (first == count) break;
                writeRecord(output, *heads[first], buffer);
                heads[first].emplace();
                if (!readRecord(files[first], *heads[first], buffer)) heads[first].reset();
            }
            output.close();
            for (size_t i = 0; i < count; ++i) {
                files[i].close();
                std::filesystem::remove(runPath(pass, start + i));
            }
            start += count;
        }
        return outputs;
    }
};

ClockEventSpool::ClockEventSpool(const std::filesystem::path& destination, size_t run_records)
    : impl_(std::make_unique<Impl>(run_records)) {
    if (run_records < 2 || run_records > MaxRunRecords)
        throw std::invalid_argument("clock sort run records must be in [2,8192]");
    // One ID per scratch directory, never an event-order counter on a producer.
    static std::atomic<uint64_t> directories{0};
    const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
    for (;;) {
        std::string scratch_name = ".";
        scratch_name += destination.filename().string();
        scratch_name += ".clock-sort-";
        scratch_name += std::to_string(stamp);
        scratch_name += '-';
        scratch_name += std::to_string(directories.fetch_add(1, std::memory_order_relaxed));
        const auto candidate = destination.parent_path() / scratch_name;
        if (std::filesystem::create_directory(candidate)) {
            impl_->directory = candidate;
            break;
        }
    }
    impl_->input = outputFile(impl_->directory / "input");
}
ClockEventSpool::~ClockEventSpool() = default;
void ClockEventSpool::append(const Record& record) {
    writeRecord(impl_->input, record, impl_->buffer);
}
void ClockEventSpool::flush() {
    if (impl_->input.is_open()) impl_->input.flush();
}
void ClockEventSpool::replay(const std::function<void(const Record&)>& consume) {
    impl_->input.close();
    auto runs = impl_->makeRuns();
    uint64_t pass = 0;
    while (runs > 1) runs = impl_->mergePass(pass++, runs);
    if (runs) {
        auto input = inputFile(impl_->runPath(pass, 0));
        Record record;
        while (readRecord(input, record, impl_->buffer)) {
            consume(record);
            record = Record{};
        }
    }
}

}  // namespace chronon::observe::detail
