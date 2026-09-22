// SPDX-License-Identifier: MPL-2.0
// Include endpoint headers first: automatic handles cannot depend on umbrella include order.
#include <unistd.h>

#include <filesystem>
#include <fstream>
#include <functional>
#include <sstream>

#include "../TestAssertions.hpp"
#include "ClockMigrationTestAccess.hpp"
#include "params/Param.hpp"
#include "params/YAMLSerialization.hpp"
#include "sender/app/SimulationApp.hpp"
#include "sender/config/SenderSimulationBuilder.hpp"
#include "sender/config/YAMLOverride.hpp"
#include "sender/port/AsyncFifo.hpp"

using namespace chronon;
using namespace chronon::sender;
using namespace chronon::sender::config;
namespace fs = std::filesystem;

struct ModelParams : params::ParameterSet {
    params::Param<uint64_t> base{this, "base", 0, "payload base"};
    params::Param<bool> lazy{this, "lazy", false, "safe input-driven sleep"};
};

struct YamlSource : factory::AutoRegisteredUnit<YamlSource> {
    using ParameterSet = ModelParams;
    static constexpr const char* unit_type_name = "YamlClockSource";
    static constexpr const char* unit_description = "ordinary source";
    OutPort<uint64_t> out{this, "out"};
    uint64_t base, sent = 0;
    std::vector<uint64_t> events;
    explicit YamlSource(const ModelParams* p) : YamlSource(p->base) {}
    explicit YamlSource(uint64_t b = 0) : AutoRegisteredUnit("source"), base(b) {}
    void tick() override {
        if (localCycle() < 350 && localCycle() % 9 != 4 && out.canSend()) {
            const auto value = base + sent + 1;
            if (out.send(value)) {
                events.insert(events.end(), {localCycle(), value});
                ++sent;
            }
        }
    }
};

static uint64_t messageKey(const uint64_t& value) { return value; }

struct YamlWriter : factory::AutoRegisteredUnit<YamlWriter> {
    using ParameterSet = ModelParams;
    static constexpr const char* unit_type_name = "YamlClockWriter";
    static constexpr const char* unit_description = "CDC writer with ordinary inputs";
    InPort<uint64_t> in{this, "in", 64};
    InPort<uint64_t> single{this, "single", 64};
    AsyncWritePort<uint64_t> out{this, "out"};
    std::optional<uint64_t> pending;
    uint64_t sent = 0, blocked = 0;
    std::vector<uint64_t> events;
    std::function<void(uint64_t)> after_tick;
    explicit YamlWriter(const ModelParams*) : YamlWriter() {}
    YamlWriter() : AutoRegisteredUnit("writer") {}
    void tick() override {
        // Exercise the existing input flush contract at a deterministic local edge.
        if (localCycle() == 80) {
            in.flush<messageKey>(FlushRange::atAndYounger(uint64_t{0}));
            single.flush<messageKey>(FlushRange::atAndYounger(uint64_t{0}));
        }
        if (auto value = single.tryReceive(localCycle()))
            events.insert(events.end(), {localCycle(), 1, *value});
        if (!pending) pending = in.tryReceive(localCycle());
        if (pending) {
            CdcPacket<uint64_t> packet{sent + 1, *pending};
            if (out.send(std::move(packet))) {
                events.insert(events.end(), {localCycle(), 2, *pending});
                pending.reset();
                ++sent;
            } else
                ++blocked;
        }
        if (after_tick) after_tick(localCycle());
    }
};

struct YamlReader : factory::AutoRegisteredUnit<YamlReader> {
    using ParameterSet = ModelParams;
    static constexpr const char* unit_type_name = "YamlClockReader";
    static constexpr const char* unit_description = "CDC reader with optional safe clock gating";
    AsyncReadPort<uint64_t> in{this, "in"};
    std::vector<uint64_t> events;
    bool lazy;
    uint64_t received = 0, stop_after = UINT64_MAX;
    explicit YamlReader(const ModelParams* p) : YamlReader(p->lazy) {}
    explicit YamlReader(bool l = false) : AutoRegisteredUnit("reader"), lazy(l) {}
    void tick() override {
        if (auto packet = in.take()) {
            CHECK(packet->transaction_id == received + 1);
            events.insert(events.end(), {localCycle(), packet->transaction_id, packet->data});
            ++received;
            if (received == stop_after) requestTermination(TerminationReason::Completed);
        }
        const bool read = in.requestRead();
        if (lazy && !in.outputValid() && !read) sleepForever();
    }
};

struct YamlOtherReader : factory::AutoRegisteredUnit<YamlOtherReader> {
    using ParameterSet = params::ParameterSet;
    static constexpr const char* unit_type_name = "YamlOtherReader";
    static constexpr const char* unit_description = "different typed CDC endpoint";
    AsyncReadPort<int> in{this, "in"};
    explicit YamlOtherReader(const ParameterSet*) : AutoRegisteredUnit("other") {}
    void tick() override {}
};

std::string modelYaml(unsigned workers, bool dynamic, bool lazy, unsigned ratio = 0) {
    std::ostringstream out;
    out << "simulation:\n  name: chip\n  num_workers: " << workers
        << "\n  enable_dynamic_rebalance: " << (dynamic ? "true" : "false")
        << "\n  rebalance_check_interval_cycles: 18446744073709551615\n"
        << "  partition_solver: Weighted\n  initial_partition_sync_cost_ns: 0\n"
        << "  clocks:\n    cpu: {frequency_hz: 1000000000}\n    io: {";
    if (ratio == 0) out << "period_s: 1/250000000";
    if (ratio == 1) out << "frequency_hz: 700000000, phase_s: 137/1000000000000";
    if (ratio == 2) out << "frequency_hz: 2000000000/3, phase_s: 1/2000000000";
    out << "}\n  run: {domain_cycles: {clock: cpu, count: 600}}\n"
        << "  unit:\n"
        << "    a:\n      type: YamlClockSource\n      clock: cpu\n      params: {base: 1000}\n"
        << "      port:\n        out:\n          - {to: writer.in, delay: 4, capacity: 64}\n"
        << "          - {to: writer.single, delay: 4, capacity: 64}\n"
        << "    b:\n      type: YamlClockSource\n      clock: cpu\n      params: {base: 2000}\n"
        << "      port: {out: {to: writer.in, delay: 4, capacity: 64}}\n"
        << "    writer:\n      type: YamlClockWriter\n      clock: cpu\n"
        << "      port: {out: {to: reader.in, cdc: {type: async_fifo, depth: 4, "
           "synchronizer_stages: 3}}}\n"
        << "    reader:\n      type: YamlClockReader\n      clock: io\n      params: {lazy: "
        << (lazy ? "true" : "false") << "}\n";
    return out.str();
}

struct Model {
    SenderSimulationBuilder::Result built;
    YamlSource *a, *b;
    YamlWriter* writer;
    YamlReader* reader;
    Model(bool yaml, unsigned workers, bool dynamic, bool lazy, unsigned ratio) {
        if (yaml) {
            built = SenderSimulationBuilder{}.buildFromYAMLString(
                modelYaml(workers, dynamic, lazy, ratio));
            a = dynamic_cast<YamlSource*>(built.unit_map.at("a"));
            b = dynamic_cast<YamlSource*>(built.unit_map.at("b"));
            writer = dynamic_cast<YamlWriter*>(built.unit_map.at("writer"));
            reader = dynamic_cast<YamlReader*>(built.unit_map.at("reader"));
            CHECK(built.ports_registered == 6 && built.connections_made == 4);
        } else {
            // Independent C++ construction, not conversion through the parsed config.
            TickSimulationConfig cfg;
            cfg.num_threads = workers;
            cfg.enable_parallel = workers > 1;
            cfg.enable_dynamic_rebalance = dynamic;
            cfg.rebalance_check_interval_cycles = UINT64_MAX;
            cfg.partition_solver = TickSimulationConfig::PartitionSolverType::Weighted;
            cfg.initial_partition_sync_cost_ns = 0;
            built.simulation = std::make_unique<TickSimulation>(cfg);
            auto& sim = *built.simulation;
            sim.addClockDomain(ClockDomain::fromHz(1, "cpu", 1'000'000'000));
            if (ratio == 0) sim.addClockDomain(ClockDomain(2, "io", SimTime(1, 250'000'000)));
            if (ratio == 1)
                sim.addClockDomain(
                    ClockDomain::fromHz(2, "io", 700'000'000, 1, SimTime::picoseconds(137)));
            if (ratio == 2)
                sim.addClockDomain(
                    ClockDomain::fromHz(2, "io", 2'000'000'000, 3, SimTime(1, 2'000'000'000)));
            a = sim.createUnitInDomain<YamlSource>(1, uint64_t{1000});
            b = sim.createUnitInDomain<YamlSource>(1, uint64_t{2000});
            writer = sim.createUnitInDomain<YamlWriter>(1);
            reader = sim.createUnitInDomain<YamlReader>(2, lazy);
            built.root_node = std::make_unique<tree::TreeNode>("chip");
            const std::vector<std::pair<std::string, Unit*>> units{
                {"a", a}, {"b", b}, {"writer", writer}, {"reader", reader}};
            for (const auto& [name, unit] : units) {
                auto node = std::make_unique<tree::TreeNode>(name, built.root_node.get());
                auto* ptr = node.get();
                built.root_node->addChild(name, std::move(node));
                sim.bindTreeNode(*unit, *ptr);
            }
            sim.connect(a->out, writer->in, 4)->configureRegisteredEdge(64, {});
            sim.connect(a->out, writer->single, 4)->configureRegisteredEdge(64, {});
            sim.connect(b->out, writer->in, 4)->configureRegisteredEdge(64, {});
            sim.connectAsyncFifo(1, writer->out, reader->in, {4, 3});
        }
    }
};

using Digest = std::vector<std::vector<uint64_t>>;
std::vector<std::string> readTrace(const fs::path& dir) {
    std::vector<std::string> lines;
    for (const auto& item : fs::directory_iterator(dir)) {
        const auto name = item.path().filename().string();
        if (!name.starts_with("text-domain-")) continue;
        std::ifstream input(item.path());
        for (std::string line; std::getline(input, line);)
            if (!line.starts_with('#')) lines.push_back(name + ":" + line);
    }
    std::sort(lines.begin(), lines.end());
    return lines;
}

Digest runModel(bool yaml, unsigned workers, bool dynamic, bool lazy, unsigned ratio,
                bool segmented, const fs::path& trace_dir = {}, bool terminate_resume = false) {
    Model model(yaml, workers, dynamic, lazy, ratio);
    auto& sim = *model.built.simulation;
    if (!trace_dir.empty()) {
        observe::ClockTraceRecorder::Config trace;
        trace.output_dir = trace_dir;
        trace.perfetto = false;
        sim.configureClockTrace(trace);
    }
    sim.initialize();
    CHECK(sim.useParallelExecution() == (workers > 1));
    CHECK(model.writer->in.isMultiProducerMode());
    if (workers > 1 && dynamic) CHECK(model.writer->single.usesLockFreeQueue());
    size_t requested = 0;
    if (dynamic && workers > 1) {
        using Access = DynamicMigrationTestAccess;
        const std::vector<size_t> actors{Access::cluster(sim, model.writer),
                                         Access::bridge(sim, 0)};
        model.writer->after_tick = [&, actors](uint64_t cycle) {
            if (requested < actors.size() && cycle >= 20 + 100 * requested &&
                Access::request(sim, actors[requested]))
                ++requested;
        };
    }
    if (terminate_resume) {
        model.reader->stop_after = 5;
        sim.runDomainCycles(1, 600);
        CHECK(sim.wasTerminationRequested());
        const auto& request = sim.terminationRequest();
        CHECK(request.reason == TerminationReason::Completed);
        CHECK(request.physical_time.has_value() && request.settled_time.has_value());
        CHECK(*request.settled_time >= *request.physical_time);
        CHECK(request.physical_time == model.reader->clockDomain().edge(request.cycle));
        CHECK(sim.lastCommittedTime() == *request.settled_time);
        model.reader->stop_after = UINT64_MAX;
        sim.resetTermination();
        sim.runDomainCycles(1, 600 - sim.domainCycleCount(1));
    } else if (segmented) {
        sim.runClockEvents(37);
        sim.runUntilTime(SimTime::nanoseconds(100));
        sim.runDomainCycles(1, 600 - sim.domainCycleCount(1));
    } else
        sim.runDomainCycles(1, 600);
    CHECK(sim.domainCycleCount(1) == 600);
    CHECK(model.writer->blocked > 0 && model.reader->received > 0);
    if (dynamic && workers > 1) CHECK(requested == 2 && sim.rebalanceCount() == 2);
    sim.runUntilTime(SimTime::nanoseconds(8000));
    sim.drainCdc(1000);
    CHECK(sim.cdcDrained());
    CHECK(model.writer->sent == model.reader->received);
    CHECK(sim.totalTransportOverflowEvents() == 0);
    sim.closeClockTrace();
    return {model.a->events,
            model.b->events,
            model.writer->events,
            model.reader->events,
            {sim.domainCycleCount(1), sim.domainCycleCount(2), sim.schedulerSteps(),
             model.writer->blocked}};
}

template <typename F>
void rejects(F&& f, const std::string& text) {
    try {
        f();
    } catch (const std::exception& error) {
        CHECK(std::string(error.what()).find(text) != std::string::npos);
        return;
    }
    throw std::runtime_error("expected rejection containing: " + text);
}

void validation() {
    auto base = YAML::Load(modelYaml(1, false, false));
    const auto invalid = [&](const std::function<void(YAML::Node&)>& edit, const char* text) {
        auto node = YAML::Clone(base);
        edit(node);
        rejects([&] { SenderSimulationBuilder{}.buildFromYAMLNode(node); }, text);
    };
    invalid([](auto& n) { n["simulation"]["clocks"]["cpu"]["frequency_hz"] = "1.5"; },
            "unsigned decimal integer");
    invalid(
        [](auto& n) { n["simulation"]["clocks"]["cpu"]["frequency_hz"] = "18446744073709551616"; },
        "uint64");
    invalid([](auto& n) { n["simulation"]["clocks"]["cpu"]["frequency_hz"] = "1/0"; },
            "denominator");
    invalid([](auto& n) { n["simulation"]["clocks"]["cpu"]["period_s"] = "1/3"; }, "exactly one");
    invalid([](auto& n) { n["simulation"]["clocks"]["cpu"]["phase_s"] = "-1"; }, "unsigned");
    invalid([](auto& n) { n["simulation"]["unit"]["a"]["clock"] = "typo"; },
            "unit 'a': unknown clock");
    invalid([](auto& n) { n["simulation"]["unit"]["a"]["clock"] = "io"; },
            "ordinary connection crosses");
    invalid([](auto& n) { n["simulation"]["unit"]["writer"]["port"]["out"]["delay"] = 1; },
            "CDC cannot be combined");
    invalid([](auto& n) { n["simulation"]["unit"]["writer"]["port"]["out"].remove("cdc"); },
            "async endpoints require");
    invalid([](auto& n) { n["simulation"]["unit"]["writer"]["port"]["out"]["cdc"]["depth"] = 3; },
            "power of two");
    invalid(
        [](auto& n) {
            n["simulation"]["unit"]["writer"]["port"]["out"]["cdc"]["synchronizer_stages"] = 1;
        },
        "[2, 64]");
    invalid(
        [](auto& n) {
            n["simulation"]["unit"]["reader"]["type"] = "YamlOtherReader";
            n["simulation"]["unit"]["reader"].remove("params");
        },
        "Type mismatch");
    invalid([](auto& n) { n["simulation"]["run_cycles"] = 10; }, "cannot combine");
    invalid([](auto& n) { n["simulation"].remove("run"); }, "requires an explicit");
    invalid([](auto& n) { n["simulation"]["run"]["event_batches"] = 10; }, "exactly one");
    invalid([](auto& n) { n["simulation"]["run"]["domain_cycles"]["clock"] = "missing"; },
            "unknown clock");
    invalid([](auto& n) { n["simulation"]["run"]["domain_cycles"]["clock"] = "default"; },
            "no units");
    rejects(
        [] {
            SenderConfigLoader{}.loadFromString(
                "simulation: {clocks: {cpu: {frequency_hz: 1}, cpu: {frequency_hz: 2}}, run: "
                "{event_batches: 1}}");
        },
        "duplicate");
    rejects(
        [] {
            SenderConfigLoader{}.loadFromString(
                "simulation: {unit: {a: {type: YamlClockSource, clock: default}}}");
        },
        "requires simulation.clocks");
    invalid(
        [](auto& n) {
            auto port = n["simulation"]["unit"]["writer"]["port"];
            auto connection = YAML::Clone(port["out"]);
            port["out"] = YAML::Node(YAML::NodeType::Sequence);
            port["out"].push_back(connection);
            port["out"].push_back(connection);
        },
        "owned and unbound");
    invalid(
        [](auto& n) {
            n["simulation"]["unit"]["a"]["port"]["out"] =
                YAML::Load("{to: reader.in, cdc: {type: async_fifo}}");
        },
        "cdc requires AsyncWritePort");
    invalid(
        [](auto& n) {
            n["simulation"]["unit"]["writer"]["port"]["out"] =
                YAML::Load("{to: reader.in, cdc: {type: async_fifo}, extra: 1}");
        },
        "unknown field");
    invalid(
        [](auto& n) {
            n["simulation"]["unit"]["writer"]["port"]["out"] =
                YAML::Load("{to: reader.in, to: a.out, cdc: {type: async_fifo}}");
        },
        "duplicate");
    auto node = YAML::Clone(base);
    YAMLOverride::applyOverride(node, "simulation.unit.a.params.literal={still: literal}");
    CHECK(node["simulation"]["unit"]["a"]["params"]["literal"].as<std::string>() ==
          "{still: literal}");
    YAMLOverride::applyOverride(node, "simulation.run={until_time_s: 1/1000}");
    CHECK(SenderConfigLoader{}.loadFromNode(node).run->until_time == SimTime(1, 1000));
    YAMLOverride::applyOverride(node, "simulation.clocks.cpu.frequency_hz=18446744073709551615");
    CHECK(SenderConfigLoader{}.loadFromNode(node).clocks[0].period() == SimTime(1, UINT64_MAX));
    YAMLOverride::applyOverride(node, "simulation.clocks.cpu.frequency_hz=1.0");
    rejects([&] { SenderConfigLoader{}.loadFromNode(node); }, "unsigned decimal integer");
    YAMLOverride::applyOverride(node, "simulation.clocks.cpu.frequency_hz=1000000000");
    YAMLOverride::applyOverride(node, "simulation.run={event_batches: 9}");
    CHECK(SenderConfigLoader{}.loadFromNode(node).run->count == 9);
}

void appRuns(const fs::path& dir) {
    fs::create_directories(dir);
    auto node = YAML::Load(modelYaml(2, false, true));
    const auto config_path = (dir / "model.yaml").string();
    {
        std::ofstream file(config_path);
        file << node;
    }
    for (const auto& limit :
         {std::string("simulation.run={event_batches: 42}"),
          std::string("simulation.run={until_time_s: 1/10000000}"),
          std::string("simulation.run={domain_cycles: {clock: cpu, count: 123}}")}) {
        std::vector<std::string> args{"app", config_path, "-p", limit};
        std::vector<char*> argv;
        for (auto& arg : args) argv.push_back(arg.data());
        bool called = false;
        SimulationApp app("clock app test");
        app.onPostRun([&](const auto& result) {
            called = true;
            CHECK(result.cycles_executed == 0 && result.event_batches_executed > 0);
            if (limit.find("42") != std::string::npos) CHECK(result.event_batches_executed == 42);
            if (limit.find("123") != std::string::npos)
                CHECK(result.simulation->domainCycleCount(1) == 123);
            if (limit.find("until_time_s") != std::string::npos)
                CHECK(result.simulation->lastCommittedTime() < SimTime(1, 10000000));
        });
        CHECK(app.run(static_cast<int>(argv.size()), argv.data()) == 0 && called);
    }
}

int main() {
    validation();
    const fs::path dir =
        fs::temp_directory_path() / ("chronon-multiclock-yaml-" + std::to_string(getpid()));
    fs::remove_all(dir);
    for (unsigned ratio = 0; ratio < 3; ++ratio) {
        const auto reference = runModel(false, 1, false, false, ratio, false);
        for (unsigned mode = 0; mode < 3; ++mode)
            for (bool lazy : {false, true}) {
                const auto actual = runModel(true, mode ? 4 : 1, mode == 2, lazy, ratio, true);
                CHECK(actual == reference);
            }
    }
    CHECK(runModel(false, 4, true, true, 1, true, dir / "direct") ==
          runModel(true, 4, true, true, 1, true, dir / "yaml"));
    CHECK(readTrace(dir / "direct") == readTrace(dir / "yaml"));
    const auto uninterrupted = runModel(false, 1, false, true, 0, false);
    CHECK(runModel(true, 1, false, true, 0, false, {}, true) == uninterrupted);
    CHECK(runModel(true, 4, false, true, 0, false, {}, true) == uninterrupted);
    appRuns(dir);
    fs::remove_all(dir);
    std::cout
        << "Multi-clock YAML validation, equivalence, migration, tracing and app limits passed\n";
}
