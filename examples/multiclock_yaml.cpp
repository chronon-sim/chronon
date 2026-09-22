// SPDX-License-Identifier: MPL-2.0
#include "chronon/Chronon.hpp"

using namespace chronon;

class FastUnit : public AutoRegisteredUnit<FastUnit> {
public:
    using ParameterSet = chronon::ParameterSet;
    static constexpr const char* unit_type_name = "FastUnit";
    static constexpr const char* unit_description = "Fast request producer";
    AsyncWritePort<uint64_t> requests{this, "requests"};
    uint64_t issued = 0;
    explicit FastUnit(const ParameterSet*) : AutoRegisteredUnit("fast") {}
    void tick() override {
        if (issued < 100 && requests.send(CdcPacket<uint64_t>{issued + 1, issued + 1})) ++issued;
    }
};

class SlowPeripheral : public AutoRegisteredUnit<SlowPeripheral> {
public:
    using ParameterSet = chronon::ParameterSet;
    static constexpr const char* unit_type_name = "SlowPeripheral";
    static constexpr const char* unit_description = "Slow registered request consumer";
    AsyncReadPort<uint64_t> requests{this, "requests"};
    uint64_t received = 0;
    explicit SlowPeripheral(const ParameterSet*) : AutoRegisteredUnit("peripheral") {}
    void tick() override {
        if (auto packet = requests.take()) {
            if (packet->transaction_id != received + 1 || packet->data != received + 1)
                throw std::runtime_error("unexpected request order or payload");
            ++received;
        }
        const bool read = requests.requestRead();
        if (!requests.outputValid() && !read) sleepForever();
    }
};

int main(int argc, char** argv) {
    return SimulationApp("Fast unit / slow peripheral")
        .setDefaultConfig("multiclock.yaml")
        .setConfigSearchPaths({".", "examples", "../examples", "../../examples"})
        .onPostRun([](const auto& result) {
            const auto* fast = result.template getUnit<FastUnit>("fast");
            const auto* slow = result.template getUnit<SlowPeripheral>("peripheral");
            if (fast->issued != 100 || slow->received != 100 || !result.simulation->cdcDrained())
                throw std::runtime_error("run limit too short to issue and drain 100 requests");
            std::cout << "Validated: 100 requests received in order; CDC drained.\n";
        })
        .run(argc, argv);
}
