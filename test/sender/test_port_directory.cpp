// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0

#include <cstdint>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <typeindex>
#include <utility>
#include <vector>

#include "../TestAssertions.hpp"
#include "sender/core/Unit.hpp"
#include "sender/port/InPort.hpp"
#include "sender/port/OutPort.hpp"
#include "sender/port/PortDirectory.hpp"
#include "tree/TreeNode.hpp"

using namespace chronon::sender;

namespace {

class DirectorySource : public Unit {
public:
    explicit DirectorySource(std::string name) : Unit(std::move(name)) {}

    void setCycle(uint64_t cycle) { setLocalCycle(cycle); }

    OutPort<int> int_out{this, "int_out", 4};
    OutPort<std::string> string_out{this, "string_out", 2};
};

class DirectoryDestination : public Unit {
public:
    explicit DirectoryDestination(std::string name) : Unit(std::move(name)) {}

    InPort<int> int_in{this, "int_in"};
    InPort<std::string> string_in{this, "string_in"};
};

template <typename Fn>
void requireRuntimeError(Fn&& fn) {
    bool threw = false;
    try {
        std::forward<Fn>(fn)();
    } catch (const std::runtime_error&) {
        threw = true;
    }
    REQUIRE(threw);
}

void testAutomaticRegistrationLookupAndBinding() {
    auto& directory = PortDirectory::instance();
    directory.clear();

    DirectorySource source("source-unit");
    DirectoryDestination destination("destination-unit");
    chronon::tree::TreeNode source_node("source");
    chronon::tree::TreeNode destination_node("destination");
    source.setTreeNode(&source_node);
    destination.setTreeNode(&destination_node);

    REQUIRE(directory.size() == 4);
    REQUIRE(directory.hasPort("source.int_out"));
    REQUIRE(directory.hasPort("source.string_out"));
    REQUIRE(directory.hasPort("destination.int_in"));
    REQUIRE(directory.hasPort("destination.string_in"));
    REQUIRE(!directory.hasPort("missing.port"));
    REQUIRE(directory.findPort("missing.port") == nullptr);

    const auto keys = directory.keys();
    REQUIRE(std::set<std::string>(keys.begin(), keys.end()) ==
            std::set<std::string>({"source.int_out", "source.string_out", "destination.int_in",
                                   "destination.string_in"}));

    auto* int_out = directory.findPort("source.int_out");
    auto* string_out = directory.findPort("source.string_out");
    auto* int_in = directory.findPort("destination.int_in");
    auto* string_in = directory.findPort("destination.string_in");
    REQUIRE(int_out != nullptr && string_out != nullptr && int_in != nullptr &&
            string_in != nullptr);

    REQUIRE(int_out->isOutPort() && !int_out->isInPort());
    REQUIRE(int_in->isInPort() && !int_in->isOutPort());
    REQUIRE(int_out->dataType() == typeid(int));
    REQUIRE(int_out->dataTypeIndex() == std::type_index(typeid(int)));
    REQUIRE(string_in->dataTypeIndex() == std::type_index(typeid(std::string)));
    REQUIRE(int_out->name() == "int_out");
    REQUIRE(int_out->fullPath() == "source.int_out");
    REQUIRE(int_out->owner() == &source);
    REQUIRE(int_in->owner() == &destination);
    REQUIRE(int_out->portBase() == &source.int_out);
    REQUIRE(int_in->portBase() == &destination.int_in);

    size_t iterated = 0;
    directory.forEach([&](const std::string& path, const IPortHandle& handle) {
        REQUIRE(path == handle.fullPath());
        ++iterated;
    });
    REQUIRE(iterated == 4);

    REQUIRE(int_in->connectTo(int_out, 1) == nullptr);
    REQUIRE(int_out->connectTo(nullptr, 1) == nullptr);
    REQUIRE(int_out->connectTo(string_out, 1) == nullptr);

    auto& binding = PortBindingRegistry::instance();
    ConnectionBase* connection = binding.bind(int_out, int_in, 3);
    REQUIRE(connection != nullptr);
    REQUIRE(connection->source() == &source);
    REQUIRE(connection->destination() == &destination);
    REQUIRE(connection->delay() == 3);
    REQUIRE(source.int_out.connectionCount() == 1);

    source.setCycle(4);
    REQUIRE(source.int_out.send(42));
    REQUIRE(!destination.int_in.tryReceive(6).has_value());
    REQUIRE(destination.int_in.tryReceive(7) == std::optional<int>{42});

    requireRuntimeError([&] { (void)binding.bind(int_in, int_out, 1); });
    requireRuntimeError([&] { (void)binding.bind(int_out, string_out, 1); });
    requireRuntimeError([&] { (void)binding.bind(int_out, string_in, 1); });

    ConnectionBase* string_connection = binding.bind(string_out, string_in, 1);
    REQUIRE(string_connection != nullptr);
    REQUIRE(source.string_out.send(std::string("payload")));
    REQUIRE(destination.string_in.tryReceive(5) == std::optional<std::string>{"payload"});

    directory.clear();
    REQUIRE(directory.size() == 0);
    REQUIRE(directory.keys().empty());
}

}  // namespace

int main() {
    testAutomaticRegistrationLookupAndBinding();
    return chronon::test::failureCount() == 0 ? 0 : 1;
}
