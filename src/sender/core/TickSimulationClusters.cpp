// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// SPDX-License-Identifier: MPL-2.0

#include <stdexcept>
#include <unordered_map>
#include <utility>
#include <vector>

#include "TickSimulation.hpp"

namespace chronon::sender {

void TickSimulation::buildSchedulingClusters_() {
    std::unordered_map<Unit*, size_t> indices;
    for (size_t i = 0; i < unit_ptrs_.size(); ++i) indices.emplace(unit_ptrs_[i], i);

    struct Edge {
        size_t source;
        size_t destination;
        size_t headroom;
    };
    std::vector<Edge> edges;
    std::vector<std::pair<size_t, size_t>> co_location;
    for (auto* connection : connections_) {
        const auto source = indices.find(connection->source());
        const auto destination = indices.find(connection->destination());
        if (source == indices.end() || destination == indices.end()) continue;
        // CDC bridges have their own sample/commit actors. Never turn separate
        // hardware domains into one synchronous tick cluster.
        if (connection->source()->clockDomainId() != connection->destination()->clockDomainId()) {
            throw std::logic_error("scheduler connection crosses hardware clock domains");
        }
        const size_t headroom = connection->modelHeadroom();
        edges.push_back({source->second, destination->second, headroom});
        if (headroom == 0) co_location.emplace_back(source->second, destination->second);
    }

    // Reuse the existing delay-zero union-find; extra pairs express scheduling
    // ownership only. The original dependency graph and Connection delays stay
    // intact, including registered feedback and deterministic member order.
    const auto& graph = *dep_graph_.graph();
    clusters_ = findTightCouplingClusters(graph, co_location);

    // A headroom-one edge installs reverse delay zero. Cycles of those waits
    // cannot start asynchronously, but can run as one ordinary per-cycle group.
    // Contract SCCs of this scheduling graph, not SCCs of the data graph. The
    // SCC condensation is acyclic, so one pass closes all transitive merges.
    DirectedGraph zero_slack(clusters_.numClusters());
    for (const auto& edge : edges) {
        const size_t source = clusters_.cluster_id[edge.source];
        const size_t destination = clusters_.cluster_id[edge.destination];
        if (source != destination && edge.headroom == 1) zero_slack.addEdge(destination, source, 0);
    }
    const auto feedback = tarjanSCC(zero_slack);
    bool merged_feedback = false;
    for (const auto& component : feedback.components) {
        if (component.size() < 2) continue;
        const size_t anchor = clusters_.clusters[component.front()].front();
        for (size_t i = 1; i < component.size(); ++i)
            co_location.emplace_back(anchor, clusters_.clusters[component[i]].front());
        merged_feedback = true;
    }
    if (merged_feedback) clusters_ = findTightCouplingClusters(graph, co_location);
    unit_to_cluster_ = clusters_.cluster_id;

    cross_cluster_connections_.clear();
    for (auto* connection : connections_) {
        const auto source = indices.find(connection->source());
        const auto destination = indices.find(connection->destination());
        // Unknown endpoints cannot be proven internal; retain their safety gate.
        if (source == indices.end() || destination == indices.end() ||
            !clusters_.sameCluster(source->second, destination->second))
            cross_cluster_connections_.push_back(connection);
    }
}

}  // namespace chronon::sender
