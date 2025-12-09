// Graph.hpp
#pragma once

#include <algorithm>
#include <functional>
#include <queue>
#include <unordered_map>
#include <vector>

template <typename A, typename Hash = std::hash<A>,
          typename Eq = std::equal_to<A>>
class Graph {
   public:
    using Vertex = A;

    explicit Graph(bool directed = false) : directed_(directed) {}

    int size() const { return adj_.size(); }

    bool addVertex(const Vertex& v) {
        auto [it, inserted] = adj_.try_emplace(v);
        return inserted;
    }

    bool addEdge(const Vertex& u, const Vertex& v) {
        addVertex(u);
        addVertex(v);

        auto& nbrsU = adj_[u];
        if (std::find(nbrsU.begin(), nbrsU.end(), v) == nbrsU.end()) {
            nbrsU.push_back(v);
        } else {
            return false;  // edge already exists
        }

        if (!directed_) {
            auto& nbrsV = adj_[v];
            if (std::find(nbrsV.begin(), nbrsV.end(), u) == nbrsV.end()) {
                nbrsV.push_back(u);
            }
        }

        return true;
    }

    // ------------------------------
    // Topological sort (Kahn's algorithm)
    // Returns {isDAG, order}
    // - isDAG == false  -> there is a cycle, order is partial
    // ------------------------------
    std::pair<bool, std::vector<Vertex>> topologicalSort() const {
        // indegree[v] = number of incoming edges to v
        std::unordered_map<Vertex, int, Hash, Eq> indegree;

        // 1) Compute indegrees
        for (const auto& [u, neighbors] : adj_) {
            // Make sure every vertex appears in indegree map
            if (indegree.find(u) == indegree.end()) {
                indegree[u] = 0;
            }
            for (const auto& v : neighbors) {
                ++indegree[v];  // if v not present, this creates it with 0 then
                                // increments
            }
        }

        // 2) Put all vertices with indegree 0 into queue
        std::queue<Vertex> q;
        for (const auto& [v, d] : indegree) {
            if (d == 0) {
                q.push(v);
            }
        }

        std::vector<Vertex> order;
        order.reserve(indegree.size());

        // 3) Kahn's algorithm
        while (!q.empty()) {
            Vertex u = q.front();
            q.pop();
            order.push_back(u);

            auto it = adj_.find(u);
            if (it != adj_.end()) {
                for (const auto& v : it->second) {
                    auto itDeg = indegree.find(v);
                    if (itDeg != indegree.end()) {
                        if (--(itDeg->second) == 0) {
                            q.push(v);
                        }
                    }
                }
            }
        }

        bool isDAG = (order.size() == indegree.size());
        return {isDAG, std::move(order)};
    }

   private:
    bool directed_;
    std::unordered_map<Vertex, std::vector<Vertex>, Hash, Eq> adj_;
};
