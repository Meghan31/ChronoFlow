#include "dag_engine.h"

namespace chronoflow {

// ---------------------------------------------------------------------------
// Graph construction
// ---------------------------------------------------------------------------

void DAGEngine::addTask(const std::string& task_id,
                        const std::vector<std::string>& dependencies) {
    // Ensure the node exists in both maps even if it has no dependencies.
    if (!in_degree_.contains(task_id)) {
        in_degree_[task_id] = 0;
        adjacency_[task_id];   // default-insert empty adjacency list
    }

    for (const auto& dep : dependencies) {
        // Ensure the dependency node exists.
        if (!in_degree_.contains(dep)) {
            in_degree_[dep] = 0;
            adjacency_[dep];
        }

        // dep → task_id  (dep must complete before task_id)
        adjacency_[dep].push_back(task_id);
        ++in_degree_[task_id];
    }
}

// ---------------------------------------------------------------------------
// Cycle detection — iterative DFS using an explicit recursion stack
// ---------------------------------------------------------------------------

bool DAGEngine::dfs(const std::string& node,
                    std::unordered_set<std::string>& visited,
                    std::unordered_set<std::string>& rec_stack) const {
    visited.insert(node);
    rec_stack.insert(node);

    if (auto it = adjacency_.find(node); it != adjacency_.end()) {
        for (const auto& neighbour : it->second) {
            if (!visited.contains(neighbour)) {
                if (dfs(neighbour, visited, rec_stack)) return true;
            } else if (rec_stack.contains(neighbour)) {
                // Back-edge → cycle detected.
                return true;
            }
        }
    }

    rec_stack.erase(node);
    return false;
}

bool DAGEngine::detectCycle() const {
    std::unordered_set<std::string> visited;
    std::unordered_set<std::string> rec_stack;

    visited.reserve(in_degree_.size());
    rec_stack.reserve(in_degree_.size());

    for (const auto& [task_id, _] : in_degree_) {
        if (!visited.contains(task_id)) {
            if (dfs(task_id, visited, rec_stack)) return true;
        }
    }
    return false;
}

// ---------------------------------------------------------------------------
// Scheduling interface
// ---------------------------------------------------------------------------

std::vector<std::string> DAGEngine::getReadyTasks() const {
    std::vector<std::string> ready;
    for (const auto& [task_id, degree] : in_degree_) {
        if (degree == 0) {
            ready.push_back(task_id);
        }
    }
    return ready;
}

std::vector<std::string> DAGEngine::markComplete(const std::string& task_id) {
    std::vector<std::string> newly_ready;

    auto adj_it = adjacency_.find(task_id);
    if (adj_it == adjacency_.end()) return newly_ready;  // unknown task

    for (const auto& dependent : adj_it->second) {
        auto deg_it = in_degree_.find(dependent);
        if (deg_it == in_degree_.end()) continue;

        if (--(deg_it->second) == 0) {
            newly_ready.push_back(dependent);
        }
    }

    return newly_ready;
}

} // namespace chronoflow
