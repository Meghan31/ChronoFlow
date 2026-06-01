#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>

namespace chronoflow {

// ---------------------------------------------------------------------------
// DAGEngine
//
// Tracks a directed acyclic graph of tasks and their dependencies.
//
// Nodes are task_ids (strings).  An edge  A → B  means "A must complete
// before B can run" (A is a dependency of B).
//
// Internally the engine stores:
//   • adjacency_  – forward edges: task → list of tasks that depend on it
//   • in_degree_  – count of unsatisfied dependencies per task
//
// Callers drive the lifecycle:
//   1. addTask()        — register a node and its dependency edges
//   2. detectCycle()    — validate the graph before scheduling begins
//   3. getReadyTasks()  — seed the initial ready set (in-degree == 0)
//   4. markComplete()   — called when a task finishes; returns newly ready
// ---------------------------------------------------------------------------
class DAGEngine {
public:
    DAGEngine() = default;

    // ------------------------------------------------------------------
    // Graph construction
    // ------------------------------------------------------------------

    /// Register task `task_id` with its list of prerequisite task_ids.
    /// Prerequisites must already have been added (or will be added later
    /// before detectCycle() / getReadyTasks() are called).
    ///
    /// If the task_id already exists, the call is a no-op (idempotent).
    void addTask(const std::string& task_id,
                 const std::vector<std::string>& dependencies);

    // ------------------------------------------------------------------
    // Validation
    // ------------------------------------------------------------------

    /// DFS-based cycle detection.  Returns true if any cycle is present.
    /// Must be called after all tasks have been added and before scheduling.
    [[nodiscard]] bool detectCycle() const;

    // ------------------------------------------------------------------
    // Scheduling interface
    // ------------------------------------------------------------------

    /// Returns all tasks whose in-degree is currently zero (ready to run).
    [[nodiscard]] std::vector<std::string> getReadyTasks() const;

    /// Mark `task_id` as completed.  Decrements the in-degree of every task
    /// that depended on it and returns the subset whose in-degree just
    /// reached zero (i.e. they are now ready to run).
    ///
    /// Returns an empty vector if task_id has no dependents or is unknown.
    [[nodiscard]] std::vector<std::string> markComplete(const std::string& task_id);

    // ------------------------------------------------------------------
    // Observers
    // ------------------------------------------------------------------

    [[nodiscard]] std::size_t task_count() const noexcept {
        return in_degree_.size();
    }

    [[nodiscard]] bool contains(const std::string& task_id) const {
        return in_degree_.contains(task_id);
    }

private:
    // Helpers for cycle detection.
    bool dfs(const std::string& node,
             std::unordered_set<std::string>& visited,
             std::unordered_set<std::string>& rec_stack) const;

    // forward edges: dependency → dependents
    std::unordered_map<std::string, std::vector<std::string>> adjacency_;

    // unsatisfied dependency count per task
    std::unordered_map<std::string, int> in_degree_;
};

} // namespace chronoflow
