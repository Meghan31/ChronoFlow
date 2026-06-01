#pragma once

#include <string>
#include <vector>
#include <chrono>
#include <string_view>

namespace chronoflow {

// ---------------------------------------------------------------------------
// TaskState — lifecycle states of a single task
// ---------------------------------------------------------------------------
enum class TaskState : uint8_t {
    PENDING     = 0,  // registered, waiting for dependencies
    READY       = 1,  // all dependencies satisfied, eligible to run
    RUNNING     = 2,  // currently executing on a worker
    SUCCESS     = 3,  // finished successfully
    FAILED      = 4,  // failed and exhausted retry limit
    RETRY_WAIT  = 5,  // failed, waiting for exponential-backoff delay
    CANCELLED   = 6,  // explicitly cancelled before completion
};

/// Human-readable name for a TaskState value.
[[nodiscard]] std::string_view toString(TaskState state) noexcept;

/// Inverse of toString() — parses the canonical uppercase name.
/// Returns PENDING for unrecognised strings.
[[nodiscard]] TaskState fromString(std::string_view s) noexcept;

// ---------------------------------------------------------------------------
// Task — core scheduling unit
// ---------------------------------------------------------------------------
struct Task {
    // ------------------------------------------------------------------
    // Identity
    // ------------------------------------------------------------------
    std::string task_id;   // globally unique identifier
    std::string command;   // shell command / work descriptor

    // ------------------------------------------------------------------
    // Dependency graph
    // ------------------------------------------------------------------
    std::vector<std::string> dependencies;  // task_ids that must complete first

    // ------------------------------------------------------------------
    // Lifecycle
    // ------------------------------------------------------------------
    TaskState state{TaskState::PENDING};

    // ------------------------------------------------------------------
    // Priority & retry
    // ------------------------------------------------------------------
    int priority{0};       // higher = more urgent
    int retry_count{0};    // number of attempts so far
    int retry_limit{3};    // maximum allowed retries

    // ------------------------------------------------------------------
    // Execution tracking (Phase 2+)
    // ------------------------------------------------------------------
    std::string worker_id;  // id of the worker currently executing this task

    // ------------------------------------------------------------------
    // Timing
    // ------------------------------------------------------------------
    std::chrono::system_clock::time_point created_at{
        std::chrono::system_clock::now()
    };

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    /// Returns true when the task has reached a terminal state.
    [[nodiscard]] bool isTerminal() const noexcept {
        return state == TaskState::SUCCESS   ||
               state == TaskState::FAILED    ||
               state == TaskState::CANCELLED;
    }

    /// Returns true when at least one more retry is allowed.
    [[nodiscard]] bool canRetry() const noexcept {
        return retry_count < retry_limit;
    }
};

} // namespace chronoflow
