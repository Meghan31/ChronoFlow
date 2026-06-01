#pragma once

// NOTE: Requires SQLite3 development headers.
//   Ubuntu/Debian:  apt install libsqlite3-dev
//   CMake:          find_package(SQLite3 REQUIRED)
//                   target_link_libraries(... SQLite::SQLite3)

#include "../../include/task.h"

#include <mutex>
#include <string>
#include <utility>
#include <vector>

struct sqlite3;       // forward-declare the opaque SQLite3 handle
struct sqlite3_stmt;  // forward-declare prepared statement handle

namespace chronoflow {

// ---------------------------------------------------------------------------
// MetadataStore
//
// Thin SQLite3 wrapper providing persistent storage for the scheduler's
// task graph.  All public methods are thread-safe.
//
// Schema
// ------
//   tasks(
//       task_id     TEXT PRIMARY KEY,
//       command     TEXT NOT NULL,
//       state       TEXT NOT NULL DEFAULT 'PENDING',
//       priority    INTEGER NOT NULL DEFAULT 0,
//       retry_count INTEGER NOT NULL DEFAULT 0,
//       retry_limit INTEGER NOT NULL DEFAULT 3,
//       worker_id   TEXT NOT NULL DEFAULT ''
//   )
//
//   dependencies(
//       parent_task TEXT NOT NULL,   -- prerequisite
//       child_task  TEXT NOT NULL,   -- task that depends on parent
//       PRIMARY KEY (parent_task, child_task)
//   )
//
// Usage
// -----
//   MetadataStore store{"chronoflow.db"};
//   store.upsertTask(task);
//   store.storeDependencies(task.task_id, task.dependencies);
//   store.updateTaskState(task.task_id, TaskState::RUNNING);
//   store.updateWorker(task.task_id, worker_id);
//
//   auto tasks = store.getAllTasks();
//   auto deps  = store.getDependencies();
// ---------------------------------------------------------------------------
class MetadataStore {
public:
    /// Open (or create) the SQLite database at db_path and initialise schema.
    /// Use ":memory:" for an ephemeral in-process database (useful for tests).
    explicit MetadataStore(const std::string& db_path);

    ~MetadataStore();

    MetadataStore(const MetadataStore&)            = delete;
    MetadataStore& operator=(const MetadataStore&) = delete;

    // ------------------------------------------------------------------
    // Write operations
    // ------------------------------------------------------------------

    /// Insert or update all columns of a task row.
    void upsertTask(const Task& task);

    /// Store dependency edges for a task (parent → task_id for each dep).
    /// Uses INSERT OR IGNORE — safe to call multiple times.
    void storeDependencies(const std::string& task_id,
                           const std::vector<std::string>& dependencies);

    /// Update the state column only (used on every state transition).
    void updateTaskState(const std::string& task_id, TaskState state);

    /// Update the worker_id column when a task is dispatched.
    void updateWorker(const std::string& task_id, const std::string& worker_id);

    // ------------------------------------------------------------------
    // Read operations (used by recoverFromDB)
    // ------------------------------------------------------------------

    /// Retrieve every task row and reconstruct Task objects.
    [[nodiscard]] std::vector<Task> getAllTasks();

    /// Retrieve every dependency edge as (parent_task, child_task) pairs.
    [[nodiscard]] std::vector<std::pair<std::string, std::string>>
    getDependencies();

private:
    void initialize();
    void exec(const char* sql);   // execute DDL / pragma; logs + throws on error

    sqlite3*           db_{nullptr};
    mutable std::mutex mutex_;
};

} // namespace chronoflow
