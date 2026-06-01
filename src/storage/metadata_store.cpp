#include "metadata_store.h"

#include <spdlog/spdlog.h>
#include <sqlite3.h>
#include <stdexcept>

namespace chronoflow {

// ---------------------------------------------------------------------------
// RAII helpers
// ---------------------------------------------------------------------------
namespace {

// Automatically finalises a prepared statement on scope exit.
struct StmtGuard {
    sqlite3_stmt* s{nullptr};
    ~StmtGuard() { if (s) sqlite3_finalize(s); }
};

} // anonymous namespace

// ---------------------------------------------------------------------------
// Construction / destruction
// ---------------------------------------------------------------------------

MetadataStore::MetadataStore(const std::string& db_path) {
    const int rc = sqlite3_open(db_path.c_str(), &db_);
    if (rc != SQLITE_OK) {
        const std::string msg = db_ ? sqlite3_errmsg(db_) : "unknown error";
        sqlite3_close(db_);
        db_ = nullptr;
        throw std::runtime_error("[MetadataStore] Failed to open '" +
                                 db_path + "': " + msg);
    }

    // WAL mode: better concurrent read/write performance.
    exec("PRAGMA journal_mode=WAL;");
    exec("PRAGMA synchronous=NORMAL;");

    initialize();
    spdlog::info("[MetadataStore] Opened database '{}'", db_path);
}

MetadataStore::~MetadataStore() {
    if (db_) {
        sqlite3_close(db_);
        db_ = nullptr;
    }
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

void MetadataStore::exec(const char* sql) {
    char* errmsg = nullptr;
    const int rc = sqlite3_exec(db_, sql, nullptr, nullptr, &errmsg);
    if (rc != SQLITE_OK) {
        const std::string msg = errmsg ? errmsg : "unknown";
        sqlite3_free(errmsg);
        throw std::runtime_error(
            std::string("[MetadataStore] exec failed: ") + msg);
    }
}

void MetadataStore::initialize() {
    exec(R"SQL(
        CREATE TABLE IF NOT EXISTS tasks (
            task_id     TEXT PRIMARY KEY,
            command     TEXT NOT NULL,
            state       TEXT NOT NULL DEFAULT 'PENDING',
            priority    INTEGER NOT NULL DEFAULT 0,
            retry_count INTEGER NOT NULL DEFAULT 0,
            retry_limit INTEGER NOT NULL DEFAULT 3,
            worker_id   TEXT NOT NULL DEFAULT ''
        );
    )SQL");

    exec(R"SQL(
        CREATE TABLE IF NOT EXISTS dependencies (
            parent_task TEXT NOT NULL,
            child_task  TEXT NOT NULL,
            PRIMARY KEY (parent_task, child_task)
        );
    )SQL");

    exec("CREATE INDEX IF NOT EXISTS idx_dep_child "
         "ON dependencies(child_task);");

    spdlog::debug("[MetadataStore] Schema initialised");
}

// ---------------------------------------------------------------------------
// Write operations
// ---------------------------------------------------------------------------

void MetadataStore::upsertTask(const Task& task) {
    static const char* kSQL =
        "INSERT OR REPLACE INTO tasks "
        "(task_id, command, state, priority, retry_count, retry_limit, worker_id) "
        "VALUES (?, ?, ?, ?, ?, ?, ?);";

    std::scoped_lock lock{mutex_};

    StmtGuard g;
    if (sqlite3_prepare_v2(db_, kSQL, -1, &g.s, nullptr) != SQLITE_OK) {
        spdlog::error("[MetadataStore] upsertTask prepare failed: {}",
                      sqlite3_errmsg(db_));
        return;
    }

    const std::string state_str{toString(task.state)};
    sqlite3_bind_text(g.s, 1, task.task_id.c_str(),   -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(g.s, 2, task.command.c_str(),    -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(g.s, 3, state_str.c_str(),       -1, SQLITE_TRANSIENT);
    sqlite3_bind_int (g.s, 4, task.priority);
    sqlite3_bind_int (g.s, 5, task.retry_count);
    sqlite3_bind_int (g.s, 6, task.retry_limit);
    sqlite3_bind_text(g.s, 7, task.worker_id.c_str(), -1, SQLITE_TRANSIENT);

    if (sqlite3_step(g.s) != SQLITE_DONE) {
        spdlog::error("[MetadataStore] upsertTask step failed: {}",
                      sqlite3_errmsg(db_));
        return;
    }

    spdlog::debug("[MetadataStore] Upserted task '{}'", task.task_id);
}

void MetadataStore::storeDependencies(const std::string& task_id,
                                       const std::vector<std::string>& dependencies) {
    if (dependencies.empty()) return;

    static const char* kSQL =
        "INSERT OR IGNORE INTO dependencies (parent_task, child_task) "
        "VALUES (?, ?);";

    std::scoped_lock lock{mutex_};

    StmtGuard g;
    if (sqlite3_prepare_v2(db_, kSQL, -1, &g.s, nullptr) != SQLITE_OK) {
        spdlog::error("[MetadataStore] storeDependencies prepare failed: {}",
                      sqlite3_errmsg(db_));
        return;
    }

    for (const auto& parent : dependencies) {
        sqlite3_bind_text(g.s, 1, parent.c_str(),   -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(g.s, 2, task_id.c_str(),  -1, SQLITE_TRANSIENT);

        if (sqlite3_step(g.s) != SQLITE_DONE) {
            spdlog::error("[MetadataStore] storeDependencies insert failed: {}",
                          sqlite3_errmsg(db_));
        }
        sqlite3_reset(g.s);
    }

    spdlog::debug("[MetadataStore] Stored {} dep(s) for task '{}'",
                  dependencies.size(), task_id);
}

void MetadataStore::updateTaskState(const std::string& task_id, TaskState state) {
    static const char* kSQL =
        "UPDATE tasks SET state = ? WHERE task_id = ?;";

    std::scoped_lock lock{mutex_};

    StmtGuard g;
    if (sqlite3_prepare_v2(db_, kSQL, -1, &g.s, nullptr) != SQLITE_OK) {
        spdlog::error("[MetadataStore] updateTaskState prepare failed: {}",
                      sqlite3_errmsg(db_));
        return;
    }

    const std::string state_str{toString(state)};
    sqlite3_bind_text(g.s, 1, state_str.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(g.s, 2, task_id.c_str(),   -1, SQLITE_TRANSIENT);

    if (sqlite3_step(g.s) != SQLITE_DONE) {
        spdlog::error("[MetadataStore] updateTaskState failed for '{}': {}",
                      task_id, sqlite3_errmsg(db_));
        return;
    }

    spdlog::debug("[MetadataStore] Task '{}' state → {}", task_id, state_str);
}

void MetadataStore::updateWorker(const std::string& task_id,
                                  const std::string& worker_id) {
    static const char* kSQL =
        "UPDATE tasks SET worker_id = ? WHERE task_id = ?;";

    std::scoped_lock lock{mutex_};

    StmtGuard g;
    if (sqlite3_prepare_v2(db_, kSQL, -1, &g.s, nullptr) != SQLITE_OK) {
        spdlog::error("[MetadataStore] updateWorker prepare failed: {}",
                      sqlite3_errmsg(db_));
        return;
    }

    sqlite3_bind_text(g.s, 1, worker_id.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(g.s, 2, task_id.c_str(),   -1, SQLITE_TRANSIENT);

    if (sqlite3_step(g.s) != SQLITE_DONE) {
        spdlog::error("[MetadataStore] updateWorker failed for '{}': {}",
                      task_id, sqlite3_errmsg(db_));
        return;
    }

    spdlog::debug("[MetadataStore] Task '{}' assigned to worker '{}'",
                  task_id, worker_id);
}

// ---------------------------------------------------------------------------
// Read operations
// ---------------------------------------------------------------------------

std::vector<Task> MetadataStore::getAllTasks() {
    static const char* kSQL =
        "SELECT task_id, command, state, priority, retry_count, retry_limit, "
        "worker_id FROM tasks;";

    std::scoped_lock lock{mutex_};

    StmtGuard g;
    if (sqlite3_prepare_v2(db_, kSQL, -1, &g.s, nullptr) != SQLITE_OK) {
        spdlog::error("[MetadataStore] getAllTasks prepare failed: {}",
                      sqlite3_errmsg(db_));
        return {};
    }

    std::vector<Task> result;
    while (sqlite3_step(g.s) == SQLITE_ROW) {
        Task t;
        // Safely read TEXT columns (may be NULL in edge cases)
        auto col_text = [&](int col) -> std::string {
            const char* p = reinterpret_cast<const char*>(
                sqlite3_column_text(g.s, col));
            return p ? p : "";
        };

        t.task_id     = col_text(0);
        t.command     = col_text(1);
        t.state       = fromString(col_text(2));
        t.priority    = sqlite3_column_int(g.s, 3);
        t.retry_count = sqlite3_column_int(g.s, 4);
        t.retry_limit = sqlite3_column_int(g.s, 5);
        t.worker_id   = col_text(6);

        result.push_back(std::move(t));
    }

    spdlog::debug("[MetadataStore] Loaded {} task(s) from DB", result.size());
    return result;
}

std::vector<std::pair<std::string, std::string>>
MetadataStore::getDependencies() {
    static const char* kSQL =
        "SELECT parent_task, child_task FROM dependencies;";

    std::scoped_lock lock{mutex_};

    StmtGuard g;
    if (sqlite3_prepare_v2(db_, kSQL, -1, &g.s, nullptr) != SQLITE_OK) {
        spdlog::error("[MetadataStore] getDependencies prepare failed: {}",
                      sqlite3_errmsg(db_));
        return {};
    }

    std::vector<std::pair<std::string, std::string>> result;
    while (sqlite3_step(g.s) == SQLITE_ROW) {
        auto col = [&](int c) -> std::string {
            const char* p = reinterpret_cast<const char*>(
                sqlite3_column_text(g.s, c));
            return p ? p : "";
        };
        result.emplace_back(col(0), col(1));
    }

    spdlog::debug("[MetadataStore] Loaded {} dependency edge(s) from DB",
                  result.size());
    return result;
}

} // namespace chronoflow
