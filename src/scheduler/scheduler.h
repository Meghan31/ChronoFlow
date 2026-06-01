#pragma once

#include "dag_engine.h"
#include "health_monitor.h"
#include "retry_manager.h"
#include "worker_registry.h"
#include "../../include/task.h"
#include "../../include/concurrent_queue.h"
#include "../common/thread_pool.h"
#include "../storage/metadata_store.h"

#include <atomic>
#include <chrono>
#include <deque>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace chronoflow {

// ---------------------------------------------------------------------------
// Scheduler  (Phase 3 + 4 — fault tolerance + persistence)
//
// Phase 3 additions
// -----------------
//   RetryManager  retry_manager_    — replaces ad-hoc retry_queue + RetryThread
//   HealthMonitor health_monitor_   — replaces old HealthMonitorThread
//   requeueWorkerTasks()            — called by HealthMonitor on worker failure
//
// Phase 4 additions
// -----------------
//   MetadataStore metadata_store_   — SQLite persistence
//   recoverFromDB()                 — called at start() before threads launch
//
// Thread layout (Phase 3+)
// ------------------------
//   IntakeThread       — DAG registration, persistence, ready-set seeding
//   DispatcherThread   — dispatch to worker (or local fallback), DB update
//   CompletionThread   — DAG advancement, RetryManager hand-off, DB update
//   RetryManager       — owns its own thread (replaces RetryThread)
//   HealthMonitor      — owns its own thread (replaces HealthMonitorThread)
//
// Concurrency model
// -----------------
//   state_mutex_  (shared_mutex)  — read/write guards on tasks_ map
//   dag_mutex_    (mutex)         — serialises DAGEngine access
//   MetadataStore — has its own internal mutex
// ---------------------------------------------------------------------------
class Scheduler {
public:
    // ------------------------------------------------------------------
    // TaskInfo — richer query result (used by gRPC service layer)
    // ------------------------------------------------------------------
    struct TaskInfo {
        TaskState state;
        int       retry_count{0};
    };

    struct MetricsSnapshot {
        std::size_t total_tasks{0};
        std::size_t pending_tasks{0};
        std::size_t ready_tasks{0};
        std::size_t running_tasks{0};
        std::size_t retry_wait_tasks{0};
        std::size_t success_tasks{0};
        std::size_t failed_tasks{0};
        std::size_t cancelled_tasks{0};
        std::size_t workers_total{0};
        std::size_t workers_alive{0};
        std::size_t completions_total{0};
        std::size_t completions_last_minute{0};
    };

    struct HealthSnapshot {
        bool        scheduler_running{false};
        std::size_t workers_total{0};
        std::size_t workers_alive{0};
        std::size_t task_total{0};
        long long   uptime_seconds{0};
    };

    // ------------------------------------------------------------------
    // Construction / destruction
    // ------------------------------------------------------------------

    /// @param base_delay_ms    Base for exponential backoff (unused by
    ///                         RetryManager which has its own 2000 ms base,
    ///                         but kept for local-fallback compatibility).
    /// @param dispatch_threads Pool size for async gRPC dispatch calls.
    /// @param db_path          SQLite database path.  Use ":memory:" for tests.
    explicit Scheduler(
        std::chrono::milliseconds base_delay_ms   = std::chrono::milliseconds{1000},
        std::size_t               dispatch_threads = 16,
        std::string               db_path          = "chronoflow.db");

    ~Scheduler();

    Scheduler(const Scheduler&)            = delete;
    Scheduler& operator=(const Scheduler&) = delete;
    Scheduler(Scheduler&&)                 = delete;
    Scheduler& operator=(Scheduler&&)      = delete;

    // ------------------------------------------------------------------
    // Task API
    // ------------------------------------------------------------------

    void submit(Task task);
    void start();
    void stop();

    [[nodiscard]] TaskState taskState(const std::string& task_id) const;
    [[nodiscard]] TaskInfo  taskInfo (const std::string& task_id) const;

    [[nodiscard]] std::vector<Task> listTasks() const;
    [[nodiscard]] std::vector<WorkerRegistry::WorkerSnapshot> listWorkers() const;
    [[nodiscard]] MetricsSnapshot metricsSnapshot() const;
    [[nodiscard]] HealthSnapshot healthSnapshot() const;

    /// Cancel a task in PENDING, READY, or RETRY_WAIT state.
    bool cancelTask(const std::string& task_id);

    // ------------------------------------------------------------------
    // Worker registry API  (called by SchedulerServiceImpl)
    // ------------------------------------------------------------------

    void registerWorker(const std::string& worker_id, const std::string& address);
    void updateWorkerHeartbeat(const std::string& worker_id, int active_tasks);

private:
    // ------------------------------------------------------------------
    // Thread entry points (3 dedicated threads remain)
    // ------------------------------------------------------------------
    void intakeThread();
    void dispatcherThread();
    void completionThread();

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------
    void setState(const std::string& task_id, TaskState new_state);

    [[nodiscard]] std::chrono::milliseconds
    backoffDelay(int retry_count) const noexcept;

    void dispatchToWorker(Task task, WorkerRegistry::DispatchHandle handle);

    /// Phase 3: collect all RUNNING tasks on dead worker, reset to READY,
    /// push to ready_queue_.  Called from HealthMonitor's callback.
    void requeueWorkerTasks(const std::string& worker_id);

    /// Phase 4: reconstruct state from MetadataStore on startup.
    void recoverFromDB();

    void recordCompletion();

    // ------------------------------------------------------------------
    // Queues
    // ------------------------------------------------------------------
    ConcurrentQueue<Task>             intake_queue_;
    ConcurrentQueue<Task>             ready_queue_;

    struct CompletionRecord { Task task; int exit_code; };
    ConcurrentQueue<CompletionRecord> completion_queue_;

    // ------------------------------------------------------------------
    // DAG engine
    // ------------------------------------------------------------------
    mutable std::mutex dag_mutex_;
    DAGEngine          dag_engine_;

    // ------------------------------------------------------------------
    // Task state map
    // ------------------------------------------------------------------
    mutable std::shared_mutex             state_mutex_;
    std::unordered_map<std::string, Task> tasks_;

    // ------------------------------------------------------------------
    // Phase 2 additions
    // ------------------------------------------------------------------
    WorkerRegistry worker_registry_;
    ThreadPool     dispatch_pool_;

    // ------------------------------------------------------------------
    // Phase 3 + 4 additions
    // (declared after queues so lambdas may safely reference them)
    // ------------------------------------------------------------------
    MetadataStore metadata_store_;
    RetryManager  retry_manager_;
    HealthMonitor health_monitor_;

    // ------------------------------------------------------------------
    // Configuration
    // ------------------------------------------------------------------
    std::chrono::milliseconds base_delay_;

    std::chrono::steady_clock::time_point start_time_{
        std::chrono::steady_clock::now()};

    std::atomic<std::size_t> completions_total_{0};
    mutable std::mutex completions_mutex_;
    mutable std::deque<std::chrono::steady_clock::time_point> completions_;

    // ------------------------------------------------------------------
    // Lifecycle
    // ------------------------------------------------------------------
    std::atomic<bool> running_{false};
    std::atomic<bool> stop_{true};

    std::thread intake_thread_;
    std::thread dispatcher_thread_;
    std::thread completion_thread_;
    // NOTE: No separate retry_thread_ or health_thread_ —
    //       RetryManager and HealthMonitor manage their own threads.
};

} // namespace chronoflow
