#pragma once

#include <atomic>
#include <chrono>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>

namespace chronoflow {

// ---------------------------------------------------------------------------
// HealthMonitor
//
// Runs on its own thread and detects worker failures by monitoring the
// elapsed time since each worker's last heartbeat.
//
// Lifecycle
// ---------
//   1. Scheduler calls registerWorker() when a worker connects.
//   2. Scheduler calls receiveHeartbeat() on every WorkerHeartbeat RPC.
//   3. Every check_interval (default 3 s) the background thread scans all
//      tracked workers.  Any worker whose last heartbeat is older than
//      timeout (default 9 s) is declared dead.
//   4. on_worker_dead is invoked with the dead worker_id.  The callback
//      (provided by the Scheduler) is expected to:
//        – scan the task state map for RUNNING tasks on that worker
//        – reset those tasks to READY and push them to ready_queue
//        – call worker_registry_.markDead()
//   5. The dead worker is removed from HealthMonitor's tracking map.
//      If it reconnects, the next RegisterWorker call re-adds it.
//
// Thread safety
// -------------
// receiveHeartbeat() and registerWorker() are safe to call from any thread
// concurrently.  on_worker_dead is invoked from the monitor thread only.
// ---------------------------------------------------------------------------
class HealthMonitor {
public:
    struct Config {
        std::chrono::seconds check_interval{3};
        std::chrono::seconds timeout{9};
    };

    /// @param on_worker_dead  Callback invoked (from monitor thread) when a
    ///                        worker misses its heartbeat deadline.
    /// @param cfg             Optional timing configuration.
    explicit HealthMonitor(std::function<void(const std::string&)> on_worker_dead);
    explicit HealthMonitor(std::function<void(const std::string&)> on_worker_dead,
                           Config cfg);

    ~HealthMonitor();

    HealthMonitor(const HealthMonitor&)            = delete;
    HealthMonitor& operator=(const HealthMonitor&) = delete;

    // ------------------------------------------------------------------
    // Lifecycle
    // ------------------------------------------------------------------

    void start();
    void stop();

    // ------------------------------------------------------------------
    // Heartbeat API
    // ------------------------------------------------------------------

    /// Register a new worker and seed its heartbeat timestamp to now,
    /// preventing an immediate false-positive on the first check cycle.
    void registerWorker(const std::string& worker_id);

    /// Update the last-seen timestamp for a worker.  Called on every
    /// WorkerHeartbeat RPC received by the scheduler.
    void receiveHeartbeat(const std::string& worker_id);

    // ------------------------------------------------------------------
    // Observers
    // ------------------------------------------------------------------

    [[nodiscard]] std::size_t trackedWorkerCount() const;

private:
    void monitorLoop();

    Config cfg_;
    std::function<void(const std::string&)> on_worker_dead_;

    mutable std::mutex heartbeats_mutex_;
    std::unordered_map<std::string,
                       std::chrono::steady_clock::time_point> heartbeats_;

    std::atomic<bool> stop_{true};
    std::thread       thread_;
};

} // namespace chronoflow
