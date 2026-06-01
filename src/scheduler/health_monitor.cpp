#include "health_monitor.h"

#include <spdlog/spdlog.h>

namespace chronoflow {

// ---------------------------------------------------------------------------
// Construction / destruction
// ---------------------------------------------------------------------------

HealthMonitor::HealthMonitor(std::function<void(const std::string&)> on_worker_dead)
    : HealthMonitor(std::move(on_worker_dead), Config{})
{}

HealthMonitor::HealthMonitor(std::function<void(const std::string&)> on_worker_dead,
                              Config cfg)
    : cfg_(cfg)
    , on_worker_dead_(std::move(on_worker_dead))
{}

HealthMonitor::~HealthMonitor() {
    stop();
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

void HealthMonitor::start() {
    if (!stop_.exchange(false)) return;
    thread_ = std::thread([this] { monitorLoop(); });
    spdlog::info("[HealthMonitor] Started — check={}s timeout={}s",
                 cfg_.check_interval.count(), cfg_.timeout.count());
}

void HealthMonitor::stop() {
    if (stop_.exchange(true)) return;
    if (thread_.joinable()) {
        thread_.join();
    }
    spdlog::info("[HealthMonitor] Stopped.");
}

// ---------------------------------------------------------------------------
// Heartbeat API
// ---------------------------------------------------------------------------

void HealthMonitor::registerWorker(const std::string& worker_id) {
    std::scoped_lock lock{heartbeats_mutex_};
    // Seed timestamp to now so the first check cycle doesn't false-positive.
    heartbeats_[worker_id] = std::chrono::steady_clock::now();
    spdlog::debug("[HealthMonitor] Tracking worker '{}'", worker_id);
}

void HealthMonitor::receiveHeartbeat(const std::string& worker_id) {
    std::scoped_lock lock{heartbeats_mutex_};
    heartbeats_[worker_id] = std::chrono::steady_clock::now();
}

// ---------------------------------------------------------------------------
// Observers
// ---------------------------------------------------------------------------

std::size_t HealthMonitor::trackedWorkerCount() const {
    std::scoped_lock lock{heartbeats_mutex_};
    return heartbeats_.size();
}

// ---------------------------------------------------------------------------
// Monitor loop
//
// Wakes every check_interval and scans all tracked workers.  Workers whose
// last heartbeat is older than timeout are declared dead:
//   1. The on_worker_dead callback is invoked.
//   2. The worker is removed from the tracking map.
//      (It will be re-inserted when it reconnects and RegisterWorker fires.)
// ---------------------------------------------------------------------------
void HealthMonitor::monitorLoop() {
    spdlog::info("[HealthMonitor] Monitor thread running");

    while (!stop_.load(std::memory_order_acquire)) {
        std::this_thread::sleep_for(cfg_.check_interval);

        if (stop_.load(std::memory_order_acquire)) break;

        const auto cutoff = std::chrono::steady_clock::now() - cfg_.timeout;

        // Collect dead workers under lock; invoke callbacks outside lock
        // to avoid a deadlock if the callback tries to call receiveHeartbeat.
        std::vector<std::string> dead;
        {
            std::scoped_lock lock{heartbeats_mutex_};
            for (const auto& [wid, last_hb] : heartbeats_) {
                if (last_hb < cutoff) {
                    dead.push_back(wid);
                }
            }
        }

        for (const auto& wid : dead) {
            spdlog::warn("[HealthMonitor] Worker '{}' missed heartbeat — declaring dead", wid);

            // Invoke scheduler callback: collect RUNNING tasks and requeue.
            on_worker_dead_(wid);

            // Remove from our map so we don't keep firing the callback.
            {
                std::scoped_lock lock{heartbeats_mutex_};
                heartbeats_.erase(wid);
            }
        }
    }

    spdlog::info("[HealthMonitor] Monitor thread exiting");
}

} // namespace chronoflow
