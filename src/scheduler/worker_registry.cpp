#include "worker_registry.h"

#include <spdlog/spdlog.h>

namespace chronoflow {

// ---------------------------------------------------------------------------
// Mutation
// ---------------------------------------------------------------------------

void WorkerRegistry::registerWorker(const std::string& worker_id,
                                    const std::string& address) {
    std::unique_lock lock{mutex_};

    if (auto it = workers_.find(worker_id); it != workers_.end()) {
        // Re-registration: worker restarted — reset liveness and address.
        it->second->is_alive      = true;
        it->second->address       = address;
        it->second->active_tasks  = 0;
        it->second->last_heartbeat = std::chrono::steady_clock::now();

        // Rebuild stub in case address changed.
        auto ch = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
        it->second->stub = WorkerService::NewStub(ch);

        spdlog::info("[WorkerRegistry] Re-registered '{}' at {}", worker_id, address);
        return;
    }

    auto info = std::make_shared<WorkerInfo>();
    info->address = address;

    auto channel = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    info->stub = WorkerService::NewStub(channel);

    workers_[worker_id] = std::move(info);
    spdlog::info("[WorkerRegistry] Registered new worker '{}' at {}", worker_id, address);
}

void WorkerRegistry::updateHeartbeat(const std::string& worker_id, int active_tasks) {
    std::unique_lock lock{mutex_};

    auto it = workers_.find(worker_id);
    if (it == workers_.end()) {
        spdlog::warn("[WorkerRegistry] Heartbeat from unknown worker '{}'", worker_id);
        return;
    }

    it->second->is_alive       = true;
    it->second->active_tasks   = active_tasks;
    it->second->last_heartbeat = std::chrono::steady_clock::now();
}

void WorkerRegistry::markDead(const std::string& worker_id) {
    std::unique_lock lock{mutex_};

    if (auto it = workers_.find(worker_id); it != workers_.end()) {
        it->second->is_alive = false;
        spdlog::warn("[WorkerRegistry] Marked worker '{}' as dead", worker_id);
    }
}

// ---------------------------------------------------------------------------
// Selection / dispatch
// ---------------------------------------------------------------------------

std::optional<WorkerRegistry::DispatchHandle>
WorkerRegistry::selectAndReserve() {
    // Write lock: we atomically select + increment active_tasks to prevent
    // multiple dispatchers from racing to the same "least loaded" worker.
    std::unique_lock lock{mutex_};

    WorkerInfo* best    = nullptr;
    std::string best_id;
    int         min_load = std::numeric_limits<int>::max();

    for (auto& [id, info] : workers_) {
        if (info->is_alive && info->active_tasks < min_load) {
            min_load = info->active_tasks;
            best     = info.get();
            best_id  = id;
        }
    }

    if (!best) return std::nullopt;

    ++best->active_tasks;

    return DispatchHandle{
        .worker_id = best_id,
        .stub      = best->stub.get(),
        .info      = workers_.at(best_id),   // shared_ptr copy keeps info alive
    };
}

void WorkerRegistry::releaseDispatch(const std::string& worker_id) {
    std::unique_lock lock{mutex_};

    if (auto it = workers_.find(worker_id); it != workers_.end()) {
        auto& tasks = it->second->active_tasks;
        if (tasks > 0) --tasks;
    }
}

// ---------------------------------------------------------------------------
// Observers
// ---------------------------------------------------------------------------

bool WorkerRegistry::hasAliveWorkers() const {
    std::shared_lock lock{mutex_};
    for (const auto& [id, info] : workers_) {
        if (info->is_alive) return true;
    }
    return false;
}

std::size_t WorkerRegistry::workerCount() const {
    std::shared_lock lock{mutex_};
    return workers_.size();
}

std::size_t WorkerRegistry::aliveCount() const {
    std::shared_lock lock{mutex_};
    std::size_t alive = 0;
    for (const auto& [id, info] : workers_) {
        if (info->is_alive) {
            ++alive;
        }
    }
    return alive;
}

std::vector<WorkerRegistry::WorkerSnapshot> WorkerRegistry::listWorkers() const {
    std::shared_lock lock{mutex_};
    std::vector<WorkerSnapshot> result;
    result.reserve(workers_.size());

    for (const auto& [id, info] : workers_) {
        result.push_back({
            .worker_id = id,
            .address = info->address,
            .active_tasks = info->active_tasks,
            .is_alive = info->is_alive,
            .last_heartbeat = info->last_heartbeat,
        });
    }

    return result;
}

// ---------------------------------------------------------------------------
// Maintenance
// ---------------------------------------------------------------------------

void WorkerRegistry::reapDeadWorkers(std::chrono::seconds timeout) {
    const auto cutoff = std::chrono::steady_clock::now() - timeout;
    std::unique_lock lock{mutex_};

    for (auto& [id, info] : workers_) {
        if (info->is_alive && info->last_heartbeat < cutoff) {
            info->is_alive = false;
            spdlog::warn("[WorkerRegistry] Worker '{}' timed out — marked dead", id);
        }
    }
}

} // namespace chronoflow
