#pragma once

// NOTE: This header requires the gRPC-generated header from worker.proto.
//   Build step:  protoc --grpc_out=. --cpp_out=. proto/worker.proto
//   Generated:   worker.pb.h  /  worker.grpc.pb.h
//   CMake adds the build directory to include paths automatically.

#include "worker.grpc.pb.h"

#include <chrono>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <limits>

#include <grpcpp/grpcpp.h>

namespace chronoflow {

// ---------------------------------------------------------------------------
// WorkerRegistry
//
// Thread-safe registry of all known worker nodes.  The scheduler uses it to:
//   • register new workers on startup (via RegisterWorker gRPC)
//   • refresh liveness info on each heartbeat
//   • select the best worker when dispatching a task (least-loaded)
//   • track in-flight task counts per worker
//   • reap workers that have gone silent
//
// Concurrency model
// -----------------
// All public methods are protected by shared_mutex_.  selectAndReserve()
// takes a write lock so that active_tasks increments are atomic with the
// selection decision (preventing double-dispatch to the same worker).
// The returned DispatchHandle holds a shared_ptr<WorkerInfo> so the stub
// stays alive for the duration of the async RPC even if the worker is
// later removed from the map.
// ---------------------------------------------------------------------------
class WorkerRegistry {
public:
    struct WorkerSnapshot {
        std::string                          worker_id;
        std::string                          address;
        int                                  active_tasks{0};
        bool                                 is_alive{false};
        std::chrono::steady_clock::time_point last_heartbeat{
            std::chrono::steady_clock::now()};
    };
    // ------------------------------------------------------------------
    // WorkerInfo — per-node state stored in the registry
    // ------------------------------------------------------------------
    struct WorkerInfo {
        std::unique_ptr<WorkerService::Stub> stub;      // gRPC channel stub
        std::string                          address;   // "host:port"
        int   active_tasks{0};                          // in-flight task count
        bool  is_alive{true};
        std::chrono::steady_clock::time_point last_heartbeat{
            std::chrono::steady_clock::now()};
    };

    // ------------------------------------------------------------------
    // DispatchHandle — returned by selectAndReserve()
    //
    // Holds the worker_id, a raw stub pointer (valid for the lifetime of
    // the shared_ptr), and a shared_ptr that keeps WorkerInfo alive during
    // the async RPC call.
    // ------------------------------------------------------------------
    struct DispatchHandle {
        std::string                 worker_id;
        WorkerService::Stub*        stub;   // raw ptr — valid while info alive
        std::shared_ptr<WorkerInfo> info;   // extends WorkerInfo lifetime
    };

    // ------------------------------------------------------------------
    // Mutation
    // ------------------------------------------------------------------

    /// Register a new worker.  Creates a gRPC channel and stub from address.
    /// If the worker_id already exists, resets its alive flag and updates
    /// the address (re-connection on worker restart).
    void registerWorker(const std::string& worker_id,
                        const std::string& address);

    /// Called on each WorkerHeartbeat RPC.  Updates last_heartbeat and
    /// active_tasks; marks the worker alive.
    void updateHeartbeat(const std::string& worker_id, int active_tasks);

    /// Explicitly mark a worker dead (called on gRPC transport error).
    void markDead(const std::string& worker_id);

    // ------------------------------------------------------------------
    // Selection / dispatch
    // ------------------------------------------------------------------

    /// Least-loaded selection: atomically picks the alive worker with the
    /// smallest active_tasks, increments its counter, and returns a handle.
    /// Returns std::nullopt when no alive workers exist.
    [[nodiscard]] std::optional<DispatchHandle> selectAndReserve();

    /// Decrements active_tasks for worker_id after a dispatch completes
    /// (success or failure).  Must be called exactly once per selectAndReserve().
    void releaseDispatch(const std::string& worker_id);

    // ------------------------------------------------------------------
    // Observers
    // ------------------------------------------------------------------

    [[nodiscard]] bool hasAliveWorkers() const;

    [[nodiscard]] std::size_t workerCount() const;

    [[nodiscard]] std::size_t aliveCount() const;

    [[nodiscard]] std::vector<WorkerSnapshot> listWorkers() const;

    // ------------------------------------------------------------------
    // Maintenance
    // ------------------------------------------------------------------

    /// Mark workers whose last heartbeat is older than timeout as dead.
    /// Called periodically by HealthMonitorThread.
    void reapDeadWorkers(std::chrono::seconds timeout = std::chrono::seconds{15});

private:
    mutable std::shared_mutex mutex_;
    std::unordered_map<std::string, std::shared_ptr<WorkerInfo>> workers_;
};

} // namespace chronoflow
