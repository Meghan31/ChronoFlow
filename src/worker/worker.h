#pragma once

// NOTE: Requires gRPC-generated headers from worker.proto and scheduler.proto.
//   Build step:  protoc --grpc_out=. --cpp_out=. proto/worker.proto
//                protoc --grpc_out=. --cpp_out=. proto/scheduler.proto
//   CMake adds the build directory to include paths automatically.

#include "worker.grpc.pb.h"
#include "scheduler.grpc.pb.h"
#include "../common/thread_pool.h"

#include <atomic>
#include <memory>
#include <string>
#include <thread>

#include <grpcpp/grpcpp.h>

namespace chronoflow {

// ---------------------------------------------------------------------------
// WorkerNode
//
// Implements the WorkerService gRPC server on a worker node.
//
// Responsibilities
// ----------------
//   • Accepts ExecuteTask RPCs from the scheduler:
//       – enqueues the command to a ThreadPool
//       – waits for the future (blocking the gRPC handler thread)
//       – captures stdout via popen(), reports exit code
//   • Responds to SendHeartbeat probes from the scheduler.
//   • Runs a background thread that:
//       1. Registers itself with the scheduler on startup.
//       2. Sends WorkerHeartbeat calls to the scheduler every 2 s,
//          reporting the current active_tasks count.
//
// Usage
// -----
//   WorkerNode node{"worker-1", "0.0.0.0:50051", "scheduler-host:50050"};
//   node.Start();        // blocks the calling thread on Wait() below
//   node.Wait();         // blocks until Stop() or server shutdown
//   node.Stop();
// ---------------------------------------------------------------------------
class WorkerNode final : public WorkerService::Service {
public:
    /// @param worker_id          Unique name for this worker, e.g. "worker-1"
    /// @param listen_address     Address to bind gRPC server, e.g. "0.0.0.0:50051"
    /// @param scheduler_address  Scheduler endpoint, e.g. "localhost:50050"
    /// @param thread_count       ThreadPool size for parallel task execution
    WorkerNode(std::string worker_id,
               std::string listen_address,
               std::string scheduler_address,
               std::size_t thread_count = 4);

    ~WorkerNode() override;

    WorkerNode(const WorkerNode&)            = delete;
    WorkerNode& operator=(const WorkerNode&) = delete;

    // ------------------------------------------------------------------
    // gRPC service implementation (WorkerService)
    // ------------------------------------------------------------------

    /// Enqueue command to ThreadPool, block until done, capture output.
    grpc::Status ExecuteTask(grpc::ServerContext*  ctx,
                             const ExecuteRequest* req,
                             ExecuteResponse*      resp) override;

    /// Liveness probe — always responds with acknowledged = true.
    grpc::Status SendHeartbeat(grpc::ServerContext*    ctx,
                               const HeartbeatRequest* req,
                               HeartbeatResponse*      resp) override;

    // ------------------------------------------------------------------
    // Lifecycle
    // ------------------------------------------------------------------

    /// Start the gRPC server and the background heartbeat thread.
    void Start();

    /// Wait until the server shuts down (blocks the calling thread).
    void Wait();

    /// Graceful shutdown: drain in-flight RPCs, stop heartbeat thread.
    void Stop();

    // ------------------------------------------------------------------
    // Observers
    // ------------------------------------------------------------------

    [[nodiscard]] int active_tasks() const noexcept {
        return active_tasks_.load(std::memory_order_relaxed);
    }

    [[nodiscard]] const std::string& worker_id() const noexcept {
        return worker_id_;
    }

private:
    // Background loop: registers with scheduler then sends periodic heartbeats.
    void heartbeatLoop();

    // ------------------------------------------------------------------
    // Configuration
    // ------------------------------------------------------------------
    std::string worker_id_;
    std::string listen_address_;
    std::string scheduler_address_;

    // ------------------------------------------------------------------
    // Execution
    // ------------------------------------------------------------------
    ThreadPool       thread_pool_;
    std::atomic<int> active_tasks_{0};

    // ------------------------------------------------------------------
    // gRPC
    // ------------------------------------------------------------------
    std::unique_ptr<grpc::Server>           server_;
    std::unique_ptr<SchedulerService::Stub> scheduler_stub_;

    // ------------------------------------------------------------------
    // Lifecycle
    // ------------------------------------------------------------------
    std::atomic<bool> running_{false};
    std::atomic<bool> stop_{true};
    std::thread       heartbeat_thread_;
};

} // namespace chronoflow
