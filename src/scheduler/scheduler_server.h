#pragma once

#include "scheduler.h"
#include "scheduler_service_impl.h"

#include <grpcpp/grpcpp.h>
#include <memory>
#include <string>

namespace chronoflow {

// ---------------------------------------------------------------------------
// SchedulerServer
//
// Binds a Scheduler (core logic) to a gRPC server (network interface).
// This is the top-level entry point for running the scheduler process.
//
// Usage
// -----
//   SchedulerServer server{"0.0.0.0:50050"};
//   server.Start();   // launches background threads + gRPC server
//   server.Wait();    // blocks until Stop() is called or process exits
//   server.Stop();    // graceful shutdown
// ---------------------------------------------------------------------------
class SchedulerServer {
public:
    /// @param listen_address   gRPC bind address, e.g. "0.0.0.0:50050"
    /// @param base_delay       Exponential-backoff base delay (default 1 s)
    /// @param dispatch_threads Worker pool size for async gRPC dispatch
    /// @param db_path          SQLite database path (":memory:" for tests)
    explicit SchedulerServer(
        std::string               listen_address,
        std::chrono::milliseconds base_delay       = std::chrono::milliseconds{1000},
        std::size_t               dispatch_threads = 16,
        std::string               db_path          = "chronoflow.db");

    ~SchedulerServer();

    SchedulerServer(const SchedulerServer&)            = delete;
    SchedulerServer& operator=(const SchedulerServer&) = delete;

    // ------------------------------------------------------------------
    // Lifecycle
    // ------------------------------------------------------------------

    /// Build and start the gRPC server, then start Scheduler threads.
    /// Throws std::runtime_error if the port cannot be bound.
    void Start();

    /// Block until the gRPC server shuts down (call from main thread).
    void Wait();

    /// Graceful shutdown: shuts down gRPC server, then Scheduler.
    void Stop();

    // ------------------------------------------------------------------
    // Accessors
    // ------------------------------------------------------------------

    [[nodiscard]] Scheduler& scheduler() noexcept { return scheduler_; }

private:
    std::string              listen_address_;
    Scheduler                scheduler_;
    SchedulerServiceImpl     service_impl_;
    std::unique_ptr<grpc::Server> grpc_server_;
};

} // namespace chronoflow
