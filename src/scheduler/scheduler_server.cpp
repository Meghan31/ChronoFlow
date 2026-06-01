#include "scheduler_server.h"

#include <spdlog/spdlog.h>
#include <stdexcept>

namespace chronoflow {

// ---------------------------------------------------------------------------
// Construction / destruction
// ---------------------------------------------------------------------------

SchedulerServer::SchedulerServer(std::string               listen_address,
                                 std::chrono::milliseconds base_delay,
                                 std::size_t               dispatch_threads,
                                 std::string               db_path)
    : listen_address_(std::move(listen_address))
    , scheduler_(base_delay, dispatch_threads, std::move(db_path))
    , service_impl_(scheduler_)
{}

SchedulerServer::~SchedulerServer() {
    Stop();
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

void SchedulerServer::Start() {
    grpc::ServerBuilder builder;

    // Bind port.  InsecureCredentials is fine for intra-cluster use;
    // swap for SslServerCredentials for production.
    builder.AddListeningPort(listen_address_,
                             grpc::InsecureServerCredentials());

    builder.RegisterService(&service_impl_);

    // Increase max message size for large task outputs (default 4 MiB → 64 MiB).
    builder.SetMaxReceiveMessageSize(64 * 1024 * 1024);
    builder.SetMaxSendMessageSize(64 * 1024 * 1024);

    grpc_server_ = builder.BuildAndStart();
    if (!grpc_server_) {
        throw std::runtime_error(
            "[SchedulerServer] Failed to start gRPC server on " + listen_address_);
    }

    spdlog::info("[SchedulerServer] gRPC server listening on {}", listen_address_);

    // Start the Scheduler's background threads after the server is up so
    // that any worker that connects immediately can be registered.
    scheduler_.start();

    spdlog::info("[SchedulerServer] Ready.");
}

void SchedulerServer::Wait() {
    if (grpc_server_) {
        grpc_server_->Wait();
    }
}

void SchedulerServer::Stop() {
    if (grpc_server_) {
        spdlog::info("[SchedulerServer] Shutting down gRPC server…");
        grpc_server_->Shutdown();
        grpc_server_.reset();
    }
    scheduler_.stop();
    spdlog::info("[SchedulerServer] Fully stopped.");
}

} // namespace chronoflow
