#include "worker.h"

#include <spdlog/spdlog.h>

#include <cerrno>
#include <cstdio>    // popen / pclose / fgets
#include <cstring>   // strerror
#include <sys/wait.h>  // WIFEXITED / WEXITSTATUS

namespace chronoflow {

// ---------------------------------------------------------------------------
// Internal helper — run a shell command, capture stdout, return exit code.
// ---------------------------------------------------------------------------
namespace {

struct CommandResult {
    int         exit_code;
    std::string output;
};

CommandResult runCommand(const std::string& cmd) {
    std::string output;

    FILE* pipe = ::popen(cmd.c_str(), "r");
    if (!pipe) {
        return {-1, "[popen failed: " + std::string(::strerror(errno)) + "]"};
    }

    char buf[512];
    while (::fgets(buf, sizeof(buf), pipe) != nullptr) {
        output += buf;
        // Cap captured output at 64 KiB to avoid unbounded memory growth.
        if (output.size() > 64 * 1024) {
            output += "\n[output truncated]";
            break;
        }
    }

    const int raw      = ::pclose(pipe);
    const int exit_code = WIFEXITED(raw) ? WEXITSTATUS(raw) : -1;
    return {exit_code, std::move(output)};
}

} // anonymous namespace

// ---------------------------------------------------------------------------
// Construction / destruction
// ---------------------------------------------------------------------------

WorkerNode::WorkerNode(std::string worker_id,
                       std::string listen_address,
                       std::string scheduler_address,
                       std::size_t thread_count)
    : worker_id_(std::move(worker_id))
    , listen_address_(std::move(listen_address))
    , scheduler_address_(std::move(scheduler_address))
    , thread_pool_(thread_count)
{
    auto channel = grpc::CreateChannel(scheduler_address_,
                                       grpc::InsecureChannelCredentials());
    scheduler_stub_ = SchedulerService::NewStub(channel);

    spdlog::info("[Worker {}] Created — listen={} scheduler={}",
                 worker_id_, listen_address_, scheduler_address_);
}

WorkerNode::~WorkerNode() {
    Stop();
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

void WorkerNode::Start() {
    if (running_.exchange(true)) {
        spdlog::warn("[Worker {}] Start() called on already-running worker", worker_id_);
        return;
    }
    stop_.store(false, std::memory_order_release);

    grpc::ServerBuilder builder;
    builder.AddListeningPort(listen_address_, grpc::InsecureServerCredentials());
    builder.RegisterService(this);
    server_ = builder.BuildAndStart();

    if (!server_) {
        running_.store(false);
        throw std::runtime_error("[Worker " + worker_id_ +
                                 "] Failed to start gRPC server on " + listen_address_);
    }

    spdlog::info("[Worker {}] gRPC server listening on {}", worker_id_, listen_address_);

    heartbeat_thread_ = std::thread([this] { heartbeatLoop(); });
}

void WorkerNode::Wait() {
    if (server_) server_->Wait();
}

void WorkerNode::Stop() {
    if (!running_.exchange(false)) return;
    stop_.store(true, std::memory_order_release);

    spdlog::info("[Worker {}] Shutting down…", worker_id_);

    if (server_) {
        server_->Shutdown();
    }
    if (heartbeat_thread_.joinable()) {
        heartbeat_thread_.join();
    }

    spdlog::info("[Worker {}] Stopped.", worker_id_);
}

// ---------------------------------------------------------------------------
// gRPC: ExecuteTask
//
// This method is called by the gRPC framework in a server-side thread.
// It enqueues the command to the ThreadPool and blocks until the future
// resolves — effectively using our bounded pool instead of unbounded
// OS threads for concurrent task execution.
// ---------------------------------------------------------------------------
grpc::Status WorkerNode::ExecuteTask(grpc::ServerContext* ctx,
                                     const ExecuteRequest* req,
                                     ExecuteResponse*      resp) {
    if (ctx->IsCancelled()) {
        return grpc::Status{grpc::StatusCode::CANCELLED, "RPC cancelled by client"};
    }

    const std::string task_id = req->task_id();
    const std::string command = req->command();

    spdlog::info("[Worker {}] ExecuteTask '{}': {}", worker_id_, task_id, command);
    ++active_tasks_;

    // Enqueue to thread pool — this call itself is non-blocking.
    auto future = thread_pool_.enqueue([cmd = command]() -> CommandResult {
        return runCommand(cmd);
    });

    // Block the gRPC handler thread until the task finishes.
    const CommandResult result = future.get();
    --active_tasks_;

    resp->set_task_id(task_id);
    resp->set_success(result.exit_code == 0);
    resp->set_output(result.output);
    resp->set_exit_code(result.exit_code);

    spdlog::info("[Worker {}] Task '{}' done — exit_code={} success={}",
                 worker_id_, task_id, result.exit_code, result.exit_code == 0);

    return grpc::Status::OK;
}

// ---------------------------------------------------------------------------
// gRPC: SendHeartbeat
//
// Scheduler-initiated liveness probe.  Responds immediately.
// ---------------------------------------------------------------------------
grpc::Status WorkerNode::SendHeartbeat(grpc::ServerContext* /*ctx*/,
                                       const HeartbeatRequest* req,
                                       HeartbeatResponse*      resp) {
    spdlog::debug("[Worker {}] Heartbeat probe — active_tasks={}",
                  worker_id_, req->active_tasks());
    resp->set_acknowledged(true);
    return grpc::Status::OK;
}

// ---------------------------------------------------------------------------
// Heartbeat loop (background thread)
//
// 1. Registers this worker with the scheduler.
// 2. Every 2 s, sends a WorkerHeartbeat carrying the current active_tasks.
// ---------------------------------------------------------------------------
void WorkerNode::heartbeatLoop() {
    spdlog::info("[Worker {}] Heartbeat loop started", worker_id_);

    // --- Step 1: Register with the scheduler ----------------------------
    {
        WorkerRegistrationRequest req;
        req.set_worker_id(worker_id_);
        req.set_address(listen_address_);

        WorkerRegistrationResponse resp;
        grpc::ClientContext ctx;
        ctx.set_deadline(std::chrono::system_clock::now() +
                         std::chrono::seconds{10});

        auto status = scheduler_stub_->RegisterWorker(&ctx, req, &resp);
        if (!status.ok()) {
            spdlog::error("[Worker {}] Registration failed: {}",
                          worker_id_, status.error_message());
        } else {
            spdlog::info("[Worker {}] Registered — accepted={}  message={}",
                         worker_id_, resp.accepted(), resp.message());
        }
    }

    // --- Step 2: Periodic heartbeat loop --------------------------------
    while (!stop_.load(std::memory_order_acquire)) {
        std::this_thread::sleep_for(std::chrono::seconds{2});

        if (stop_.load(std::memory_order_acquire)) break;

        WorkerHeartbeatRequest req;
        req.set_worker_id(worker_id_);
        req.set_active_tasks(active_tasks_.load(std::memory_order_relaxed));

        WorkerHeartbeatResponse resp;
        grpc::ClientContext ctx;
        ctx.set_deadline(std::chrono::system_clock::now() +
                         std::chrono::seconds{5});

        auto status = scheduler_stub_->WorkerHeartbeat(&ctx, req, &resp);
        if (!status.ok()) {
            spdlog::warn("[Worker {}] Heartbeat failed: {}",
                         worker_id_, status.error_message());
        } else {
            spdlog::debug("[Worker {}] Heartbeat sent — active_tasks={}",
                          worker_id_, req.active_tasks());
        }
    }

    spdlog::info("[Worker {}] Heartbeat loop exiting", worker_id_);
}

} // namespace chronoflow
