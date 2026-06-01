#include "scheduler_service_impl.h"
#include "scheduler.h"
#include "../../include/task.h"

#include <spdlog/spdlog.h>
#include <chrono>

namespace chronoflow {

SchedulerServiceImpl::SchedulerServiceImpl(Scheduler& scheduler)
    : scheduler_(scheduler) {}

// ---------------------------------------------------------------------------
// SubmitTask
// ---------------------------------------------------------------------------
grpc::Status SchedulerServiceImpl::SubmitTask(grpc::ServerContext* /*ctx*/,
                                               const TaskRequest*  req,
                                               TaskResponse*       resp) {
    spdlog::info("[gRPC] SubmitTask '{}'  deps={}", req->task_id(),
                 req->dependencies_size());

    Task task;
    task.task_id     = req->task_id();
    task.command     = req->command();
    task.priority    = req->priority();
    task.retry_limit = req->retry_limit() > 0 ? req->retry_limit() : 3;

    for (const auto& dep : req->dependencies()) {
        task.dependencies.push_back(dep);
    }

    try {
        scheduler_.submit(std::move(task));
        resp->set_task_id(req->task_id());
        resp->set_accepted(true);
        resp->set_message("Task accepted");
        return grpc::Status::OK;
    } catch (const std::exception& e) {
        spdlog::error("[gRPC] SubmitTask error: {}", e.what());
        resp->set_task_id(req->task_id());
        resp->set_accepted(false);
        resp->set_message(e.what());
        return grpc::Status{grpc::StatusCode::INTERNAL, e.what()};
    }
}

// ---------------------------------------------------------------------------
// GetTaskStatus
// ---------------------------------------------------------------------------
grpc::Status SchedulerServiceImpl::GetTaskStatus(grpc::ServerContext* /*ctx*/,
                                                   const StatusRequest* req,
                                                   StatusResponse*      resp) {
    spdlog::debug("[gRPC] GetTaskStatus '{}'", req->task_id());

    try {
        auto info = scheduler_.taskInfo(req->task_id());
        resp->set_task_id(req->task_id());
        resp->set_state(std::string{toString(info.state)});
        resp->set_retry_count(info.retry_count);
        return grpc::Status::OK;
    } catch (const std::out_of_range& e) {
        return grpc::Status{grpc::StatusCode::NOT_FOUND,
                            "Unknown task_id: " + req->task_id()};
    }
}

// ---------------------------------------------------------------------------
// ListTasks
// ---------------------------------------------------------------------------
grpc::Status SchedulerServiceImpl::ListTasks(grpc::ServerContext* /*ctx*/,
                                             const ListTasksRequest* req,
                                             ListTasksResponse*      resp) {
    const std::string filter = req->state_filter();
    const auto tasks = scheduler_.listTasks();

    for (const auto& task : tasks) {
        const std::string state = std::string{toString(task.state)};
        if (!filter.empty() && state != filter) {
            continue;
        }

        auto* out = resp->add_tasks();
        out->set_task_id(task.task_id);
        out->set_command(task.command);
        out->set_state(state);
        out->set_priority(task.priority);
        out->set_retry_count(task.retry_count);
        out->set_retry_limit(task.retry_limit);
        out->set_worker_id(task.worker_id);

        for (const auto& dep : task.dependencies) {
            out->add_dependencies(dep);
        }
    }

    return grpc::Status::OK;
}

// ---------------------------------------------------------------------------
// CancelTask
// ---------------------------------------------------------------------------
grpc::Status SchedulerServiceImpl::CancelTask(grpc::ServerContext* /*ctx*/,
                                               const CancelRequest* req,
                                               CancelResponse*      resp) {
    spdlog::info("[gRPC] CancelTask '{}'", req->task_id());

    const bool ok = scheduler_.cancelTask(req->task_id());
    resp->set_task_id(req->task_id());
    resp->set_cancelled(ok);
    resp->set_message(ok ? "Task cancelled"
                         : "Task not in a cancellable state or unknown");
    return grpc::Status::OK;
}

// ---------------------------------------------------------------------------
// ListWorkers
// ---------------------------------------------------------------------------
grpc::Status SchedulerServiceImpl::ListWorkers(grpc::ServerContext* /*ctx*/,
                                               const ListWorkersRequest* /*req*/,
                                               ListWorkersResponse*      resp) {
    const auto workers = scheduler_.listWorkers();
    const auto now = std::chrono::steady_clock::now();

    for (const auto& worker : workers) {
        auto* out = resp->add_workers();
        out->set_worker_id(worker.worker_id);
        out->set_address(worker.address);
        out->set_active_tasks(worker.active_tasks);
        out->set_is_alive(worker.is_alive);
        const auto age_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            now - worker.last_heartbeat).count();
        out->set_last_heartbeat_ms(age_ms);
    }

    return grpc::Status::OK;
}

// ---------------------------------------------------------------------------
// GetMetrics
// ---------------------------------------------------------------------------
grpc::Status SchedulerServiceImpl::GetMetrics(grpc::ServerContext* /*ctx*/,
                                              const MetricsRequest* /*req*/,
                                              MetricsResponse*      resp) {
    const auto metrics = scheduler_.metricsSnapshot();

    resp->set_total_tasks(static_cast<int32_t>(metrics.total_tasks));
    resp->set_pending_tasks(static_cast<int32_t>(metrics.pending_tasks));
    resp->set_ready_tasks(static_cast<int32_t>(metrics.ready_tasks));
    resp->set_running_tasks(static_cast<int32_t>(metrics.running_tasks));
    resp->set_retry_wait_tasks(static_cast<int32_t>(metrics.retry_wait_tasks));
    resp->set_success_tasks(static_cast<int32_t>(metrics.success_tasks));
    resp->set_failed_tasks(static_cast<int32_t>(metrics.failed_tasks));
    resp->set_cancelled_tasks(static_cast<int32_t>(metrics.cancelled_tasks));
    resp->set_workers_total(static_cast<int32_t>(metrics.workers_total));
    resp->set_workers_alive(static_cast<int32_t>(metrics.workers_alive));
    resp->set_completions_total(static_cast<int32_t>(metrics.completions_total));
    resp->set_completions_last_minute(
        static_cast<int32_t>(metrics.completions_last_minute));

    return grpc::Status::OK;
}

// ---------------------------------------------------------------------------
// GetHealth
// ---------------------------------------------------------------------------
grpc::Status SchedulerServiceImpl::GetHealth(grpc::ServerContext* /*ctx*/,
                                             const HealthRequest* /*req*/,
                                             HealthResponse*      resp) {
    const auto health = scheduler_.healthSnapshot();

    resp->set_scheduler_running(health.scheduler_running);
    resp->set_workers_total(static_cast<int32_t>(health.workers_total));
    resp->set_workers_alive(static_cast<int32_t>(health.workers_alive));
    resp->set_task_total(static_cast<int32_t>(health.task_total));
    resp->set_uptime_seconds(static_cast<int64_t>(health.uptime_seconds));

    return grpc::Status::OK;
}

// ---------------------------------------------------------------------------
// RegisterWorker
// ---------------------------------------------------------------------------
grpc::Status SchedulerServiceImpl::RegisterWorker(
    grpc::ServerContext*             /*ctx*/,
    const WorkerRegistrationRequest* req,
    WorkerRegistrationResponse*      resp)
{
    spdlog::info("[gRPC] RegisterWorker '{}' at {}", req->worker_id(), req->address());

    try {
        scheduler_.registerWorker(req->worker_id(), req->address());
        resp->set_accepted(true);
        resp->set_message("Registered");
        return grpc::Status::OK;
    } catch (const std::exception& e) {
        resp->set_accepted(false);
        resp->set_message(e.what());
        return grpc::Status{grpc::StatusCode::INTERNAL, e.what()};
    }
}

// ---------------------------------------------------------------------------
// WorkerHeartbeat
// ---------------------------------------------------------------------------
grpc::Status SchedulerServiceImpl::WorkerHeartbeat(
    grpc::ServerContext*          /*ctx*/,
    const WorkerHeartbeatRequest* req,
    WorkerHeartbeatResponse*      resp)
{
    spdlog::debug("[gRPC] WorkerHeartbeat '{}' active_tasks={}",
                  req->worker_id(), req->active_tasks());

    scheduler_.updateWorkerHeartbeat(req->worker_id(), req->active_tasks());
    resp->set_acknowledged(true);
    return grpc::Status::OK;
}

} // namespace chronoflow
