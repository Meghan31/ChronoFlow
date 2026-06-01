#pragma once

// NOTE: Requires gRPC-generated header from scheduler.proto.
//   Build step:  protoc --grpc_out=. --cpp_out=. proto/scheduler.proto
//   CMake adds the build directory to include paths automatically.

#include "scheduler.grpc.pb.h"

#include <grpcpp/grpcpp.h>

namespace chronoflow {

class Scheduler;   // forward declaration — avoids pulling all of scheduler.h

// ---------------------------------------------------------------------------
// SchedulerServiceImpl
//
// Thin gRPC adapter layer that delegates every RPC to the Scheduler core.
// It is designed to be registered with a grpc::ServerBuilder and should
// outlive the gRPC server it is registered with.
//
// Thread safety: all public methods may be called concurrently by gRPC
// worker threads.  The Scheduler's internal state machine is already
// fully thread-safe.
// ---------------------------------------------------------------------------
class SchedulerServiceImpl final : public SchedulerService::Service {
public:
    explicit SchedulerServiceImpl(Scheduler& scheduler);

    // ------------------------------------------------------------------
    // Client-facing RPCs
    // ------------------------------------------------------------------

    grpc::Status SubmitTask(grpc::ServerContext* ctx,
                            const TaskRequest*   req,
                            TaskResponse*        resp) override;

    grpc::Status GetTaskStatus(grpc::ServerContext*  ctx,
                               const StatusRequest*  req,
                               StatusResponse*       resp) override;

    grpc::Status ListTasks(grpc::ServerContext*      ctx,
                           const ListTasksRequest*   req,
                           ListTasksResponse*        resp) override;

    grpc::Status CancelTask(grpc::ServerContext* ctx,
                            const CancelRequest* req,
                            CancelResponse*      resp) override;

    grpc::Status ListWorkers(grpc::ServerContext*      ctx,
                             const ListWorkersRequest* req,
                             ListWorkersResponse*      resp) override;

    grpc::Status GetMetrics(grpc::ServerContext*    ctx,
                            const MetricsRequest*  req,
                            MetricsResponse*       resp) override;

    grpc::Status GetHealth(grpc::ServerContext*   ctx,
                           const HealthRequest*  req,
                           HealthResponse*       resp) override;

    // ------------------------------------------------------------------
    // Worker-facing RPCs
    // ------------------------------------------------------------------

    grpc::Status RegisterWorker(grpc::ServerContext*             ctx,
                                const WorkerRegistrationRequest* req,
                                WorkerRegistrationResponse*      resp) override;

    grpc::Status WorkerHeartbeat(grpc::ServerContext*          ctx,
                                 const WorkerHeartbeatRequest* req,
                                 WorkerHeartbeatResponse*      resp) override;

private:
    Scheduler& scheduler_;
};

} // namespace chronoflow
