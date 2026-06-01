#include "scheduler.h"

#include "worker.pb.h"
#include "worker.grpc.pb.h"

#include <spdlog/spdlog.h>
#include <grpcpp/grpcpp.h>

#include <algorithm>
#include <cstdlib>
#include <stdexcept>
#include <unordered_map>

namespace chronoflow {

// ---------------------------------------------------------------------------
// Construction
//
// Member initialisation order mirrors the declaration order in scheduler.h
// to avoid use-before-initialisation.  The RetryManager and HealthMonitor
// lambdas capture `this` but are only invoked after start() is called, so
// all captured members are fully initialised by then.
// ---------------------------------------------------------------------------
Scheduler::Scheduler(std::chrono::milliseconds base_delay_ms,
                     std::size_t               dispatch_threads,
                     std::string               db_path)
    : dispatch_pool_(dispatch_threads)
    , metadata_store_(std::move(db_path))
    //
    // RetryManager callbacks
    //   requeue    — resets state to READY, persists it, pushes to ready_queue_
    //   markFailed — sets state to FAILED, persists it
    //
    , retry_manager_(
        [this](Task t) {
            spdlog::info("[RetryManager→Sched] Re-queuing task '{}' (attempt {}/{})",
                         t.task_id, t.retry_count, t.retry_limit);
            setState(t.task_id, TaskState::READY);
            metadata_store_.updateTaskState(t.task_id, TaskState::READY);
            ready_queue_.push(std::move(t));
        },
        [this](const std::string& id) {
            spdlog::error("[RetryManager→Sched] Task '{}' permanently FAILED", id);
            setState(id, TaskState::FAILED);
            metadata_store_.updateTaskState(id, TaskState::FAILED);
        })
    //
    // HealthMonitor callback
    //   on_worker_dead — collects RUNNING tasks on the dead worker and requeues
    //
    , health_monitor_(
        [this](const std::string& worker_id) {
            requeueWorkerTasks(worker_id);
        })
    , base_delay_(base_delay_ms)
{}

// ---------------------------------------------------------------------------
// Destruction
// ---------------------------------------------------------------------------

Scheduler::~Scheduler() {
    stop();
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

void Scheduler::start() {
    if (running_.exchange(true)) {
        spdlog::warn("[Scheduler] start() on already-running scheduler");
        return;
    }
    stop_.store(false, std::memory_order_release);

    spdlog::info("[Scheduler] Starting (base_delay={}ms dispatch_threads={})",
                 base_delay_.count(), dispatch_pool_.thread_count());

    start_time_ = std::chrono::steady_clock::now();

    // --- Phase 4: recover persistent state BEFORE launching threads ----
    recoverFromDB();

    // --- Phase 3: start managed sub-components -------------------------
    retry_manager_.start();
    health_monitor_.start();

    // --- Launch the three dedicated scheduler threads ------------------
    intake_thread_     = std::thread([this] { intakeThread(); });
    dispatcher_thread_ = std::thread([this] { dispatcherThread(); });
    completion_thread_ = std::thread([this] { completionThread(); });
}

void Scheduler::stop() {
    if (!running_.exchange(false)) return;
    stop_.store(true, std::memory_order_release);

    spdlog::info("[Scheduler] Shutting down…");

    // Wake all blocked queue consumers so threads exit.
    intake_queue_.shutdown();
    ready_queue_.shutdown();
    completion_queue_.shutdown();

    if (intake_thread_.joinable()) {
        intake_thread_.join();
    }
    if (dispatcher_thread_.joinable()) {
        dispatcher_thread_.join();
    }
    if (completion_thread_.joinable()) {
        completion_thread_.join();
    }

    // Stop the managed sub-components (they join their own threads).
    retry_manager_.stop();
    health_monitor_.stop();

    spdlog::info("[Scheduler] Stopped.");
}

// ---------------------------------------------------------------------------
// Task API
// ---------------------------------------------------------------------------

void Scheduler::submit(Task task) {
    if (!running_.load(std::memory_order_acquire)) {
        throw std::runtime_error("[Scheduler] submit() on stopped scheduler");
    }
    spdlog::debug("[Scheduler] Queuing submit for task '{}'", task.task_id);
    intake_queue_.push(std::move(task));
}

TaskState Scheduler::taskState(const std::string& task_id) const {
    std::shared_lock lock{state_mutex_};
    auto it = tasks_.find(task_id);
    if (it == tasks_.end())
        throw std::out_of_range("[Scheduler] Unknown task_id: " + task_id);
    return it->second.state;
}

Scheduler::TaskInfo Scheduler::taskInfo(const std::string& task_id) const {
    std::shared_lock lock{state_mutex_};
    auto it = tasks_.find(task_id);
    if (it == tasks_.end())
        throw std::out_of_range("[Scheduler] Unknown task_id: " + task_id);
    return {it->second.state, it->second.retry_count};
}

std::vector<Task> Scheduler::listTasks() const {
    std::shared_lock lock{state_mutex_};
    std::vector<Task> result;
    result.reserve(tasks_.size());

    for (const auto& [id, task] : tasks_) {
        result.push_back(task);
    }

    return result;
}

std::vector<WorkerRegistry::WorkerSnapshot> Scheduler::listWorkers() const {
    return worker_registry_.listWorkers();
}

Scheduler::MetricsSnapshot Scheduler::metricsSnapshot() const {
    MetricsSnapshot snapshot;

    {
        std::shared_lock lock{state_mutex_};
        snapshot.total_tasks = tasks_.size();
        for (const auto& [id, task] : tasks_) {
            switch (task.state) {
            case TaskState::PENDING:
                ++snapshot.pending_tasks;
                break;
            case TaskState::READY:
                ++snapshot.ready_tasks;
                break;
            case TaskState::RUNNING:
                ++snapshot.running_tasks;
                break;
            case TaskState::RETRY_WAIT:
                ++snapshot.retry_wait_tasks;
                break;
            case TaskState::SUCCESS:
                ++snapshot.success_tasks;
                break;
            case TaskState::FAILED:
                ++snapshot.failed_tasks;
                break;
            case TaskState::CANCELLED:
                ++snapshot.cancelled_tasks;
                break;
            }
        }
    }

    snapshot.workers_total = worker_registry_.workerCount();
    snapshot.workers_alive = worker_registry_.aliveCount();
    snapshot.completions_total = completions_total_.load(std::memory_order_relaxed);

    {
        std::scoped_lock lock{completions_mutex_};
        const auto cutoff = std::chrono::steady_clock::now() - std::chrono::minutes{1};
        while (!completions_.empty() && completions_.front() < cutoff) {
            completions_.pop_front();
        }
        snapshot.completions_last_minute = completions_.size();
    }

    return snapshot;
}

Scheduler::HealthSnapshot Scheduler::healthSnapshot() const {
    HealthSnapshot snapshot;
    snapshot.scheduler_running = running_.load(std::memory_order_relaxed);
    snapshot.workers_total = worker_registry_.workerCount();
    snapshot.workers_alive = worker_registry_.aliveCount();

    {
        std::shared_lock lock{state_mutex_};
        snapshot.task_total = tasks_.size();
    }

    if (snapshot.scheduler_running) {
        snapshot.uptime_seconds = std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::steady_clock::now() - start_time_).count();
    }

    return snapshot;
}

bool Scheduler::cancelTask(const std::string& task_id) {
    std::unique_lock lock{state_mutex_};
    auto it = tasks_.find(task_id);
    if (it == tasks_.end()) return false;

    const TaskState s = it->second.state;
    if (s == TaskState::PENDING   ||
        s == TaskState::READY     ||
        s == TaskState::RETRY_WAIT) {
        spdlog::info("[Scheduler] Cancelling task '{}'", task_id);
        it->second.state = TaskState::CANCELLED;
        lock.unlock();
        metadata_store_.updateTaskState(task_id, TaskState::CANCELLED);
        recordCompletion();
        return true;
    }
    return false;
}

// ---------------------------------------------------------------------------
// Worker registry API
// ---------------------------------------------------------------------------

void Scheduler::registerWorker(const std::string& worker_id,
                                const std::string& address) {
    worker_registry_.registerWorker(worker_id, address);
    health_monitor_.registerWorker(worker_id);   // seed heartbeat timestamp
}

void Scheduler::updateWorkerHeartbeat(const std::string& worker_id,
                                       int                active_tasks) {
    worker_registry_.updateHeartbeat(worker_id, active_tasks);
    health_monitor_.receiveHeartbeat(worker_id);
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

void Scheduler::setState(const std::string& task_id, TaskState new_state) {
    std::unique_lock lock{state_mutex_};
    if (auto it = tasks_.find(task_id); it != tasks_.end()) {
        spdlog::debug("[Scheduler] {} : {} → {}",
                      task_id, toString(it->second.state), toString(new_state));
        it->second.state = new_state;
    }
}

std::chrono::milliseconds
Scheduler::backoffDelay(int retry_count) const noexcept {
    constexpr auto kCap = std::chrono::milliseconds{5 * 60 * 1000};
    const long long mult = 1LL << std::min(retry_count, 10);
    return std::min(std::chrono::milliseconds{base_delay_.count() * mult}, kCap);
}

void Scheduler::dispatchToWorker(Task task, WorkerRegistry::DispatchHandle handle) {
    const std::string wid = handle.worker_id;
    spdlog::info("[DispatcherThread] Remote '{}' → worker '{}'",
                 task.task_id, wid);

    auto future = dispatch_pool_.enqueue(
        [this, task = std::move(task), handle = std::move(handle)]() mutable {
            ExecuteRequest req;
            req.set_task_id(task.task_id);
            req.set_command(task.command);

            ExecuteResponse resp;
            grpc::ClientContext ctx;
            ctx.set_deadline(std::chrono::system_clock::now() +
                             std::chrono::hours{1});

            auto status = handle.stub->ExecuteTask(&ctx, req, &resp);
            worker_registry_.releaseDispatch(handle.worker_id);

            int exit_code;
            if (!status.ok()) {
                spdlog::error("[Dispatch] gRPC error '{}' on worker '{}': {}",
                              task.task_id, handle.worker_id,
                              status.error_message());
                worker_registry_.markDead(handle.worker_id);
                exit_code = -1;
            } else {
                exit_code = resp.success() ? 0 : resp.exit_code();
                spdlog::info("[Dispatch] Task '{}' exit_code={} from '{}'",
                             task.task_id, exit_code, handle.worker_id);
            }
            completion_queue_.push({std::move(task), exit_code});
        });
    (void)future;
}

void Scheduler::recordCompletion() {
    completions_total_.fetch_add(1, std::memory_order_relaxed);
    std::scoped_lock lock{completions_mutex_};
    completions_.push_back(std::chrono::steady_clock::now());
}

// ---------------------------------------------------------------------------
// requeueWorkerTasks (Phase 3)
//
// Called by HealthMonitor when a worker's heartbeat times out.
// Scans the task map for all RUNNING tasks assigned to that worker,
// resets them to READY, and pushes them back to ready_queue_.
// ---------------------------------------------------------------------------
void Scheduler::requeueWorkerTasks(const std::string& worker_id) {
    std::vector<Task> to_requeue;

    {
        std::unique_lock lock{state_mutex_};
        for (auto& [id, task] : tasks_) {
            if (task.state     == TaskState::RUNNING &&
                task.worker_id == worker_id) {
                spdlog::warn("[Scheduler] Reclaiming task '{}' from dead worker '{}'",
                             id, worker_id);
                task.state     = TaskState::READY;
                task.worker_id.clear();
                to_requeue.push_back(task);   // copy with updated state
            }
        }
    }

    for (auto& task : to_requeue) {
        metadata_store_.updateTaskState(task.task_id, TaskState::READY);
        metadata_store_.updateWorker(task.task_id, "");
        ready_queue_.push(std::move(task));
    }

    // Let WorkerRegistry know the worker is dead so it won't be selected
    // for new dispatches.
    worker_registry_.markDead(worker_id);

    spdlog::info("[Scheduler] Requeued {} task(s) from dead worker '{}'",
                 to_requeue.size(), worker_id);
}

// ---------------------------------------------------------------------------
// recoverFromDB (Phase 4)
//
// Called once at the top of start(), before any threads are launched.
// Reconstructs the DAG and task state from the SQLite database.
//
// Recovery rules
// --------------
//   RUNNING / PENDING → reset to READY, push to ready_queue_
//   READY             → push to ready_queue_ (already ready)
//   RETRY_WAIT        → hand off to RetryManager
//   SUCCESS / FAILED / CANCELLED → restore to task map only (terminal)
// ---------------------------------------------------------------------------
void Scheduler::recoverFromDB() {
    auto tasks = metadata_store_.getAllTasks();
    auto deps  = metadata_store_.getDependencies();

    if (tasks.empty()) {
        spdlog::info("[Recovery] No persisted tasks found — fresh start");
        return;
    }

    spdlog::info("[Recovery] Found {} task(s) and {} dependency edge(s)",
                 tasks.size(), deps.size());

    // --- Build dependency lookup: child → [parents] --------------------
    std::unordered_map<std::string, std::vector<std::string>> task_deps;
    for (const auto& [parent, child] : deps) {
        task_deps[child].push_back(parent);
    }

    // --- Rebuild DAGEngine ---------------------------------------------
    {
        std::scoped_lock dag_lock{dag_mutex_};
        for (const auto& task : tasks) {
            const auto& d = task_deps.count(task.task_id)
                                ? task_deps.at(task.task_id)
                                : std::vector<std::string>{};
            dag_engine_.addTask(task.task_id, d);
        }
        if (dag_engine_.detectCycle()) {
            spdlog::error("[Recovery] Cycle detected in recovered DAG — skipping requeue");
            return;
        }
    }

    // --- Restore task map and re-inject live tasks ---------------------
    int requeued = 0, retried = 0, terminal = 0;

    for (auto& task : tasks) {
        const std::string id    = task.task_id;
        const TaskState   state = task.state;

        // Hydrate dependencies from the edges table.
        if (task_deps.count(id)) {
            task.dependencies = task_deps.at(id);
        }

        // Unconditionally store in the state map.
        {
            std::unique_lock sl{state_mutex_};
            tasks_[id] = task;
        }

        switch (state) {
        case TaskState::RUNNING:
            // Worker that was executing this is gone — treat like PENDING.
            spdlog::info("[Recovery] Task '{}' was RUNNING — resetting to READY", id);
            setState(id, TaskState::READY);
            metadata_store_.updateTaskState(id, TaskState::READY);
            metadata_store_.updateWorker(id, "");
            ready_queue_.push(task);    // push the copied task (state is READY now in map)
            ++requeued;
            break;

        case TaskState::PENDING:
            spdlog::info("[Recovery] Task '{}' was PENDING — promoting to READY", id);
            setState(id, TaskState::READY);
            metadata_store_.updateTaskState(id, TaskState::READY);
            ready_queue_.push(task);
            ++requeued;
            break;

        case TaskState::READY:
            spdlog::info("[Recovery] Task '{}' READY — re-queuing", id);
            ready_queue_.push(task);
            ++requeued;
            break;

        case TaskState::RETRY_WAIT:
            spdlog::info("[Recovery] Task '{}' RETRY_WAIT — handing to RetryManager", id);
            // NOTE: retry_manager_ is not started yet here, but scheduleRetry()
            // only enqueues to the priority_queue — safe before start().
            retry_manager_.scheduleRetry(task);
            ++retried;
            break;

        case TaskState::SUCCESS:
        case TaskState::FAILED:
        case TaskState::CANCELLED:
            spdlog::info("[Recovery] Task '{}' terminal ({}) — restored to map",
                         id, toString(state));
            ++terminal;
            break;
        }
    }

    spdlog::info("[Recovery] Complete — requeued={} retried={} terminal={}",
                 requeued, retried, terminal);
}

// ---------------------------------------------------------------------------
// IntakeThread
// ---------------------------------------------------------------------------
void Scheduler::intakeThread() {
    spdlog::info("[IntakeThread] Started");

    while (!stop_.load(std::memory_order_acquire)) {
        Task task;
        if (!intake_queue_.wait_pop(task, std::chrono::milliseconds{50})) continue;

        const std::string id = task.task_id;
        spdlog::info("[IntakeThread] Registering task '{}' ({} dep(s))",
                     id, task.dependencies.size());

        // Register with DAG.
        {
            std::scoped_lock dag_lock{dag_mutex_};
            dag_engine_.addTask(id, task.dependencies);
        }

        // Persist.
        metadata_store_.upsertTask(task);
        metadata_store_.storeDependencies(id, task.dependencies);

        // Store in state map.
        {
            std::unique_lock sl{state_mutex_};
            task.state = TaskState::PENDING;
            tasks_[id] = task;
        }

        // If this task has no dependencies, it's immediately ready.
        {
            std::scoped_lock dag_lock{dag_mutex_};
            for (const auto& rid : dag_engine_.getReadyTasks()) {
                if (rid != id) continue;
                Task ready_task;
                {
                    std::shared_lock sl{state_mutex_};
                    auto it = tasks_.find(rid);
                    if (it == tasks_.end()) break;
                    if (it->second.state != TaskState::PENDING) break;
                    ready_task = it->second;
                }
                setState(rid, TaskState::READY);
                metadata_store_.updateTaskState(rid, TaskState::READY);
                spdlog::info("[IntakeThread] Task '{}' immediately ready", rid);
                ready_queue_.push(std::move(ready_task));
                break;
            }
        }
    }

    spdlog::info("[IntakeThread] Exiting");
}

// ---------------------------------------------------------------------------
// DispatcherThread
// ---------------------------------------------------------------------------
void Scheduler::dispatcherThread() {
    spdlog::info("[DispatcherThread] Started");

    while (!stop_.load(std::memory_order_acquire)) {
        Task task;
        if (!ready_queue_.wait_pop(task, std::chrono::milliseconds{100})) continue;

        // Skip tasks that were cancelled while in ready_queue_.
        {
            std::shared_lock sl{state_mutex_};
            auto it = tasks_.find(task.task_id);
            if (it != tasks_.end() && it->second.state == TaskState::CANCELLED) {
                spdlog::info("[DispatcherThread] Skipping cancelled task '{}'",
                             task.task_id);
                continue;
            }
        }

        setState(task.task_id, TaskState::RUNNING);
        metadata_store_.updateTaskState(task.task_id, TaskState::RUNNING);

        // --- Phase 2: dispatch to a remote worker ----------------------
        auto handle = worker_registry_.selectAndReserve();
        if (handle) {
            // Track which worker owns this task (needed by HealthMonitor
            // callback to collect stranded tasks on worker failure).
            task.worker_id = handle->worker_id;
            {
                std::unique_lock sl{state_mutex_};
                if (auto it = tasks_.find(task.task_id); it != tasks_.end()) {
                    it->second.worker_id = task.worker_id;
                }
            }
            metadata_store_.updateWorker(task.task_id, task.worker_id);
            dispatchToWorker(std::move(task), std::move(*handle));
            continue;
        }

        // --- Phase 1 fallback: run locally ----------------------------
        spdlog::info("[DispatcherThread] No workers — local exec '{}': {}",
                     task.task_id, task.command);

        const int exit_code = std::system(task.command.c_str());  // NOLINT
        spdlog::info("[DispatcherThread] Local exec '{}' exit_code={}",
                     task.task_id, exit_code);

        completion_queue_.push({std::move(task), exit_code});
    }

    spdlog::info("[DispatcherThread] Exiting");
}

// ---------------------------------------------------------------------------
// CompletionThread
// ---------------------------------------------------------------------------
void Scheduler::completionThread() {
    spdlog::info("[CompletionThread] Started");

    while (!stop_.load(std::memory_order_acquire)) {
        CompletionRecord rec;
        if (!completion_queue_.wait_pop(rec, std::chrono::milliseconds{100})) continue;

        const std::string& id        = rec.task.task_id;
        const int          exit_code = rec.exit_code;

        if (exit_code == 0) {
            // ----- SUCCESS -----------------------------------------------
            setState(id, TaskState::SUCCESS);
            metadata_store_.updateTaskState(id, TaskState::SUCCESS);
            spdlog::info("[CompletionThread] Task '{}' succeeded", id);
            recordCompletion();

            std::vector<std::string> newly_ready;
            {
                std::scoped_lock dag_lock{dag_mutex_};
                newly_ready = dag_engine_.markComplete(id);
            }

            for (const auto& rid : newly_ready) {
                Task ready_task;
                {
                    std::shared_lock sl{state_mutex_};
                    auto it = tasks_.find(rid);
                    if (it == tasks_.end()) continue;
                    if (it->second.state == TaskState::CANCELLED) continue;
                    ready_task = it->second;
                }
                setState(rid, TaskState::READY);
                metadata_store_.updateTaskState(rid, TaskState::READY);
                spdlog::info("[CompletionThread] Unlocked task '{}'", rid);
                ready_queue_.push(std::move(ready_task));
            }

        } else {
            // ----- FAILURE -----------------------------------------------
            Task& failed = rec.task;
            ++failed.retry_count;

            // Update retry_count in the state map so it's reflected in DB.
            {
                std::unique_lock sl{state_mutex_};
                if (auto it = tasks_.find(id); it != tasks_.end()) {
                    it->second.retry_count = failed.retry_count;
                    it->second.worker_id.clear();
                }
            }

            if (failed.canRetry()) {
                setState(id, TaskState::RETRY_WAIT);
                metadata_store_.updateTaskState(id, TaskState::RETRY_WAIT);
                // Persist the incremented retry_count.
                metadata_store_.upsertTask(failed);

                spdlog::warn(
                    "[CompletionThread] Task '{}' failed (attempt {}/{}) "
                    "— scheduling retry",
                    id, failed.retry_count, failed.retry_limit);

                retry_manager_.scheduleRetry(std::move(failed));

            } else {
                setState(id, TaskState::FAILED);
                metadata_store_.updateTaskState(id, TaskState::FAILED);
                recordCompletion();

                spdlog::error(
                    "[CompletionThread] Task '{}' permanently FAILED after {} attempt(s)",
                    id, failed.retry_count);
            }
        }
    }

    spdlog::info("[CompletionThread] Exiting");
}

} // namespace chronoflow
