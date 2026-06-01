#include "thread_pool.h"

#include <stdexcept>

namespace chronoflow {

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

ThreadPool::ThreadPool(std::size_t thread_count) {
    if (thread_count == 0) {
        throw std::invalid_argument(
            "ThreadPool: thread_count must be greater than zero");
    }

    workers_.reserve(thread_count);
    for (std::size_t i = 0; i < thread_count; ++i) {
        // Threads are joined in the destructor after shutdown.
        workers_.emplace_back([this] { worker_loop(); });
    }
}

// ---------------------------------------------------------------------------
// Destruction — graceful shutdown
// ---------------------------------------------------------------------------

ThreadPool::~ThreadPool() {
    // Signal that no new jobs will arrive and wake all blocked workers.
    stop_.store(true, std::memory_order_release);
    job_queue_.shutdown();

    for (auto& worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }
}

// ---------------------------------------------------------------------------
// Worker loop
// ---------------------------------------------------------------------------

void ThreadPool::worker_loop() {
    while (true) {
        // Blocking pop: sleeps until a job is available or the queue shuts down.
        auto opt_job = job_queue_.pop();

        // pop() returns nullopt when shutdown() was called and queue is empty.
        if (!opt_job.has_value()) {
            break;
        }

        // Execute the job.  Exceptions thrown by the job are captured inside
        // the packaged_task and surfaced through the future, so we don't catch
        // them here (letting them propagate would terminate the thread).
        (*opt_job)();
    }
}

} // namespace chronoflow
