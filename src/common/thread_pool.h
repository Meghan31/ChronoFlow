#pragma once

#include "../../include/concurrent_queue.h"

#include <functional>
#include <future>
#include <memory>
#include <thread>
#include <vector>
#include <atomic>
#include <stdexcept>
#include <type_traits>

namespace chronoflow {

// ---------------------------------------------------------------------------
// ThreadPool
//
// Fixed-size pool of worker threads.  Jobs are submitted via enqueue() and
// executed in FIFO order.  Workers sleep on ConcurrentQueue::pop() when idle
// and are woken automatically when a new job arrives.  Graceful shutdown is
// performed in the destructor: no new jobs are accepted and workers drain the
// queue before exiting.
// ---------------------------------------------------------------------------
class ThreadPool {
public:
    // ------------------------------------------------------------------
    // Construction / destruction
    // ------------------------------------------------------------------

    /// Creates `thread_count` worker threads.  Throws std::invalid_argument
    /// when thread_count is 0.
    explicit ThreadPool(std::size_t thread_count);

    /// Graceful shutdown: stops accepting new work, waits for all workers to
    /// finish their current job, then joins every thread.
    ~ThreadPool();

    // Non-copyable, non-movable.
    ThreadPool(const ThreadPool&)            = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;
    ThreadPool(ThreadPool&&)                 = delete;
    ThreadPool& operator=(ThreadPool&&)      = delete;

    // ------------------------------------------------------------------
    // Job submission
    // ------------------------------------------------------------------

    /// Enqueue a callable (with optional arguments) and return a std::future
    /// bound to its return value.  Throws std::runtime_error if the pool has
    /// been shut down.
    ///
    /// Example:
    ///   auto fut = pool.enqueue([](int x){ return x * 2; }, 21);
    ///   int result = fut.get();  // == 42
    template <typename F, typename... Args>
    [[nodiscard]] auto enqueue(F&& f, Args&&... args)
        -> std::future<std::invoke_result_t<F, Args...>>;

    // ------------------------------------------------------------------
    // Observers
    // ------------------------------------------------------------------

    [[nodiscard]] std::size_t thread_count() const noexcept {
        return workers_.size();
    }

    [[nodiscard]] std::size_t pending_jobs() const noexcept {
        return job_queue_.size();
    }

    [[nodiscard]] bool is_stopped() const noexcept {
        return stop_.load(std::memory_order_acquire);
    }

private:
    using Job = std::function<void()>;

    void worker_loop();

    std::vector<std::thread> workers_;
    ConcurrentQueue<Job>      job_queue_;
    std::atomic<bool>         stop_{false};
};

// ---------------------------------------------------------------------------
// Template implementation
// ---------------------------------------------------------------------------

template <typename F, typename... Args>
auto ThreadPool::enqueue(F&& f, Args&&... args)
    -> std::future<std::invoke_result_t<F, Args...>>
{
    using ReturnType = std::invoke_result_t<F, Args...>;

    if (stop_.load(std::memory_order_acquire)) {
        throw std::runtime_error("ThreadPool::enqueue called on stopped pool");
    }

    // Wrap the callable + arguments into a packaged_task so we can extract a
    // future before moving ownership into the queue.
    auto task = std::make_shared<std::packaged_task<ReturnType()>>(
        [f  = std::forward<F>(f),
         ... bound_args = std::forward<Args>(args)]() mutable -> ReturnType {
            return std::invoke(std::forward<F>(f),
                               std::forward<Args>(bound_args)...);
        });

    std::future<ReturnType> future = task->get_future();

    job_queue_.push([task = std::move(task)]() { (*task)(); });

    return future;
}

} // namespace chronoflow
