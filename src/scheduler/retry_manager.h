#pragma once

#include "../../include/task.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <vector>

namespace chronoflow {

// ---------------------------------------------------------------------------
// RetryManager
//
// Owns an exponential-backoff retry queue implemented as a min-heap ordered
// by next_retry_time.  A background thread sleeps until the earliest entry
// is due, then dispatches it.
//
// Backoff formula
// ---------------
//   delay = 2000 ms × 2^retry_count,  capped at 60 000 ms (60 s)
//   retry_count=0 → 2 s, =1 → 4 s, =2 → 8 s, …, =5+ → 60 s
//
// Lifecycle
// ---------
//   1. On task failure CompletionThread calls scheduleRetry(task).
//      retry_count has already been incremented by the caller.
//   2. If task.retry_count >= task.retry_limit the task is immediately
//      forwarded to mark_failed (no queue insertion).
//   3. Otherwise the task is enqueued with deadline =
//      now + computeDelay(retry_count).
//   4. The background thread wakes via condition_variable::wait_until at
//      the earliest deadline and re-queues eligible tasks.
//
// Thread safety
// -------------
// scheduleRetry() is safe to call from any thread.
// requeue / mark_failed callbacks are invoked from the retry thread only.
// ---------------------------------------------------------------------------
class RetryManager {
public:
    using RequeueFn    = std::function<void(Task)>;
    using MarkFailedFn = std::function<void(const std::string& task_id)>;

    RetryManager(RequeueFn requeue, MarkFailedFn mark_failed);
    ~RetryManager();

    RetryManager(const RetryManager&)            = delete;
    RetryManager& operator=(const RetryManager&) = delete;

    // ------------------------------------------------------------------
    // Lifecycle
    // ------------------------------------------------------------------

    void start();
    void stop();

    // ------------------------------------------------------------------
    // Public API
    // ------------------------------------------------------------------

    /// Schedule a task for retry.  retry_count must already reflect the
    /// failed attempt (caller increments before calling).
    ///
    /// If retry_count >= retry_limit the task is permanently failed
    /// immediately (mark_failed is called, task is not enqueued).
    void scheduleRetry(Task task);

    /// Returns the computed backoff delay for a given retry_count.
    [[nodiscard]] static std::chrono::milliseconds
    computeDelay(int retry_count) noexcept;

    [[nodiscard]] std::size_t pendingCount() const;

private:
    void retryLoop();

    // ------------------------------------------------------------------
    // Internal priority-queue entry
    // ------------------------------------------------------------------
    struct RetryEntry {
        std::chrono::steady_clock::time_point next_retry_time;
        Task task;

        // Min-heap ordering (earliest deadline = top).
        bool operator>(const RetryEntry& rhs) const noexcept {
            return next_retry_time > rhs.next_retry_time;
        }
    };

    RequeueFn    requeue_;
    MarkFailedFn mark_failed_;

    mutable std::mutex      mutex_;
    std::condition_variable cv_;
    std::priority_queue<RetryEntry,
                        std::vector<RetryEntry>,
                        std::greater<RetryEntry>> pq_;

    std::atomic<bool> stop_{true};
    std::thread       thread_;
};

} // namespace chronoflow
