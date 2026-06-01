#include "retry_manager.h"

#include <spdlog/spdlog.h>

namespace chronoflow {

// ---------------------------------------------------------------------------
// Static helper
// ---------------------------------------------------------------------------

std::chrono::milliseconds RetryManager::computeDelay(int retry_count) noexcept {
    // 2000 ms × 2^retry_count, capped at 60 000 ms (60 s).
    // Shift is capped at 10 to prevent 64-bit overflow before the min().
    constexpr auto kBase = std::chrono::milliseconds{2000};
    constexpr auto kMax  = std::chrono::milliseconds{60'000};
    const long long multiplier = 1LL << std::min(retry_count, 10);
    return std::min<std::chrono::milliseconds>(kBase * multiplier, kMax);
}

// ---------------------------------------------------------------------------
// Construction / destruction
// ---------------------------------------------------------------------------

RetryManager::RetryManager(RequeueFn requeue, MarkFailedFn mark_failed)
    : requeue_(std::move(requeue))
    , mark_failed_(std::move(mark_failed))
{}

RetryManager::~RetryManager() {
    stop();
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

void RetryManager::start() {
    if (!stop_.exchange(false)) return;
    thread_ = std::thread([this] { retryLoop(); });
    spdlog::info("[RetryManager] Started");
}

void RetryManager::stop() {
    if (stop_.exchange(true)) return;

    // Wake the retry thread so it can observe stop_ == true.
    cv_.notify_all();

    if (thread_.joinable()) {
        thread_.join();
    }
    spdlog::info("[RetryManager] Stopped ({} entries discarded)", pq_.size());
}

// ---------------------------------------------------------------------------
// scheduleRetry
// ---------------------------------------------------------------------------

void RetryManager::scheduleRetry(Task task) {
    // Safety guard: if retry_count already meets the limit, fail immediately.
    if (task.retry_count >= task.retry_limit) {
        spdlog::error(
            "[RetryManager] Task '{}' has retry_count={} >= retry_limit={} — "
            "marking FAILED immediately",
            task.task_id, task.retry_count, task.retry_limit);
        mark_failed_(task.task_id);
        return;
    }

    const auto delay    = computeDelay(task.retry_count);
    const auto deadline = std::chrono::steady_clock::now() + delay;

    spdlog::info(
        "[RetryManager] Scheduled retry for '{}' (attempt {}/{}) in {}ms",
        task.task_id, task.retry_count, task.retry_limit, delay.count());

    {
        std::scoped_lock lock{mutex_};
        pq_.push(RetryEntry{.next_retry_time = deadline, .task = std::move(task)});
    }
    cv_.notify_one();
}

// ---------------------------------------------------------------------------
// Observers
// ---------------------------------------------------------------------------

std::size_t RetryManager::pendingCount() const {
    std::scoped_lock lock{mutex_};
    return pq_.size();
}

// ---------------------------------------------------------------------------
// Retry loop
//
// Uses condition_variable::wait_until to sleep exactly until the earliest
// deadline in the priority queue, then processes all expired entries.
// ---------------------------------------------------------------------------
void RetryManager::retryLoop() {
    spdlog::info("[RetryManager] Retry thread running");

    while (!stop_.load(std::memory_order_acquire)) {
        std::unique_lock lock{mutex_};

        // Block until there's something in the queue or shutdown.
        cv_.wait(lock, [this] {
            return !pq_.empty() || stop_.load(std::memory_order_acquire);
        });

        if (stop_.load(std::memory_order_acquire)) break;
        if (pq_.empty()) continue;

        // Sleep until the earliest entry is due (or a new entry is added,
        // or the queue is shut down — whichever comes first).
        const auto deadline = pq_.top().next_retry_time;

        const bool expired = cv_.wait_until(lock, deadline, [this, &deadline] {
            // Re-check: new entries may have been added with an earlier deadline.
            return stop_.load(std::memory_order_acquire) ||
                   pq_.empty() ||
                   pq_.top().next_retry_time <= std::chrono::steady_clock::now();
        });

        if (stop_.load(std::memory_order_acquire)) break;
        if (!expired || pq_.empty()) continue;

        // Drain all entries whose deadline has passed.
        const auto now = std::chrono::steady_clock::now();
        std::vector<RetryEntry> due;

        while (!pq_.empty() && pq_.top().next_retry_time <= now) {
            // Move from const top — safe because we pop immediately after.
            due.push_back(std::move(const_cast<RetryEntry&>(pq_.top())));
            pq_.pop();
        }
        lock.unlock();

        for (auto& entry : due) {
            const std::string& id = entry.task.task_id;

            if (entry.task.retry_count >= entry.task.retry_limit) {
                // Safety net: shouldn't happen if CompletionThread checked
                // canRetry() before calling scheduleRetry(), but guard anyway.
                spdlog::error(
                    "[RetryManager] Task '{}' exhausted retries ({}/{}) — "
                    "marking FAILED",
                    id, entry.task.retry_count, entry.task.retry_limit);
                mark_failed_(id);
            } else {
                spdlog::info(
                    "[RetryManager] Re-queuing task '{}' (attempt {}/{})",
                    id, entry.task.retry_count, entry.task.retry_limit);
                // Clear the stale worker_id before requeueing.
                entry.task.worker_id.clear();
                requeue_(std::move(entry.task));
            }
        }
    }

    spdlog::info("[RetryManager] Retry thread exiting");
}

} // namespace chronoflow
