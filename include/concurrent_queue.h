#pragma once

#include <queue>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <optional>
#include <concepts>

namespace chronoflow {

// ---------------------------------------------------------------------------
// ConcurrentQueue<T>
//
// A thread-safe FIFO queue backed by std::queue.
//
// API surface
// -----------
//   push(item)                  — enqueue (notifies one waiter)
//   pop() → T                   — blocking dequeue
//   try_pop(T&) → bool          — non-blocking attempt; returns false if empty
//   wait_pop(T&, timeout) → bool— blocking with deadline; false on timeout
//   size() → size_t             — approximate snapshot
//   empty() → bool              — approximate snapshot
//   shutdown()                  — wake all blocked pop calls so threads can exit
// ---------------------------------------------------------------------------
template <typename T>
class ConcurrentQueue {
public:
    ConcurrentQueue() = default;

    // Non-copyable, non-movable (holds live synchronisation primitives).
    ConcurrentQueue(const ConcurrentQueue&)            = delete;
    ConcurrentQueue& operator=(const ConcurrentQueue&) = delete;
    ConcurrentQueue(ConcurrentQueue&&)                 = delete;
    ConcurrentQueue& operator=(ConcurrentQueue&&)      = delete;

    ~ConcurrentQueue() { shutdown(); }

    // ------------------------------------------------------------------
    // Producers
    // ------------------------------------------------------------------

    /// Enqueue an item and wake one waiting consumer.
    void push(T item) {
        {
            std::scoped_lock lock{mutex_};
            queue_.push(std::move(item));
        }
        cv_.notify_one();
    }

    // ------------------------------------------------------------------
    // Consumers
    // ------------------------------------------------------------------

    /// Blocking dequeue. Returns the front item, waiting indefinitely until
    /// one is available (or until shutdown() is called, in which case the
    /// returned optional is empty).
    [[nodiscard]] std::optional<T> pop() {
        std::unique_lock lock{mutex_};
        cv_.wait(lock, [this] { return !queue_.empty() || shutdown_; });
        if (queue_.empty()) return std::nullopt;  // woken by shutdown
        T item = std::move(queue_.front());
        queue_.pop();
        return item;
    }

    /// Non-blocking attempt. Returns true and fills `item` if the queue was
    /// non-empty; returns false immediately otherwise.
    bool try_pop(T& item) {
        std::scoped_lock lock{mutex_};
        if (queue_.empty()) return false;
        item = std::move(queue_.front());
        queue_.pop();
        return true;
    }

    /// Blocking dequeue with timeout. Returns true and fills `item` if an
    /// element became available within the timeout window; false otherwise.
    template <typename Rep, typename Period>
    bool wait_pop(T& item,
                  const std::chrono::duration<Rep, Period>& timeout) {
        std::unique_lock lock{mutex_};
        const bool signalled = cv_.wait_for(
            lock, timeout,
            [this] { return !queue_.empty() || shutdown_; });

        if (!signalled || queue_.empty()) return false;
        item = std::move(queue_.front());
        queue_.pop();
        return true;
    }

    // ------------------------------------------------------------------
    // Observers
    // ------------------------------------------------------------------

    /// Snapshot size. Not guaranteed to be accurate by the time the caller
    /// acts on the value.
    [[nodiscard]] std::size_t size() const {
        std::scoped_lock lock{mutex_};
        return queue_.size();
    }

    /// Snapshot emptiness check.
    [[nodiscard]] bool empty() const {
        std::scoped_lock lock{mutex_};
        return queue_.empty();
    }

    // ------------------------------------------------------------------
    // Lifecycle
    // ------------------------------------------------------------------

    /// Wake all blocked pop() / wait_pop() callers so threads can drain and
    /// exit. Safe to call multiple times.
    void shutdown() {
        {
            std::scoped_lock lock{mutex_};
            shutdown_ = true;
        }
        cv_.notify_all();
    }

    [[nodiscard]] bool is_shutdown() const {
        std::scoped_lock lock{mutex_};
        return shutdown_;
    }

private:
    mutable std::mutex      mutex_;
    std::condition_variable cv_;
    std::queue<T>           queue_;
    bool                    shutdown_{false};
};

} // namespace chronoflow
