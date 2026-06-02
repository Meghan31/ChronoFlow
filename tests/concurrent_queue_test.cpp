#include <gtest/gtest.h>

#include "concurrent_queue.h"

#include <chrono>
#include <future>
#include <optional>
#include <thread>

namespace chronoflow {

TEST(ConcurrentQueueTest, PushPopMaintainsFifoOrder) {
    ConcurrentQueue<int> queue;

    queue.push(1);
    queue.push(2);

    auto first = queue.pop();
    ASSERT_TRUE(first.has_value());
    EXPECT_EQ(first.value(), 1);

    auto second = queue.pop();
    ASSERT_TRUE(second.has_value());
    EXPECT_EQ(second.value(), 2);

    EXPECT_TRUE(queue.empty());
}

TEST(ConcurrentQueueTest, TryPopReturnsFalseWhenEmpty) {
    ConcurrentQueue<int> queue;
    int value = 0;
    EXPECT_FALSE(queue.try_pop(value));
}

TEST(ConcurrentQueueTest, WaitPopTimesOut) {
    ConcurrentQueue<int> queue;
    int value = 0;

    const bool popped = queue.wait_pop(value, std::chrono::milliseconds(20));
    EXPECT_FALSE(popped);
}

TEST(ConcurrentQueueTest, ShutdownUnblocksPop) {
    ConcurrentQueue<int> queue;

    std::promise<std::optional<int>> promise;
    std::future<std::optional<int>> future = promise.get_future();

    std::thread waiter([&queue, &promise] {
        promise.set_value(queue.pop());
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    queue.shutdown();

    waiter.join();
    EXPECT_FALSE(future.get().has_value());
}

} // namespace chronoflow
