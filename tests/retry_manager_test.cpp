#include <gtest/gtest.h>

#include "../src/scheduler/retry_manager.h"

#include <atomic>

namespace chronoflow {

TEST(RetryManagerTest, ComputeDelayBackoffAndCap) {
    EXPECT_EQ(RetryManager::computeDelay(0).count(), 2000);
    EXPECT_EQ(RetryManager::computeDelay(1).count(), 4000);
    EXPECT_EQ(RetryManager::computeDelay(2).count(), 8000);
    EXPECT_EQ(RetryManager::computeDelay(5).count(), 60000);
    EXPECT_EQ(RetryManager::computeDelay(10).count(), 60000);
}

TEST(RetryManagerTest, ScheduleRetryMarksFailedWhenLimitReached) {
    std::atomic<int> failed_calls{0};

    RetryManager manager(
        [](Task) {},
        [&failed_calls](const std::string&) { failed_calls.fetch_add(1); }
    );

    Task task;
    task.task_id = "task-1";
    task.retry_count = 3;
    task.retry_limit = 3;

    manager.scheduleRetry(task);

    EXPECT_EQ(failed_calls.load(), 1);
    EXPECT_EQ(manager.pendingCount(), 0u);
}

TEST(RetryManagerTest, ScheduleRetryEnqueuesWhenAllowed) {
    std::atomic<int> requeue_calls{0};

    RetryManager manager(
        [&requeue_calls](Task) { requeue_calls.fetch_add(1); },
        [](const std::string&) {}
    );

    Task task;
    task.task_id = "task-2";
    task.retry_count = 1;
    task.retry_limit = 3;

    manager.scheduleRetry(task);

    EXPECT_EQ(manager.pendingCount(), 1u);
    EXPECT_EQ(requeue_calls.load(), 0);
}

} // namespace chronoflow
