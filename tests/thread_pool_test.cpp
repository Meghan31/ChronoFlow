#include <gtest/gtest.h>

#include "../src/common/thread_pool.h"

#include <atomic>
#include <future>
#include <vector>

namespace chronoflow {

TEST(ThreadPoolTest, ConstructorRejectsZeroThreads) {
    EXPECT_THROW(ThreadPool pool(0), std::invalid_argument);
}

TEST(ThreadPoolTest, EnqueueExecutesTasksAndReturnsFutures) {
    ThreadPool pool(2);

    auto future = pool.enqueue([](int x) { return x * 2; }, 21);
    EXPECT_EQ(future.get(), 42);
}

TEST(ThreadPoolTest, ProcessesMultipleJobs) {
    ThreadPool pool(4);

    std::atomic<int> sum{0};
    std::vector<std::future<void>> futures;
    futures.reserve(50);

    for (int i = 0; i < 50; ++i) {
        futures.push_back(pool.enqueue([&sum] { sum.fetch_add(1); }));
    }

    for (auto& future : futures) {
        future.get();
    }

    EXPECT_EQ(sum.load(), 50);
    EXPECT_EQ(pool.pending_jobs(), 0u);
}

} // namespace chronoflow
