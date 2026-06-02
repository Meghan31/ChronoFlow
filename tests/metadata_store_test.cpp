#include <gtest/gtest.h>

#include "../src/storage/metadata_store.h"

#include <algorithm>

namespace chronoflow {

TEST(MetadataStoreTest, UpsertAndReadBackTask) {
    MetadataStore store(":memory:");

    Task task;
    task.task_id = "task-1";
    task.command = "echo hello";
    task.state = TaskState::READY;
    task.priority = 7;
    task.retry_count = 1;
    task.retry_limit = 5;
    task.worker_id = "worker-1";

    store.upsertTask(task);

    auto tasks = store.getAllTasks();
    ASSERT_EQ(tasks.size(), 1u);

    const Task& loaded = tasks.front();
    EXPECT_EQ(loaded.task_id, task.task_id);
    EXPECT_EQ(loaded.command, task.command);
    EXPECT_EQ(loaded.state, task.state);
    EXPECT_EQ(loaded.priority, task.priority);
    EXPECT_EQ(loaded.retry_count, task.retry_count);
    EXPECT_EQ(loaded.retry_limit, task.retry_limit);
    EXPECT_EQ(loaded.worker_id, task.worker_id);
}

TEST(MetadataStoreTest, UpdatesStateAndWorker) {
    MetadataStore store(":memory:");

    Task task;
    task.task_id = "task-2";
    task.command = "echo update";
    store.upsertTask(task);

    store.updateTaskState(task.task_id, TaskState::RUNNING);
    store.updateWorker(task.task_id, "worker-2");

    auto tasks = store.getAllTasks();
    ASSERT_EQ(tasks.size(), 1u);
    EXPECT_EQ(tasks.front().state, TaskState::RUNNING);
    EXPECT_EQ(tasks.front().worker_id, "worker-2");
}

TEST(MetadataStoreTest, StoresDependencies) {
    MetadataStore store(":memory:");

    Task task;
    task.task_id = "child";
    task.command = "echo deps";
    store.upsertTask(task);

    std::vector<std::string> deps = {"parent-a", "parent-b"};
    store.storeDependencies(task.task_id, deps);

    auto edges = store.getDependencies();
    EXPECT_EQ(edges.size(), 2u);

    auto has_edge = [&](const std::string& parent) {
        return std::find(edges.begin(), edges.end(),
                         std::make_pair(parent, task.task_id)) != edges.end();
    };

    EXPECT_TRUE(has_edge("parent-a"));
    EXPECT_TRUE(has_edge("parent-b"));
}

} // namespace chronoflow
