#include <gtest/gtest.h>

#include "task.h"

namespace chronoflow {

TEST(TaskStateTest, ToStringAndFromStringRoundTrip) {
	EXPECT_EQ(toString(TaskState::PENDING), "PENDING");
	EXPECT_EQ(toString(TaskState::READY), "READY");
	EXPECT_EQ(toString(TaskState::RUNNING), "RUNNING");
	EXPECT_EQ(toString(TaskState::SUCCESS), "SUCCESS");
	EXPECT_EQ(toString(TaskState::FAILED), "FAILED");
	EXPECT_EQ(toString(TaskState::RETRY_WAIT), "RETRY_WAIT");
	EXPECT_EQ(toString(TaskState::CANCELLED), "CANCELLED");

	EXPECT_EQ(fromString("READY"), TaskState::READY);
	EXPECT_EQ(fromString("RUNNING"), TaskState::RUNNING);
	EXPECT_EQ(fromString("SUCCESS"), TaskState::SUCCESS);
	EXPECT_EQ(fromString("FAILED"), TaskState::FAILED);
	EXPECT_EQ(fromString("RETRY_WAIT"), TaskState::RETRY_WAIT);
	EXPECT_EQ(fromString("CANCELLED"), TaskState::CANCELLED);
	EXPECT_EQ(fromString("PENDING"), TaskState::PENDING);
	EXPECT_EQ(fromString("UNKNOWN"), TaskState::PENDING);
}

TEST(TaskTest, TerminalAndRetryHelpers) {
	Task task;
	task.state = TaskState::SUCCESS;
	EXPECT_TRUE(task.isTerminal());

	task.state = TaskState::FAILED;
	EXPECT_TRUE(task.isTerminal());

	task.state = TaskState::CANCELLED;
	EXPECT_TRUE(task.isTerminal());

	task.state = TaskState::RUNNING;
	EXPECT_FALSE(task.isTerminal());

	task.retry_limit = 3;
	task.retry_count = 0;
	EXPECT_TRUE(task.canRetry());

	task.retry_count = 3;
	EXPECT_FALSE(task.canRetry());
}

} // namespace chronoflow
