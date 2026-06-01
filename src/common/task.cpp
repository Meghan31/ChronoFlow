#include "task.h"

namespace chronoflow {

std::string_view toString(TaskState state) noexcept {
    switch (state) {
        case TaskState::PENDING:    return "PENDING";
        case TaskState::READY:      return "READY";
        case TaskState::RUNNING:    return "RUNNING";
        case TaskState::SUCCESS:    return "SUCCESS";
        case TaskState::FAILED:     return "FAILED";
        case TaskState::RETRY_WAIT: return "RETRY_WAIT";
        case TaskState::CANCELLED:  return "CANCELLED";
        default:                    return "UNKNOWN";
    }
}

TaskState fromString(std::string_view s) noexcept {
    if (s == "READY")      return TaskState::READY;
    if (s == "RUNNING")    return TaskState::RUNNING;
    if (s == "SUCCESS")    return TaskState::SUCCESS;
    if (s == "FAILED")     return TaskState::FAILED;
    if (s == "RETRY_WAIT") return TaskState::RETRY_WAIT;
    if (s == "CANCELLED")  return TaskState::CANCELLED;
    return TaskState::PENDING;   // default + "PENDING" → PENDING
}

} // namespace chronoflow
