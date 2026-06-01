#include "scheduler_server.h"
#include <spdlog/spdlog.h>

int main(int argc, char* argv[]) {
    spdlog::set_level(spdlog::level::info);
    spdlog::info("Starting ChronoFlow Scheduler...");

    chronoflow::SchedulerServer server{"0.0.0.0:50051"};
    server.Start();

    spdlog::info("Scheduler running on port 50051. Press Ctrl+C to stop.");
    server.Wait();

    return 0;
}