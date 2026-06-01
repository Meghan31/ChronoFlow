#include "worker.h"
#include <spdlog/spdlog.h>

int main(int argc, char* argv[]) {
    spdlog::set_level(spdlog::level::info);

    std::string worker_id  = (argc > 1) ? argv[1] : "worker-1";
    std::string port       = (argc > 2) ? argv[2] : "50052";
    std::string scheduler  = (argc > 3) ? argv[3] : "localhost:50051";

    spdlog::info("Starting worker {} on port {}", worker_id, port);

    chronoflow::WorkerNode worker(worker_id, "0.0.0.0:" + port, scheduler);
    worker.Start();
    worker.Wait();

    return 0;
}