# cmake_minimum_required(VERSION 3.20)

# project(ChronoFlow LANGUAGES CXX)

# set(CMAKE_CXX_STANDARD 20)
# set(CMAKE_CXX_STANDARD_REQUIRED ON)

# # Homebrew paths (Apple Silicon)
# list(APPEND CMAKE_PREFIX_PATH "/opt/homebrew")

# # ---------------- Dependencies ----------------

# # find_package(Protobuf REQUIRED)
# find_package(gRPC CONFIG REQUIRED)
# find_package(SQLite3 REQUIRED)
# find_package(spdlog REQUIRED)
# find_package(GTest REQUIRED)

# # ---------------- Scheduler ----------------

# add_executable(scheduler_service
#     src/scheduler/main.cpp
# )

# target_include_directories(scheduler_service PRIVATE include)

# target_link_libraries(scheduler_service PRIVATE
#     gRPC::grpc++
#     protobuf::libprotobuf
#     SQLite::SQLite3
#     spdlog::spdlog
# )

# # ---------------- Worker ----------------

# add_executable(worker_service
#     src/worker/main.cpp
# )

# target_include_directories(worker_service PRIVATE include)

# target_link_libraries(worker_service PRIVATE
#     gRPC::grpc++
#     protobuf::libprotobuf
#     SQLite::SQLite3
#     spdlog::spdlog
# )

# # ---------------- Tests ----------------

# enable_testing()

# add_executable(run_tests
#     tests/test_dummy.cpp
# )

# target_link_libraries(run_tests PRIVATE
#     GTest::gtest_main
# )

# include(GoogleTest)
# gtest_discover_tests(run_tests)


cmake_minimum_required(VERSION 3.20)
project(ChronoFlow LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

list(APPEND CMAKE_PREFIX_PATH "/opt/homebrew") # macOS Homebrew
list(APPEND CMAKE_PREFIX_PATH "/opt/grpc")      # Linux Docker (gRPC built from source)

# ------------ Dependencies ------------
# protobuf must be found before gRPC so gRPCConfig.cmake can resolve
# protobuf::libprotobuf / protobuf::libprotoc targets it depends on.
find_package(protobuf CONFIG QUIET)
find_package(gRPC CONFIG REQUIRED)
find_package(SQLite3 REQUIRED)
find_package(spdlog REQUIRED)
find_package(GTest REQUIRED)

# ------------ Proto Generation ------------
find_program(PROTOC protoc REQUIRED)
find_program(GRPC_CPP_PLUGIN grpc_cpp_plugin REQUIRED)

set(PROTO_DIR ${CMAKE_SOURCE_DIR}/proto)
set(PROTO_OUT ${CMAKE_BINARY_DIR}/proto_gen)
file(MAKE_DIRECTORY ${PROTO_OUT})

set(PROTO_FILES
    ${PROTO_DIR}/scheduler.proto
    ${PROTO_DIR}/worker.proto
)

set(PROTO_SRCS
    ${PROTO_OUT}/scheduler.pb.cc
    ${PROTO_OUT}/scheduler.grpc.pb.cc
    ${PROTO_OUT}/worker.pb.cc
    ${PROTO_OUT}/worker.grpc.pb.cc
)

add_custom_command(
    OUTPUT ${PROTO_SRCS}
    COMMAND ${PROTOC}
        --proto_path=${PROTO_DIR}
        --cpp_out=${PROTO_OUT}
        --grpc_out=${PROTO_OUT}
        --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN}
        ${PROTO_FILES}
    DEPENDS ${PROTO_FILES}
)

add_library(proto_gen STATIC ${PROTO_SRCS})
target_include_directories(proto_gen PUBLIC ${PROTO_OUT})
target_link_libraries(proto_gen PUBLIC gRPC::grpc++ protobuf::libprotobuf)

# ------------ Common sources ------------
file(GLOB COMMON_SRCS src/common/*.cpp)
file(GLOB STORAGE_SRCS src/storage/*.cpp)

# ------------ Scheduler ------------
file(GLOB SCHEDULER_SRCS src/scheduler/*.cpp)

add_executable(scheduler_service
    ${SCHEDULER_SRCS}
    ${COMMON_SRCS}
    ${STORAGE_SRCS}
)
target_include_directories(scheduler_service PRIVATE include ${PROTO_OUT})
target_link_libraries(scheduler_service PRIVATE
    proto_gen
    gRPC::grpc++
    protobuf::libprotobuf
    SQLite::SQLite3
    spdlog::spdlog
)

# ------------ Worker ------------
file(GLOB WORKER_SRCS src/worker/*.cpp)

add_executable(worker_service
    ${WORKER_SRCS}
    ${COMMON_SRCS}
)
target_include_directories(worker_service PRIVATE include ${PROTO_OUT})
target_link_libraries(worker_service PRIVATE
    proto_gen
    gRPC::grpc++
    protobuf::libprotobuf
    SQLite::SQLite3
    spdlog::spdlog
)

# ------------ Tests ------------
enable_testing()
file(GLOB TEST_SRCS tests/*.cpp)

add_executable(run_tests
    ${TEST_SRCS}
    ${COMMON_SRCS}
    ${STORAGE_SRCS}
)
target_include_directories(run_tests PRIVATE include ${PROTO_OUT})
target_link_libraries(run_tests PRIVATE
    proto_gen
    GTest::gtest_main
    SQLite::SQLite3
    spdlog::spdlog
)

include(GoogleTest)
gtest_discover_tests(run_tests)