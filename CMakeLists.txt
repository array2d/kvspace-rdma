cmake_minimum_required(VERSION 3.16)
project(kvspace-rdma VERSION 0.1.0 LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# ── RDMA (optional) ────────────────────────────────────────────────────────
find_package(libibverbs QUIET)
if(libibverbs_FOUND)
    add_definitions(-DKVSPACE_HAS_RDMA)
    message(STATUS "RDMA: libibverbs found")
else()
    message(STATUS "RDMA: libibverbs not found — UDP-only mode")
endif()

# ── server library (header-only for now) ───────────────────────────────────
add_library(kvspace_rdma_server INTERFACE)
target_include_directories(kvspace_rdma_server INTERFACE include src)
if(libibverbs_FOUND)
    target_link_libraries(kvspace_rdma_server INTERFACE ibverbs)
endif()

# ── server binary ──────────────────────────────────────────────────────────
add_executable(kvspace-rdma src/main.cc)
target_link_libraries(kvspace-rdma kvspace_rdma_server)

# ── tests ──────────────────────────────────────────────────────────────────
enable_testing()

# store unit tests (no I/O, no RDMA hardware needed)
add_executable(store_test test/store_test.cc)
target_link_libraries(store_test kvspace_rdma_server)
add_test(NAME store_test COMMAND store_test)

# proto unit tests
add_executable(proto_test test/proto_test.cc)
target_link_libraries(proto_test kvspace_rdma_server)
add_test(NAME proto_test COMMAND proto_test)

# integration test (UDP, no RDMA needed)
add_executable(integration_test test/integration_test.cc)
target_link_libraries(integration_test kvspace_rdma_server)
add_test(NAME integration_test COMMAND integration_test)
