// Per-core shard: owns store state, processes requests from transport.
#pragma once
#include "handlers.h"
#include "../transport/transport.h"
#include <atomic>
#include <thread>

namespace kvspace::server {

class Shard {
public:
    Shard(int id, size_t slots_per_table)
        : id_(id), store_(slots_per_table) {}

    // 同步处理一个请求
    proto::Frame process(const proto::Frame& req) {
        processed_.fetch_add(1, std::memory_order_relaxed);
        return dispatch(store_, req);
    }

    int id() const { return id_; }
    uint64_t processed() const { return processed_.load(std::memory_order_relaxed); }

private:
    int id_;
    ShardStore store_;
    std::atomic<uint64_t> processed_{0};
};

} // namespace kvspace::server
