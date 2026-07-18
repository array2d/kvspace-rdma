// Main server: owns transport + shards, runs recv→dispatch→reply loop.
#pragma once
#include "shard.h"
#include "../transport/transport.h"
#include "../transport/udp_transport.h"
#include <atomic>
#include <csignal>
#include <memory>
#include <thread>
#include <vector>

namespace kvspace::server {

class Server {
public:
    struct Config {
        std::string listen_addr = "0.0.0.0:9000";
        int    num_shards = 1;          // v0: single-thread
        size_t slots_per_shard = 1024 * 1024; // 1M slots
        int    poll_timeout_ms = 0;     // 0 = blocking, >0 = polling
    };

    explicit Server(const Config& cfg) : cfg_(cfg) {
        transport_ = transport::Create(cfg.listen_addr);
        if (cfg.num_shards < 1) cfg_.num_shards = 1;
        for (int i = 0; i < cfg_.num_shards; i++)
            shards_.emplace_back(std::make_unique<Shard>(i, cfg.slots_per_shard));
    }

    ~Server() { stop(); }

    void start() {
        running_.store(true, std::memory_order_release);
        // v0: single-thread recv→dispatch→reply loop
        thread_ = std::thread([this] { run_loop(); });
    }

    void stop() {
        running_.store(false, std::memory_order_release);
        if (transport_) transport_->close(); // unblock recvfrom
        if (thread_.joinable()) thread_.join();
    }

    uint64_t total_processed() const {
        uint64_t n = 0;
        for (const auto& s : shards_) n += s->processed();
        return n;
    }

    const Config& config() const { return cfg_; }

private:
    void run_loop() {
        // set non-blocking if polling
        auto* udp = dynamic_cast<transport::UdpTransport*>(transport_.get());
        if (udp && cfg_.poll_timeout_ms > 0)
            udp->set_nonblocking(true);

        while (running_.load(std::memory_order_acquire)) {
            transport::Peer peer;
            proto::Frame req;
            if (!transport_->recv(peer, req)) {
                if (!running_.load(std::memory_order_acquire)) break; // stopped
                if (cfg_.poll_timeout_ms > 0) continue;
                break;
            }

            // route to shard (v0: shard 0 only; v1: hash→shard)
            int shard_id = 0;
            if (cfg_.num_shards > 1) {
                uint64_t h = store::hash1(req.key_str());
                shard_id = static_cast<int>(h % cfg_.num_shards);
            }

            auto resp = shards_[shard_id]->process(req);
            transport_->send(peer, resp);
        }
    }

    Config cfg_;
    std::unique_ptr<transport::Transport> transport_;
    std::vector<std::unique_ptr<Shard>> shards_;
    std::atomic<bool> running_{false};
    std::thread thread_;
};

} // namespace kvspace::server
