// Transport abstraction — unified message-passing interface for RDMA and UDP.
#pragma once
#include "../proto/frame.h"
#include <functional>
#include <memory>
#include <string>

namespace kvspace::transport {

using proto::Frame;

// ── peer address ────────────────────────────────────────────────────────────
struct Peer {
    uint32_t node_id = 0;   // 0 = unknown/single-node
    std::string addr;       // "ip:port"
};

// ── transport interface ─────────────────────────────────────────────────────
class Transport {
public:
    virtual ~Transport() = default;

    // 启动监听。addr 格式: "0.0.0.0:9000"（UDP）或 IB 设备名（RDMA）
    virtual bool listen(const std::string& addr) = 0;

    // 发送响应给指定 peer
    virtual bool send(const Peer& to, const Frame& frame) = 0;

    // 接收请求（阻塞）。out_peer 填充发送方地址
    virtual bool recv(Peer& out_peer, Frame& out_frame) = 0;

    // 关闭
    virtual void close() = 0;

    // 本地地址
    virtual std::string local_addr() const = 0;
};

// ── handler callback ────────────────────────────────────────────────────────
using Handler = std::function<void(const Peer&, const Frame&)>;

// ── factory: auto-detect RDMA availability ──────────────────────────────────
std::unique_ptr<Transport> Create(const std::string& listen_addr);

} // namespace kvspace::transport
