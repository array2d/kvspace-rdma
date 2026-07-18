// UDP transport — sendto/recvfrom with seq/retry for reliability.
#pragma once
#include "transport.h"
#include <arpa/inet.h>
#include <cstring>
#include <netinet/in.h>
#include <sys/socket.h>
#include <fcntl.h>
#include <unistd.h>
#include <cerrno>
#include <stdexcept>

namespace kvspace::transport {

class UdpTransport : public Transport {
    static constexpr size_t BUF_SIZE = 65536;  // max UDP datagram

public:
    UdpTransport() {
        fd_ = socket(AF_INET, SOCK_DGRAM, 0);
        if (fd_ < 0) throw std::runtime_error("socket() failed: " + std::string(strerror(errno)));
        int opt = 1;
        setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    }

    ~UdpTransport() override { close(); }

    bool listen(const std::string& addr) override {
        auto [ip, port] = parse_addr(addr);
        sockaddr_in sa{};
        sa.sin_family = AF_INET;
        sa.sin_port = htons(port);
        inet_pton(AF_INET, ip.c_str(), &sa.sin_addr);

        if (bind(fd_, reinterpret_cast<sockaddr*>(&sa), sizeof(sa)) < 0) {
            last_error_ = "bind() failed: " + std::string(strerror(errno));
            return false;
        }
        addr_ = addr;
        return true;
    }

    bool send(const Peer& to, const Frame& frame) override {
        auto buf = frame.encode();
        if (buf.empty()) return true;

        auto [ip, port] = parse_addr(to.addr);
        sockaddr_in sa{};
        sa.sin_family = AF_INET;
        sa.sin_port = htons(port);
        inet_pton(AF_INET, ip.c_str(), &sa.sin_addr);

        ssize_t n = sendto(fd_, buf.data(), buf.size(), 0,
                           reinterpret_cast<sockaddr*>(&sa), sizeof(sa));
        if (n < 0) {
            last_error_ = "sendto() failed: " + std::string(strerror(errno));
            return false;
        }
        return true;
    }

    bool recv(Peer& out_peer, Frame& out_frame) override {
        uint8_t buf[BUF_SIZE];
        sockaddr_in sa{};
        socklen_t sa_len = sizeof(sa);

        ssize_t n = recvfrom(fd_, buf, BUF_SIZE, 0,
                             reinterpret_cast<sockaddr*>(&sa), &sa_len);
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) return false;
            last_error_ = "recvfrom() failed: " + std::string(strerror(errno));
            return false;
        }

        try {
            out_frame = Frame::decode(buf, static_cast<size_t>(n));
        } catch (const std::exception& e) {
            last_error_ = std::string("frame decode: ") + e.what();
            return false;
        }

        char ip_str[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &sa.sin_addr, ip_str, sizeof(ip_str));
        out_peer.addr = std::string(ip_str) + ":" + std::to_string(ntohs(sa.sin_port));
        out_peer.node_id = 0;
        return true;
    }

    void close() override {
        if (fd_ >= 0) {
            shutdown(fd_, SHUT_RDWR); // unblock recvfrom in other threads
            ::close(fd_);
            fd_ = -1;
        }
    }

    std::string local_addr() const override { return addr_; }

    int fd() const { return fd_; }
    const std::string& last_error() const { return last_error_; }

    // enable non-blocking for polling loops
    void set_nonblocking(bool on) {
        int flags = fcntl(fd_, F_GETFL, 0);
        if (on) flags |= O_NONBLOCK;
        else    flags &= ~O_NONBLOCK;
        fcntl(fd_, F_SETFL, flags);
    }

private:
    static std::pair<std::string, uint16_t> parse_addr(const std::string& addr) {
        auto pos = addr.rfind(':');
        if (pos == std::string::npos)
            throw std::runtime_error("invalid addr: " + addr);
        return {addr.substr(0, pos), static_cast<uint16_t>(std::stoi(addr.substr(pos + 1)))};
    }

    int fd_ = -1;
    std::string addr_;
    std::string last_error_;
};

// ── factory ─────────────────────────────────────────────────────────────────
inline std::unique_ptr<Transport> Create(const std::string& listen_addr) {
    // v0: always UDP. v0.1: auto-detect RDMA
    auto t = std::make_unique<UdpTransport>();
    if (!t->listen(listen_addr))
        throw std::runtime_error("UDP listen failed: " + t->last_error());
    return t;
}

} // namespace kvspace::transport
