// kvspace-rdma wire protocol frame.
// 32-byte header + variable-length key + variable-length value payload.
#pragma once
#include "op_codes.h"
#include <cstring>
#include <string>
#include <string_view>
#include <vector>
#include <stdexcept>

namespace kvspace::proto {

// ── 32-byte wire header ──────────────────────────────────────────────────────

#pragma pack(push, 1)
struct Header {
    uint16_t magic   = MAGIC;
    uint8_t  version = VERSION;
    uint8_t  op      = 0;
    uint32_t seq     = 0;       // client seq number, server 原样返回
    uint16_t key_len = 0;
    uint32_t val_len = 0;
    uint16_t flags   = 0;
    uint8_t  reserved[16] = {};
};
#pragma pack(pop)
static_assert(sizeof(Header) == 32, "Header must be exactly 32 bytes");

// ── Frame: header + payload ──────────────────────────────────────────────────

struct Frame {
    Header header;
    std::vector<uint8_t> key_data;
    std::vector<uint8_t> val_data;

    Frame() = default;

    Frame(Op op, uint32_t seq, std::string_view key,
          const void* val = nullptr, size_t val_len = 0, uint16_t flags = 0)
        : key_data(key.begin(), key.end())
        , val_data(static_cast<const uint8_t*>(val),
                    static_cast<const uint8_t*>(val) + val_len)
    {
        header.magic   = MAGIC;
        header.version = VERSION;
        header.op      = static_cast<uint8_t>(op);
        header.seq     = seq;
        header.key_len = static_cast<uint16_t>(key_data.size());
        header.val_len = static_cast<uint32_t>(val_data.size());
        header.flags   = flags;
    }

    std::string key_str() const {
        return std::string(reinterpret_cast<const char*>(key_data.data()), key_data.size());
    }

    // ── encode/decode ──────────────────────────────────────────────────────

    std::vector<uint8_t> encode() const {
        size_t total = sizeof(Header) + key_data.size() + val_data.size();
        std::vector<uint8_t> buf(total);
        std::memcpy(buf.data(), &header, sizeof(Header));
        if (!key_data.empty())
            std::memcpy(buf.data() + sizeof(Header), key_data.data(), key_data.size());
        if (!val_data.empty())
            std::memcpy(buf.data() + sizeof(Header) + key_data.size(), val_data.data(), val_data.size());
        return buf;
    }

    static Frame decode(const uint8_t* data, size_t len) {
        if (len < sizeof(Header)) throw std::runtime_error("frame too short");
        Header h;
        std::memcpy(&h, data, sizeof(Header));
        if (h.magic != MAGIC) throw std::runtime_error("bad magic");
        size_t total = sizeof(Header) + h.key_len + h.val_len;
        if (len < total) throw std::runtime_error("frame truncated");

        Frame f;
        f.header = h;
        if (h.key_len > 0) {
            const uint8_t* k = data + sizeof(Header);
            f.key_data.assign(k, k + h.key_len);
        }
        if (h.val_len > 0) {
            const uint8_t* v = data + sizeof(Header) + h.key_len;
            f.val_data.assign(v, v + h.val_len);
        }
        return f;
    }

    static Frame decode(const std::vector<uint8_t>& buf) {
        return decode(buf.data(), buf.size());
    }

    // ── helpers ───────────────────────────────────────────────────────────

    Op op_code() const { return static_cast<Op>(header.op); }

    Frame make_response(Op resp_op, const void* val = nullptr, size_t val_len = 0, uint16_t flags = 0) const {
        Frame resp(resp_op, header.seq, key_str(), val, val_len, flags);
        return resp;
    }

    Frame make_ok(const void* val = nullptr, size_t val_len = 0, uint16_t flags = 0) const {
        return make_response(Op::Resp_OK, val, val_len, flags);
    }

    Frame make_err(const char* msg) const {
        return make_response(Op::Resp_Err, msg, std::strlen(msg));
    }
};

} // namespace kvspace::proto
