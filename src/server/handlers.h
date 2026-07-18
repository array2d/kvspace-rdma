// Per-op request handlers — 唯一同时依赖 store/ 和 transport/ 的编译单元。
#pragma once
#include "../proto/frame.h"
#include "../store/cuckoo.h"
#include "../store/dir_index.h"
#include "../store/slab.h"
#include "../store/symlink.h"
#include "../store/watch_queue.h"
#include <cstring>
#include <vector>

namespace kvspace::server {

using proto::Frame;
using proto::Op;
using proto::Flag;

struct ShardStore {
    store::CuckooTable     kv;
    store::DirIndex        dir;
    store::WatchQueue      watch;
    store::SymlinkTable    links;
    store::SlabAllocator   slab;

    explicit ShardStore(size_t slots) : kv(&slab, slots) {}
};

// ── response builder ────────────────────────────────────────────────────────

static Frame ok(const Frame& req, const void* val = nullptr, size_t val_len = 0) {
    return req.make_ok(val, val_len);
}
static Frame err(const Frame& req, const char* msg) {
    return req.make_err(msg);
}

// ── key resolution: resolve link prefix, then do the operation ─────────────

inline std::string resolve_key(ShardStore& s, const Frame& req, bool for_delete = false) {
    std::string k = req.key_str();
    if (for_delete && s.links.is_link(k))
        return k; // Del/DelTree: 链接本体操作
    return s.links.resolve_prefix(k);
}

// ── handlers ────────────────────────────────────────────────────────────────

inline Frame handle_get(ShardStore& s, const Frame& req) {
    std::string key = resolve_key(s, req);
    std::vector<uint8_t> val;
    if (s.kv.get(key, val))
        return ok(req, val.data(), val.size());
    return err(req, "not found");
}

inline Frame handle_getmany(ShardStore& s, const Frame& req) {
    // key_data contains len-prefixed keys: [u16 len][key_bytes]...
    const uint8_t* p = req.key_data.data();
    const uint8_t* end = p + req.key_data.size();
    std::vector<uint8_t> resp;
    while (p < end) {
        if (p + 2 > end) break;
        uint16_t klen = (uint16_t(p[0]) << 8) | p[1]; p += 2;
        std::string_view key(reinterpret_cast<const char*>(p), klen); p += klen;
        std::string rk = s.links.resolve_prefix(key);
        std::vector<uint8_t> val;
        if (s.kv.get(rk, val)) {
            resp.push_back(static_cast<uint8_t>(val.size() >> 24));
            resp.push_back(static_cast<uint8_t>(val.size() >> 16));
            resp.push_back(static_cast<uint8_t>(val.size() >> 8));
            resp.push_back(static_cast<uint8_t>(val.size()));
            resp.insert(resp.end(), val.begin(), val.end());
        } else {
            // null: val_len=0xFFFFFFFF
            resp.push_back(0xFF); resp.push_back(0xFF);
            resp.push_back(0xFF); resp.push_back(0xFF);
        }
    }
    return ok(req, resp.data(), resp.size());
}

inline Frame handle_set(ShardStore& s, const Frame& req) {
    std::string key = req.key_str();
    std::string real = s.links.resolve_prefix(key);
    s.kv.set(real, req.val_data.data(), req.val_data.size());
    s.dir.add(real);
    return ok(req);
}

inline Frame handle_setmany(ShardStore& s, const Frame& req) {
    // val_data: [u16 klen][key][u32 vlen][val]...
    const uint8_t* p = req.val_data.data();
    const uint8_t* end = p + req.val_data.size();
    while (p < end) {
        if (p + 2 > end) break;
        uint16_t klen = (uint16_t(p[0]) << 8) | p[1]; p += 2;
        std::string key(reinterpret_cast<const char*>(p), klen); p += klen;
        if (p + 4 > end) break;
        uint32_t vlen = (uint32_t(p[0]) << 24) | (uint32_t(p[1]) << 16) |
                        (uint32_t(p[2]) << 8)  | uint32_t(p[3]); p += 4;
        std::string real = s.links.resolve_prefix(key);
        s.kv.set(real, p, vlen);
        s.dir.add(real);
        p += vlen;
    }
    return ok(req);
}

inline Frame handle_del(ShardStore& s, const Frame& req) {
    std::string key = resolve_key(s, req, true);
    if (s.links.is_link(key)) {
        s.links.unlink(key);
    } else {
        s.kv.del(key);
        s.dir.remove(key);
    }
    return ok(req);
}

inline Frame handle_deltree(ShardStore& s, const Frame& req) {
    std::string prefix = resolve_key(s, req, true);
    if (s.links.is_link(prefix)) {
        s.links.unlink(prefix);
        return ok(req);
    }
    std::vector<std::string> keys;
    s.dir.collect_prefix(prefix, keys);
    // also delete the prefix itself
    s.kv.del(prefix);
    for (const auto& k : keys) {
        s.kv.del(k);
        s.dir.remove(k);
    }
    return ok(req);
}

inline Frame handle_list(ShardStore& s, const Frame& req) {
    std::string prefix = resolve_key(s, req);
    auto children = s.dir.list(prefix);
    std::vector<uint8_t> resp;
    for (const auto& c : children) {
        resp.push_back(static_cast<uint8_t>(c.size() >> 8));
        resp.push_back(static_cast<uint8_t>(c.size()));
        resp.insert(resp.end(), c.begin(), c.end());
    }
    return ok(req, resp.data(), resp.size());
}

inline Frame handle_watch(ShardStore& s, const Frame& req) {
    std::string key = resolve_key(s, req);
    // timeout from val_data, 4 bytes milliseconds LE
    auto timeout_ms = std::chrono::milliseconds(30000);
    if (req.val_data.size() >= 4) {
        uint32_t ms = (uint32_t(req.val_data[0])) |
                      (uint32_t(req.val_data[1]) << 8) |
                      (uint32_t(req.val_data[2]) << 16) |
                      (uint32_t(req.val_data[3]) << 24);
        timeout_ms = std::chrono::milliseconds(ms);
    }
    std::vector<uint8_t> val;
    if (s.watch.watch(key, val, timeout_ms))
        return ok(req, val.data(), val.size());
    return err(req, "timeout");
}

inline Frame handle_notify(ShardStore& s, const Frame& req) {
    std::string key = resolve_key(s, req);
    s.watch.notify(key, req.val_data.data(), req.val_data.size());
    return ok(req);
}

inline Frame handle_link(ShardStore& s, const Frame& req) {
    std::string target = req.key_str();
    std::string linkpath(req.val_data.begin(), req.val_data.end());
    s.links.link(target, linkpath);
    return ok(req);
}

inline Frame handle_unlink(ShardStore& s, const Frame& req) {
    std::string linkpath = req.key_str();
    if (s.links.unlink(linkpath))
        return ok(req);
    return err(req, "not a link");
}

inline Frame handle_disconn(ShardStore&, const Frame& req) {
    return ok(req); // v0: no-op
}

// ── dispatch ────────────────────────────────────────────────────────────────

inline Frame dispatch(ShardStore& s, const Frame& req) {
    switch (req.op_code()) {
        case Op::Get:     return handle_get(s, req);
        case Op::GetMany: return handle_getmany(s, req);
        case Op::Set:     return handle_set(s, req);
        case Op::SetMany: return handle_setmany(s, req);
        case Op::Del:     return handle_del(s, req);
        case Op::DelTree: return handle_deltree(s, req);
        case Op::List:    return handle_list(s, req);
        case Op::Watch:   return handle_watch(s, req);
        case Op::Notify:  return handle_notify(s, req);
        case Op::Link:    return handle_link(s, req);
        case Op::Unlink:  return handle_unlink(s, req);
        case Op::DisConn: return handle_disconn(s, req);
        default:          return err(req, "unknown op");
    }
}

} // namespace kvspace::server
