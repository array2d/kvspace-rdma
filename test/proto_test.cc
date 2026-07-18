// proto layer unit tests.
#include "../src/proto/frame.h"
#include <cassert>
#include <iostream>

using namespace kvspace::proto;

static int passed = 0, failed = 0;

void check(const char* name, bool cond) {
    if (cond) { passed++; }
    else { failed++; std::cerr << "FAIL: " << name << "\n"; }
}

int main() {
    // ── header size ──
    check("header 32 bytes", sizeof(Header) == 32);

    // ── encode/decode roundtrip ──
    {
        Frame req(Op::Get, 42, "/hello/world");
        auto buf = req.encode();
        check("encode non-empty", !buf.empty());
        check("encode size = 32 + key_len", buf.size() == 32 + 12);

        auto decoded = Frame::decode(buf);
        check("decode magic", decoded.header.magic == MAGIC);
        check("decode version", decoded.header.version == VERSION);
        check("decode op", decoded.op_code() == Op::Get);
        check("decode seq", decoded.header.seq == 42);
        check("decode key", decoded.key_str() == "/hello/world");
        check("decode val_len", decoded.header.val_len == 0);
    }

    // ── Set with value ──
    {
        const char* val = "world_data";
        Frame req(Op::Set, 1, "/key", val, 10);
        auto buf = req.encode();
        check("set encode size", buf.size() == 32 + 4 + 10);

        auto decoded = Frame::decode(buf);
        check("set decode key", decoded.key_str() == "/key");
        check("set decode val_len", decoded.header.val_len == 10);
        check("set decode val", std::string(decoded.val_data.begin(), decoded.val_data.end()) == "world_data");
    }

    // ── response helpers ──
    {
        Frame req(Op::Get, 100, "/x");
        auto resp = req.make_ok("data", 4);
        check("resp op", resp.op_code() == Op::Resp_OK);
        check("resp seq", resp.header.seq == 100);

        auto err = req.make_err("oops");
        check("err op", err.op_code() == Op::Resp_Err);
    }

    // ── decode errors ──
    {
        bool caught = false;
        try { Frame::decode(nullptr, 0); }
        catch (const std::exception&) { caught = true; }
        check("decode empty throws", caught);

        caught = false;
        uint8_t bad[32] = {};
        try { Frame::decode(bad, 32); }  // magic != 0x4B56
        catch (const std::exception&) { caught = true; }
        check("decode bad magic throws", caught);
    }

    // ── flags ──
    {
        Frame req(Op::DisConn, 0, "/", nullptr, 0, static_cast<uint16_t>(Flag::TAIL_CALL));
        check("flags TAIL_CALL", req.header.flags == 1);
    }

    std::cout << passed << " passed, " << failed << " failed\n";
    return failed > 0 ? 1 : 0;
}
