// Integration test — UDP roundtrip, all 12 operations.
#include "../src/server/server.h"
#include "../src/transport/udp_transport.h"
#include <cassert>
#include <iostream>
#include <thread>
#include <chrono>

using namespace kvspace;

static int passed = 0, failed = 0;
void check(const char* name, bool cond) {
    if (cond) passed++;
    else { failed++; std::cerr << "FAIL: " << name << "\n"; }
}

struct Client {
    transport::UdpTransport t;
    std::string server_addr;

    explicit Client(const std::string& addr) : server_addr(addr) {}

    proto::Frame call(const proto::Frame& req) {
        transport::Peer p;
        p.addr = server_addr;
        t.send(p, req);
        transport::Peer from;
        proto::Frame resp;
        for (int retry = 0; retry < 5; retry++) {
            if (t.recv(from, resp)) return resp;
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        throw std::runtime_error("no response from server");
    }
};

void test_get_set(Client& c) {
    proto::Frame set(proto::Op::Set, 1, "/test/hello", "world", 5);
    auto resp = c.call(set);
    check("set ok", resp.op_code() == proto::Op::Resp_OK);

    proto::Frame get(proto::Op::Get, 2, "/test/hello");
    resp = c.call(get);
    check("get ok", resp.op_code() == proto::Op::Resp_OK);
    check("get value", std::string(resp.val_data.begin(), resp.val_data.end()) == "world");

    proto::Frame gm(proto::Op::Get, 3, "/test/nope");
    resp = c.call(gm);
    check("get missing", resp.op_code() == proto::Op::Resp_Err);
}

void test_del(Client& c) {
    c.call(proto::Frame(proto::Op::Set, 10, "/test/delme", "tmp", 3));
    proto::Frame del(proto::Op::Del, 11, "/test/delme");
    auto resp = c.call(del);
    check("del ok", resp.op_code() == proto::Op::Resp_OK);

    resp = c.call(proto::Frame(proto::Op::Get, 12, "/test/delme"));
    check("del then get → err", resp.op_code() == proto::Op::Resp_Err);
}

void test_list(Client& c) {
    c.call(proto::Frame(proto::Op::Set, 20, "/test/list/a", "1", 1));
    c.call(proto::Frame(proto::Op::Set, 21, "/test/list/b", "2", 1));
    c.call(proto::Frame(proto::Op::Set, 22, "/test/list/c", "3", 1));

    auto resp = c.call(proto::Frame(proto::Op::List, 23, "/test/list"));
    check("list ok", resp.op_code() == proto::Op::Resp_OK);
    check("list non-empty", !resp.val_data.empty());
}

void test_watch_notify(Client& c) {
    proto::Frame nf(proto::Op::Notify, 30, "/test/ch", "event", 5);
    check("notify ok", c.call(nf).op_code() == proto::Op::Resp_OK);

    proto::Frame watch(proto::Op::Watch, 31, "/test/ch");
    uint8_t to[4] = {0xE8, 0x03, 0x00, 0x00}; // 1000ms LE
    watch.val_data.assign(to, to + 4);
    auto resp = c.call(watch);
    check("watch ok", resp.op_code() == proto::Op::Resp_OK);
    check("watch value", std::string(resp.val_data.begin(), resp.val_data.end()) == "event");
}

void test_deltree(Client& c) {
    c.call(proto::Frame(proto::Op::Set, 40, "/test/tree/a", "1", 1));
    c.call(proto::Frame(proto::Op::Set, 41, "/test/tree/b", "2", 1));
    c.call(proto::Frame(proto::Op::Set, 42, "/test/tree/sub/x", "3", 1));

    check("deltree ok", c.call(proto::Frame(proto::Op::DelTree, 43, "/test/tree")).op_code() == proto::Op::Resp_OK);
    check("deltree then get → err", c.call(proto::Frame(proto::Op::Get, 44, "/test/tree/a")).op_code() == proto::Op::Resp_Err);
}

void test_link_unlink(Client& c) {
    // set real data
    c.call(proto::Frame(proto::Op::Set, 50, "/real/data", "secret", 6));

    // Link: key = target, val_data = linkpath. /alias → /real
    proto::Frame link(proto::Op::Link, 51, "/real", "/alias", 6);
    check("link ok", c.call(link).op_code() == proto::Op::Resp_OK);

    // Get via link: /alias/data → /real/data
    auto resp = c.call(proto::Frame(proto::Op::Get, 52, "/alias/data"));
    check("get via link ok", resp.op_code() == proto::Op::Resp_OK);
    check("get via link value", std::string(resp.val_data.begin(), resp.val_data.end()) == "secret");

    // Unlink
    check("unlink ok", c.call(proto::Frame(proto::Op::Unlink, 53, "/alias")).op_code() == proto::Op::Resp_OK);
    // Get should still work on real path
    check("get real after unlink", c.call(proto::Frame(proto::Op::Get, 54, "/real/data")).op_code() == proto::Op::Resp_OK);
}

void test_setmany_getmany(Client& c) {
    // SetMany: val_data = [u16 klen][key][u32 vlen][val] repeated
    std::vector<uint8_t> data;
    auto add_kv = [&](const std::string& k, const std::string& v) {
        data.push_back(static_cast<uint8_t>(k.size() >> 8));
        data.push_back(static_cast<uint8_t>(k.size()));
        data.insert(data.end(), k.begin(), k.end());
        data.push_back(static_cast<uint8_t>(v.size() >> 24));
        data.push_back(static_cast<uint8_t>(v.size() >> 16));
        data.push_back(static_cast<uint8_t>(v.size() >> 8));
        data.push_back(static_cast<uint8_t>(v.size()));
        data.insert(data.end(), v.begin(), v.end());
    };
    add_kv("/sm/a", "111");
    add_kv("/sm/b", "222");

    proto::Frame req(proto::Op::SetMany, 60, "/sm", data.data(), data.size());
    check("setmany ok", c.call(req).op_code() == proto::Op::Resp_OK);

    // Get each individually
    auto r1 = c.call(proto::Frame(proto::Op::Get, 61, "/sm/a"));
    check("sm get a", std::string(r1.val_data.begin(), r1.val_data.end()) == "111");

    auto r2 = c.call(proto::Frame(proto::Op::Get, 62, "/sm/b"));
    check("sm get b", std::string(r2.val_data.begin(), r2.val_data.end()) == "222");
}

int main() {
    server::Server::Config cfg;
    cfg.listen_addr = "127.0.0.1:19001";
    cfg.num_shards = 1;
    cfg.slots_per_shard = 64 * 1024;

    server::Server srv(cfg);
    srv.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    Client c("127.0.0.1:19001");

    test_get_set(c);
    test_del(c);
    test_list(c);
    test_watch_notify(c);
    test_deltree(c);
    test_link_unlink(c);
    test_setmany_getmany(c);

    srv.stop();
    std::cout << "server processed " << srv.total_processed() << " requests\n";
    std::cout << passed << " passed, " << failed << " failed\n";
    return failed > 0 ? 1 : 0;
}
