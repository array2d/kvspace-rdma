// store layer unit tests — no I/O, no RDMA hardware needed.
#include "../src/store/slab.h"
#include "../src/store/cuckoo.h"
#include "../src/store/dir_index.h"
#include "../src/store/watch_queue.h"
#include "../src/store/symlink.h"
#include <cassert>
#include <iostream>
#include <thread>

using namespace kvspace::store;

static int passed = 0, failed = 0;
void check(const char* name, bool cond) {
    if (cond) passed++;
    else { failed++; std::cerr << "FAIL: " << name << "\n"; }
}

// ── slab allocator ──────────────────────────────────────────────────────────

void test_slab() {
    SlabAllocator slab;

    // basic alloc/free
    void* p1 = slab.alloc(100);
    check("slab alloc 100", p1 != nullptr);
    slab.free(p1, 100);

    // multiple allocs
    void* a = slab.alloc(32);
    void* b = slab.alloc(32);
    check("slab two allocs different", a != b);
    slab.free(a, 32);
    slab.free(b, 32);

    // large allocation
    void* big = slab.alloc(8192);
    check("slab alloc 8K", big != nullptr);
    slab.free(big, 8192);

    // zero-size
    check("slab alloc 0", slab.alloc(0) == nullptr);

    // size_to_idx
    check("size_to_idx 32 → 0", SlabAllocator::size_to_idx(32) == 0);
    check("size_to_idx 64 → 1", SlabAllocator::size_to_idx(64) == 1);
    check("size_to_idx 100 → 2 (128)", SlabAllocator::size_to_idx(100) == 2);
    check("size_to_idx 65536 → 11", SlabAllocator::size_to_idx(65536) == 11);
    check("size_to_idx 100000 → 11", SlabAllocator::size_to_idx(100000) == 11);
}

// ── cuckoo hash ─────────────────────────────────────────────────────────────

void test_cuckoo() {
    SlabAllocator slab;
    CuckooTable t(&slab, 64 * 1024); // 64K slots

    // set + get (inline)
    check("set hello", t.set("hello", "world", 5));
    std::vector<uint8_t> val;
    check("get hello", t.get("hello", val));
    check("get hello value", std::string(val.begin(), val.end()) == "world");

    // get missing
    check("get missing", !t.get("nope", val));

    // overwrite
    check("set hello v2", t.set("hello", "v2_data", 7));
    check("get hello v2", t.get("hello", val));
    check("get hello v2 value", std::string(val.begin(), val.end()) == "v2_data");

    // delete
    check("del hello", t.del("hello"));
    check("get after del", !t.get("hello", val));

    // delete missing
    check("del missing", !t.del("never_there"));

    // large value (heap path)
    std::vector<uint8_t> large(1024, 0xAB);
    check("set large", t.set("large_key", large.data(), large.size()));
    std::vector<uint8_t> out;
    check("get large", t.get("large_key", out));
    check("get large size", out.size() == 1024);
    check("get large content", out[0] == 0xAB && out[1023] == 0xAB);

    // many keys
    char key_buf[64];
    for (int i = 0; i < 1000; i++) {
        snprintf(key_buf, sizeof(key_buf), "key_%06d", i);
        check("set many", t.set(key_buf, &i, sizeof(i)));
    }
    for (int i = 0; i < 1000; i++) {
        snprintf(key_buf, sizeof(key_buf), "key_%06d", i);
        check("get many", t.get(key_buf, out));
        int v = *reinterpret_cast<const int*>(out.data());
        check("get many value", v == i);
    }

    // zero-length value
    check("set empty", t.set("empty_val", nullptr, 0));
    check("get empty", t.get("empty_val", out));
    check("get empty size", out.empty());
}

// ── directory index ─────────────────────────────────────────────────────────

void test_dir_index() {
    DirIndex idx;

    idx.add("/a/x");
    idx.add("/a/y");
    idx.add("/a/z");
    idx.add("/b/w");

    auto a_children = idx.list("/a");
    check("list /a size", a_children.size() == 3);
    check("list /a sorted", a_children[0] == "x" && a_children[1] == "y" && a_children[2] == "z");

    auto b_children = idx.list("/b");
    check("list /b size", b_children.size() == 1);
    check("list /b val", b_children[0] == "w");

    auto empty = idx.list("/nonexist");
    check("list nonexist", empty.empty());

    // remove
    idx.remove("/a/y");
    auto after = idx.list("/a");
    check("list after del", after.size() == 2);
    check("list after del sorted", after[0] == "x" && after[1] == "z");

    // collect_prefix for DelTree
    idx.add("/a/sub/p");
    idx.add("/a/sub/q");
    std::vector<std::string> collected;
    idx.collect_prefix("/a", collected);
    // should include /a/sub/p, /a/sub/q but NOT /b/w
    bool has_p = false, has_q = false, has_w = false;
    for (const auto& k : collected) {
        if (k == "/a/sub/p") has_p = true;
        if (k == "/a/sub/q") has_q = true;
        if (k == "/b/w") has_w = true;
    }
    check("collect /a has /a/sub/p", has_p);
    check("collect /a has /a/sub/q", has_q);
    check("collect /a no /b/w", !has_w);
}

// ── watch queue ─────────────────────────────────────────────────────────────

void test_watch_queue() {
    WatchQueue wq;

    // notify → watch should succeed immediately
    wq.notify("ch1", "msg1", 4);
    std::vector<uint8_t> out;
    bool ok = wq.watch("ch1", out, std::chrono::milliseconds(100));
    check("watch after notify", ok);
    check("watch value", std::string(out.begin(), out.end()) == "msg1");

    // watch with timeout on empty key
    auto start = Clock::now();
    ok = wq.watch("empty", out, std::chrono::milliseconds(50));
    auto elapsed = Clock::now() - start;
    check("watch timeout", !ok);
    check("watch timeout elapsed", elapsed >= std::chrono::milliseconds(40));

    // notify wakes blocked watcher
    std::atomic<bool> waiter_done{false};
    std::vector<uint8_t> waiter_out;
    std::thread waiter([&]() {
        WatchQueue local;
        std::vector<uint8_t> o;
        local.notify("ready", "go", 2);
        waiter_done = true;
    });
    waiter.join();
    check("waiter done", waiter_done.load());

    // multiple notifies
    wq.notify("multi", "a", 1);
    wq.notify("multi", "b", 1);
    wq.notify("multi", "c", 1);
    check("queue depth 3", wq.queue_depth("multi") == 3);

    // LIFO order (stack semantics): last notify is first watch
    ok = wq.watch("multi", out, std::chrono::milliseconds(10));
    check("pop c (LIFO)", ok);
    check("pop c val", std::string(out.begin(), out.end()) == "c");
    check("queue depth 2", wq.queue_depth("multi") == 2);

    ok = wq.watch("multi", out, std::chrono::milliseconds(10));
    check("pop b", ok);
    ok = wq.watch("multi", out, std::chrono::milliseconds(10));
    check("pop a", ok);
    check("queue depth 0", wq.queue_depth("multi") == 0);
}

// ── symlink ─────────────────────────────────────────────────────────────────

void test_symlink() {
    SymlinkTable links;

    links.link("/real/path", "/alias");
    check("link count", links.count() == 1);

    check("/alias is_link", links.is_link("/alias"));
    check("/real not link", !links.is_link("/real/path"));

    // resolve_prefix
    check("resolve /alias → /real/path", links.resolve_prefix("/alias") == "/real/path");
    check("resolve /alias/x → /real/path/x", links.resolve_prefix("/alias/x") == "/real/path/x");
    check("resolve /other → /other (no link)", links.resolve_prefix("/other") == "/other");

    // link to another link (chain)
    links.link("/real/path", "/shortcut");
    check("chain: /shortcut → /real/path", links.resolve_prefix("/shortcut") == "/real/path");

    // unlink
    check("unlink /alias", links.unlink("/alias"));
    check("unlink count", links.count() == 1);
    check("unlink missing", !links.unlink("/nope"));

    // after unlink, path not resolved
    check("after unlink /alias/x → /alias/x", links.resolve_prefix("/alias/x") == "/alias/x");
}

// ── main ────────────────────────────────────────────────────────────────────

int main() {
    test_slab();
    test_cuckoo();
    test_dir_index();
    test_watch_queue();
    test_symlink();

    std::cout << passed << " passed, " << failed << " failed\n";
    return failed > 0 ? 1 : 0;
}
