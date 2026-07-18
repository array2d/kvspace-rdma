// Watch/Notify queue — MPSC value list + watcher linked list.
#pragma once
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace kvspace::store {

using Clock = std::chrono::steady_clock;

// ── value node in MPSC queue ───────────────────────────────────────────────
struct ValueNode {
    std::vector<uint8_t> data;
    ValueNode* next = nullptr;
};

// ── waiter registration ────────────────────────────────────────────────────
struct Watcher {
    std::vector<uint8_t>* out;   // 写入结果
    bool*                ready;  // 唤醒标志
    std::mutex*          mu;
    std::condition_variable* cv;
    Watcher* next = nullptr;
};

// ── per-key queue ──────────────────────────────────────────────────────────
struct WatchEntry {
    std::atomic<ValueNode*> head{nullptr};   // newest value
    std::atomic<Watcher*>   watchers{nullptr}; // waiting clients

    ~WatchEntry() {
        while (head.load()) {
            ValueNode* n = head.load()->next;
            delete head.load();
            head.store(n);
        }
    }
};

// ── watch queue manager ────────────────────────────────────────────────────
class WatchQueue {
public:
    // Notify: push value, wake all waiters
    void notify(const std::string& key, const void* val, size_t val_len) {
        auto& entry = get(key);

        // push value node
        ValueNode* vn = new ValueNode;
        vn->data.assign(static_cast<const uint8_t*>(val),
                         static_cast<const uint8_t*>(val) + val_len);
        vn->next = entry.head.load(std::memory_order_acquire);
        while (!entry.head.compare_exchange_weak(vn->next, vn,
                std::memory_order_release, std::memory_order_acquire)) {}

        // wake all watchers
        Watcher* w = entry.watchers.exchange(nullptr, std::memory_order_acquire);
        while (w) {
            Watcher* next = w->next;
            {
                std::lock_guard lk(*w->mu);
                *w->out = vn->data;
                *w->ready = true;
            }
            w->cv->notify_one();
            w = next;
        }
    }

    // Watch: try pop value, or register waiter with timeout
    bool watch(const std::string& key, std::vector<uint8_t>& out,
               Clock::duration timeout)
    {
        auto& entry = get(key);

        // try pop existing value
        ValueNode* vn = entry.head.load(std::memory_order_acquire);
        while (vn) {
            if (entry.head.compare_exchange_weak(vn, vn->next,
                    std::memory_order_release, std::memory_order_acquire)) {
                out = std::move(vn->data);
                delete vn;
                return true;
            }
        }

        // register waiter
        std::mutex mu;
        std::condition_variable cv;
        bool ready = false;

        Watcher w;
        w.out = &out;
        w.ready = &ready;
        w.mu = &mu;
        w.cv = &cv;

        // push to watchers list
        w.next = entry.watchers.load(std::memory_order_acquire);
        while (!entry.watchers.compare_exchange_weak(w.next, &w,
                std::memory_order_release, std::memory_order_acquire)) {}

        // block with timeout
        std::unique_lock lk(mu);
        if (!cv.wait_for(lk, timeout, [&ready] { return ready; })) {
            // timeout: remove from watchers list (best-effort)
            Watcher* cur = entry.watchers.load(std::memory_order_acquire);
            Watcher* prev = nullptr;
            while (cur) {
                if (cur == &w) {
                    Watcher* next = cur->next;
                    if (prev)
                        prev->next = next;
                    else
                        entry.watchers.store(next, std::memory_order_release);
                    break;
                }
                prev = cur;
                cur = cur->next;
            }
            return false;  // timeout
        }
        return true;
    }

    size_t queue_depth(const std::string& key) const {
        auto it = entries_.find(key);
        if (it == entries_.end()) return 0;
        size_t n = 0;
        ValueNode* vn = it->second.head.load(std::memory_order_acquire);
        while (vn) { n++; vn = vn->next; }
        return n;
    }

private:
    WatchEntry& get(const std::string& key) {
        std::lock_guard lk(mu_);
        return entries_[key];
    }

    mutable std::mutex mu_;
    std::unordered_map<std::string, WatchEntry> entries_;
};

} // namespace kvspace::store
