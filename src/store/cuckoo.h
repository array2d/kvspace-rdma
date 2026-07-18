// Cuckoo hash table — 64B cache-line-aligned slots, seqlock concurrency.
#pragma once
#include "slab.h"
#include <atomic>
#include <cstring>
#include <string>
#include <string_view>
#include <vector>

namespace kvspace::store {

inline uint64_t hash1(std::string_view key) {
    uint64_t h = 14695981039346656037ULL;
    for (char c : key) { h ^= static_cast<uint8_t>(c); h *= 1099511628211ULL; }
    return h;
}
inline uint64_t hash2(uint64_t h1) {
    h1 += 0x9e3779b97f4a7c15ULL;
    h1 = (h1 ^ (h1 >> 30)) * 0xbf58476d1ce4e5b9ULL;
    h1 = (h1 ^ (h1 >> 27)) * 0x94d049bb133111ebULL;
    return h1 ^ (h1 >> 31);
}

struct alignas(64) HashSlot {
    std::atomic<uint32_t> version{0};
    uint16_t key_len = 0;
    uint16_t val_len = 0;
    char     key[24] = {};
    union {
        char     inline_val[24];
        uint64_t heap_ptr;
    };

    bool key_match(std::string_view k) const {
        return key_len == k.size() && std::memcmp(key, k.data(), key_len) == 0;
    }
    bool is_inline() const { return val_len <= 24; }
};

// 可拷贝的 snapshot（eviction 用）
struct SlotSnapshot {
    std::string key;
    std::vector<uint8_t> val;
};

class CuckooTable {
    static constexpr int MAX_EVICT = 500;

public:
    explicit CuckooTable(SlabAllocator* slab, size_t num_slots = 1024 * 1024)
        : slab_(slab) { resize(num_slots); }

    ~CuckooTable() {
        for (size_t i = 0; i < num_slots_; i++) {
            if (!t1_[i].is_inline() && t1_[i].val_len > 24)
                slab_->free(reinterpret_cast<void*>(t1_[i].heap_ptr), t1_[i].val_len);
            if (!t2_[i].is_inline() && t2_[i].val_len > 24)
                slab_->free(reinterpret_cast<void*>(t2_[i].heap_ptr), t2_[i].val_len);
        }
        delete[] t1_; delete[] t2_;
    }

    // ── Get: lock-free read via seqlock ──────────────────────────────────

    bool get(std::string_view key, std::vector<uint8_t>& out) const {
        uint64_t h1 = hash1(key), h2 = hash2(h1);
        if (try_read(t1_[idx(h1)], key, out)) return true;
        if (try_read(t2_[idx(h2)], key, out)) return true;
        return false;
    }

    // ── Set: cuckoo insert with seqlock ──────────────────────────────────

    bool set(std::string_view key, const void* val, size_t val_len) {
        uint64_t h1 = hash1(key), h2 = hash2(h1);
        if (try_write(t1_[idx(h1)], key, val, val_len)) return true;
        if (try_write(t2_[idx(h2)], key, val, val_len)) return true;

        // cuckoo eviction
        SlotSnapshot displaced;
        if (!evict_slot(t1_[idx(h1)], displaced)) {
            grow(); return set(key, val, val_len);
        }

        // write new value into the freed slot
        try_write(t1_[idx(h1)], key, val, val_len);

        uint64_t cur_hash = h1;
        bool in_t1 = false; // displaced is now in hand, we evicted from t1

        for (int i = 0; i < MAX_EVICT; i++) {
            // determine target table for displaced key
            uint64_t dh1 = hash1(displaced.key), dh2 = hash2(dh1);
            HashSlot* target = &t2_[idx(dh2)]; // always try alternate table
            if (in_t1) target = &t1_[idx(dh1)]; else target = &t2_[idx(dh2)];

            in_t1 = !in_t1; // flip for next iteration

            // try direct insert
            if (try_write(*target, displaced.key, displaced.val.data(), displaced.val.size()))
                return true;

            // evict from target
            SlotSnapshot next;
            if (!evict_slot(*target, next)) {
                grow(); return set(key, val, val_len);
            }
            try_write(*target, displaced.key, displaced.val.data(), displaced.val.size());
            displaced = std::move(next);
        }

        grow(); return set(key, val, val_len);
    }

    // ── Del ─────────────────────────────────────────────────────────────

    bool del(std::string_view key) {
        uint64_t h1 = hash1(key), h2 = hash2(h1);
        if (try_delete(t1_[idx(h1)], key)) return true;
        if (try_delete(t2_[idx(h2)], key)) return true;
        return false;
    }

    size_t num_slots() const { return num_slots_; }

private:
    size_t idx(uint64_t h) const { return h % num_slots_; }

    bool try_read(const HashSlot& s, std::string_view key, std::vector<uint8_t>& out) const {
        uint32_t v1 = s.version.load(std::memory_order_acquire);
        if (v1 & 1) return false;
        if (!s.key_match(key)) return false;

        uint16_t vl = s.val_len;
        if (vl <= 24) out.assign(s.inline_val, s.inline_val + vl);
        else { out.resize(vl); std::memcpy(out.data(), reinterpret_cast<const void*>(s.heap_ptr), vl); }

        std::atomic_thread_fence(std::memory_order_acquire);
        uint32_t v2 = s.version.load(std::memory_order_relaxed);
        return v1 == v2;
    }

    bool try_write(HashSlot& s, std::string_view key, const void* val, size_t val_len) {
        // if slot is occupied (non-zero version), must match key for overwrite
        uint32_t v = s.version.load(std::memory_order_acquire);
        if (!(v & 1) && v > 0 && !s.key_match(key)) return false;

        if (v & 1) return false;
        if (!s.version.compare_exchange_strong(v, v + 1,
                std::memory_order_acquire, std::memory_order_relaxed))
            return false;

        s.key_len = static_cast<uint16_t>(key.size());
        std::memcpy(s.key, key.data(), std::min(key.size(), sizeof(s.key)));
        s.val_len = static_cast<uint16_t>(val_len);
        if (val_len <= 24) {
            if (val_len > 0) std::memcpy(s.inline_val, val, val_len);
        } else {
            void* heap = slab_->alloc(val_len);
            std::memcpy(heap, val, val_len);
            s.heap_ptr = reinterpret_cast<uint64_t>(heap);
        }

        s.version.store(v + 2, std::memory_order_release);
        return true;
    }

    // evict: read slot into snapshot, then clear it
    bool evict_slot(HashSlot& s, SlotSnapshot& snap) {
        uint32_t v;
        do {
            v = s.version.load(std::memory_order_acquire);
            if (v & 1) continue; // concurrent write
            if (v == 0) { snap.key.clear(); return true; } // empty slot, no eviction needed

            snap.key.assign(s.key, s.key_len);
            if (s.is_inline())
                snap.val.assign(s.inline_val, s.inline_val + s.val_len);
            else {
                snap.val.resize(s.val_len);
                std::memcpy(snap.val.data(), reinterpret_cast<void*>(s.heap_ptr), s.val_len);
            }
            std::atomic_thread_fence(std::memory_order_acquire);
        } while (v != s.version.load(std::memory_order_relaxed));

        // now delete from slot
        if (!s.version.compare_exchange_strong(v, v + 1,
                std::memory_order_acquire, std::memory_order_relaxed))
            return false;

        if (s.val_len > 24) {
            slab_->free(reinterpret_cast<void*>(s.heap_ptr), s.val_len);
            s.heap_ptr = 0;
        }
        s.key_len = 0;
        s.val_len = 0;
        s.version.store(v + 2, std::memory_order_release);
        return true;
    }

    bool try_delete(HashSlot& s, std::string_view key) {
        uint32_t v = s.version.load(std::memory_order_acquire);
        if (v & 1 || !s.key_match(key)) return false;
        if (!s.version.compare_exchange_strong(v, v + 1,
                std::memory_order_acquire, std::memory_order_relaxed))
            return false;

        uint16_t vl = s.val_len;
        if (vl > 24) slab_->free(reinterpret_cast<void*>(s.heap_ptr), vl);
        s.key_len = 0; s.val_len = 0;
        s.version.store(v + 2, std::memory_order_release);
        return true;
    }

    void grow() { resize(num_slots_ * 2); }

    void resize(size_t n) {
        auto* t1 = new HashSlot[n];
        auto* t2 = new HashSlot[n];
        delete[] t1_; t1_ = t1;
        delete[] t2_; t2_ = t2;
        num_slots_ = n;
    }

    SlabAllocator* slab_;
    HashSlot* t1_ = nullptr;
    HashSlot* t2_ = nullptr;
    size_t num_slots_ = 0;
};

} // namespace kvspace::store
