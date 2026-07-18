// Slab allocator for value heap — zero malloc in hot path.
#pragma once
#include <atomic>
#include <cstdint>
#include <mutex>
#include <vector>

namespace kvspace::store {

inline constexpr size_t SLAB_SIZES[] = {
    32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536
};
inline constexpr size_t SLAB_COUNT = sizeof(SLAB_SIZES) / sizeof(SLAB_SIZES[0]);
inline constexpr size_t SLOTS_PER_CHUNK = 256;

struct FreeSlot { FreeSlot* next; };

class SlabClass {
public:
    explicit SlabClass(size_t slot_size) : slot_size_(slot_size) {
        min_size_ = slot_size < sizeof(FreeSlot) ? sizeof(FreeSlot) : slot_size;
    }

    void* alloc() {
        FreeSlot* s = head_.load(std::memory_order_acquire);
        while (s) {
            if (head_.compare_exchange_weak(s, s->next,
                    std::memory_order_release, std::memory_order_acquire))
                return s;
        }
        return alloc_chunk();
    }

    void free(void* p) {
        FreeSlot* s = static_cast<FreeSlot*>(p);
        s->next = head_.load(std::memory_order_acquire);
        while (!head_.compare_exchange_weak(s->next, s,
                std::memory_order_release, std::memory_order_acquire)) {}
    }

    size_t slot_size() const { return min_size_; }

private:
    void* alloc_chunk() {
        std::lock_guard<std::mutex> lk(chunk_mutex_);
        FreeSlot* s = head_.load(std::memory_order_acquire);
        if (s) return alloc();

        size_t chunk_bytes = min_size_ * SLOTS_PER_CHUNK;
        char* raw = new char[chunk_bytes];
        chunks_.push_back(raw);

        for (size_t i = 0; i < SLOTS_PER_CHUNK - 1; i++) {
            auto* cur = reinterpret_cast<FreeSlot*>(raw + i * min_size_);
            cur->next = reinterpret_cast<FreeSlot*>(raw + (i + 1) * min_size_);
        }
        auto* last = reinterpret_cast<FreeSlot*>(raw + (SLOTS_PER_CHUNK - 1) * min_size_);
        last->next = nullptr;

        head_.store(reinterpret_cast<FreeSlot*>(raw), std::memory_order_release);
        return alloc();
    }

    size_t slot_size_, min_size_;
    std::atomic<FreeSlot*> head_{nullptr};
    std::mutex chunk_mutex_;
    std::vector<char*> chunks_;
};

class SlabAllocator {
public:
    SlabAllocator() {
        for (size_t i = 0; i < SLAB_COUNT; i++)
            classes_[i] = new SlabClass(SLAB_SIZES[i]);
    }
    ~SlabAllocator() {
        for (size_t i = 0; i < SLAB_COUNT; i++) delete classes_[i];
    }

    void* alloc(size_t size) {
        if (size == 0) return nullptr;
        return classes_[size_to_idx(size)]->alloc();
    }
    void free(void* p, size_t size) {
        if (!p || size == 0) return;
        classes_[size_to_idx(size)]->free(p);
    }

    static int size_to_idx(size_t size) {
        for (size_t i = 0; i < SLAB_COUNT; i++)
            if (SLAB_SIZES[i] >= size) return static_cast<int>(i);
        return static_cast<int>(SLAB_COUNT - 1);
    }

private:
    SlabClass* classes_[SLAB_COUNT];
};

} // namespace kvspace::store
