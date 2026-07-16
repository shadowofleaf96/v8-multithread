// Copyright 2026 the V8 project authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef V8_THREADING_WORK_STEALING_DEQUE_H_
#define V8_THREADING_WORK_STEALING_DEQUE_H_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <vector>

#include "src/base/macros.h"

namespace v8 {
namespace internal {
namespace threading {

// Dynamic, lock-free Chase-Lev Work-Stealing Deque.
// Single-producer (owner thread pushes and pops from the bottom/LIFO end)
// Multi-consumer (thief threads steal from the top/FIFO end)
template <typename T>
class WorkStealingDeque {
  static_assert(std::is_trivially_copyable<T>::value,
                "T must be trivially copyable for use in atomic circular buffer");

 public:
  explicit WorkStealingDeque(size_t initial_capacity = 64) {
    size_t cap = 1;
    while (cap < initial_capacity) {
      cap <<= 1;
    }
    active_array_.store(new Array(cap), std::memory_order_relaxed);
  }

  ~WorkStealingDeque() {
    Array* arr = active_array_.load(std::memory_order_relaxed);
    delete arr;
    for (Array* old : obsolete_arrays_) {
      delete old;
    }
  }

  WorkStealingDeque(const WorkStealingDeque&) = delete;
  WorkStealingDeque& operator=(const WorkStealingDeque&) = delete;

  // Push an item to the bottom (LIFO end). Only owner thread may call this.
  void Push(T item) {
    int64_t b = bottom_.load(std::memory_order_relaxed);
    int64_t t = top_.load(std::memory_order_acquire);
    Array* a = active_array_.load(std::memory_order_relaxed);

    if (b - t > static_cast<int64_t>(a->capacity() - 1)) {
      // Array is full, resize it
      Resize(b, t, a);
      a = active_array_.load(std::memory_order_relaxed);
    }

    a->Set(b, item);
    std::atomic_thread_fence(std::memory_order_release);
    bottom_.store(b + 1, std::memory_order_relaxed);
  }

  // Pop an item from the bottom (LIFO end). Only owner thread may call this.
  bool Pop(T* item) {
    int64_t b = bottom_.load(std::memory_order_relaxed) - 1;
    Array* a = active_array_.load(std::memory_order_relaxed);
    bottom_.store(b, std::memory_order_relaxed);
    std::atomic_thread_fence(std::memory_order_seq_cst);

    int64_t t = top_.load(std::memory_order_relaxed);
    if (t <= b) {
      // Deque is not empty
      T val = a->Get(b);
      if (t == b) {
        // Single element remaining, race with thieves
        if (!top_.compare_exchange_strong(t, t + 1, std::memory_order_seq_cst,
                                         std::memory_order_relaxed)) {
          // Thief won the race
          bottom_.store(b + 1, std::memory_order_relaxed);
          return false;
        }
        bottom_.store(b + 1, std::memory_order_relaxed);
        *item = val;
        return true;
      }
      *item = val;
      return true;
    } else {
      // Deque is empty
      bottom_.store(b + 1, std::memory_order_relaxed);
      return false;
    }
  }

  // Steal an item from the top (FIFO end). Any concurrent thief thread may call this.
  bool Steal(T* item) {
    while (true) {
      int64_t t = top_.load(std::memory_order_acquire);
      std::atomic_thread_fence(std::memory_order_seq_cst);
      int64_t b = bottom_.load(std::memory_order_acquire);

      if (t < b) {
        // Deque is not empty
        Array* a = active_array_.load(std::memory_order_acquire);
        T val = a->Get(t);

        if (top_.compare_exchange_strong(t, t + 1, std::memory_order_seq_cst,
                                         std::memory_order_relaxed)) {
          *item = val;
          return true;
        }
        // Lost race to another thief or owner; retry
        continue;
      } else {
        return false;
      }
    }
  }

  // Return approximation of size
  size_t Size() const {
    int64_t b = bottom_.load(std::memory_order_relaxed);
    int64_t t = top_.load(std::memory_order_relaxed);
    return b > t ? static_cast<size_t>(b - t) : 0;
  }

  bool IsEmpty() const {
    return Size() == 0;
  }

 private:
  struct Array {
    explicit Array(size_t cap) : cap_(cap), mask_(cap - 1) {
      buffer_ = new std::atomic<T>[cap];
    }

    ~Array() {
      delete[] buffer_;
    }

    size_t capacity() const { return cap_; }

    T Get(int64_t index) {
      return buffer_[index & mask_].load(std::memory_order_relaxed);
    }

    void Set(int64_t index, T val) {
      buffer_[index & mask_].store(val, std::memory_order_relaxed);
    }

   private:
    size_t cap_;
    size_t mask_;
    std::atomic<T>* buffer_;
  };

  // Resize function. Only called by the owner thread inside Push()
  void Resize(int64_t b, int64_t t, Array* old_arr) {
    size_t new_cap = old_arr->capacity() * 2;
    Array* new_arr = new Array(new_cap);
    for (int64_t i = t; i < b; ++i) {
      new_arr->Set(i, old_arr->Get(i));
    }
    active_array_.store(new_arr, std::memory_order_release);
    obsolete_arrays_.push_back(old_arr); // Safe to delete in destructor
  }

  std::atomic<int64_t> top_{0};
  std::atomic<int64_t> bottom_{0};
  std::atomic<Array*> active_array_;
  std::vector<Array*> obsolete_arrays_;
};

}  // namespace threading
}  // namespace internal
}  // namespace v8

#endif  // V8_THREADING_WORK_STEALING_DEQUE_H_
