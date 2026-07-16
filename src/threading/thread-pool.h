// Copyright 2026 the V8 project authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef V8_THREADING_THREAD_POOL_H_
#define V8_THREADING_THREAD_POOL_H_

#include <vector>
#include <queue>
#include <memory>
#include <atomic>
#include "include/v8.h"
#include "src/base/platform/mutex.h"
#include "src/base/platform/condition-variable.h"
#include "src/base/platform/platform.h"
#include "src/threading/task.h"
#include "src/threading/work-stealing-deque.h"

namespace v8 {
namespace internal {
namespace threading {

extern thread_local int g_worker_index;

class ThreadPool {
 public:
  static ThreadPool* GetInstance();

  explicit ThreadPool(size_t pool_size);
  ~ThreadPool();

  ThreadPool(const ThreadPool&) = delete;
  ThreadPool& operator=(const ThreadPool&) = delete;

  void Submit(ThreadTask* task);
  void SubmitFromWorker(size_t worker_index, ThreadTask* task);
  void SubmitToWorker(size_t worker_index, ThreadTask* task);
  ThreadTask* PopPrivateTask(size_t worker_index);

  // Called by worker threads to get a task
  ThreadTask* GetTask(size_t worker_index);

  void Terminate();

  size_t pool_size() const { return pool_size_; }

 private:
  class WorkerThread : public v8::base::Thread {
   public:
    WorkerThread(ThreadPool* pool, size_t index);
    ~WorkerThread() override = default;

    void Run() override;

   private:
    void ExecuteTask(v8::Isolate* isolate, ThreadTask* task);
    void SerializeAndSetResult(v8::Isolate* isolate, v8::Local<v8::Context> context, v8::TryCatch& try_catch, ThreadTask* task, bool success);

    ThreadPool* pool_;
    size_t worker_index_;
  };

  bool HasWork(size_t worker_index);

  size_t pool_size_;
  std::vector<std::unique_ptr<WorkerThread>> threads_;
  std::vector<std::unique_ptr<WorkStealingDeque<ThreadTask*>>> deques_;

  // Private queues for isolate-bound tasks
  std::vector<std::unique_ptr<v8::base::Mutex>> private_mutexes_;
  std::vector<std::unique_ptr<std::queue<ThreadTask*>>> private_queues_;

  // Global queue for tasks submitted by non-worker threads
  v8::base::Mutex global_queue_mutex_;
  std::queue<ThreadTask*> global_queue_;

  // Thread safety and synchronization
  v8::base::Mutex pool_mutex_;
  v8::base::ConditionVariable work_cv_;
  std::atomic<bool> terminated_{false};
  std::atomic<int> sleeping_workers_{0};
};

}  // namespace threading
}  // namespace internal
}  // namespace v8

#endif  // V8_THREADING_THREAD_POOL_H_
