// Copyright 2026 the V8 project authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "src/threading/thread-pool.h"
#include "include/v8-locker.h"
#include "include/v8-value-serializer.h"
#include "src/threading/serializer.h"
#include "src/base/logging.h"
#include "src/base/platform/time.h"
#include <algorithm>
#include <thread>

namespace v8 {
namespace internal {
namespace threading {

thread_local int g_worker_index = -1;

ThreadPool* ThreadPool::GetInstance() {
  static ThreadPool* instance = new ThreadPool(
      std::max(size_t(1), static_cast<size_t>(std::thread::hardware_concurrency())));
  return instance;
}

ThreadPool::ThreadPool(size_t pool_size) : pool_size_(pool_size) {
  deques_.reserve(pool_size_);
  threads_.reserve(pool_size_);
  private_mutexes_.reserve(pool_size_);
  private_queues_.reserve(pool_size_);
  for (size_t i = 0; i < pool_size_; ++i) {
    deques_.push_back(std::make_unique<WorkStealingDeque<ThreadTask*>>());
    threads_.push_back(std::make_unique<WorkerThread>(this, i));
    private_mutexes_.push_back(std::make_unique<v8::base::Mutex>());
    private_queues_.push_back(std::make_unique<std::queue<ThreadTask*>>());
  }
  for (size_t i = 0; i < pool_size_; ++i) {
    bool started = threads_[i]->Start();
    CHECK(started);
  }
}

ThreadPool::~ThreadPool() {
  Terminate();

  // Clean up remaining tasks in global queue
  while (!global_queue_.empty()) {
    ThreadTask* task = global_queue_.front();
    global_queue_.pop();
    delete task;
  }

  // Clean up remaining tasks in work-stealing deques
  for (size_t i = 0; i < pool_size_; ++i) {
    ThreadTask* task;
    while (deques_[i]->Pop(&task)) {
      delete task;
    }
  }

  // Clean up remaining tasks in private queues
  for (size_t i = 0; i < pool_size_; ++i) {
    v8::base::MutexGuard private_guard(private_mutexes_[i].get());
    while (!private_queues_[i]->empty()) {
      ThreadTask* task = private_queues_[i]->front();
      private_queues_[i]->pop();
      delete task;
    }
  }
}

void ThreadPool::Submit(ThreadTask* task) {
  {
    v8::base::MutexGuard global_guard(&global_queue_mutex_);
    global_queue_.push(task);
  }
  if (sleeping_workers_.load(std::memory_order_seq_cst) > 0) {
    v8::base::MutexGuard guard(&pool_mutex_);
    work_cv_.NotifyAll();
  }
}

void ThreadPool::SubmitFromWorker(size_t worker_index, ThreadTask* task) {
  deques_[worker_index]->Push(task);
  if (sleeping_workers_.load(std::memory_order_seq_cst) > 0) {
    v8::base::MutexGuard guard(&pool_mutex_);
    work_cv_.NotifyOne();
  }
}

void ThreadPool::SubmitToWorker(size_t worker_index, ThreadTask* task) {
  DCHECK_LT(worker_index, pool_size_);
  {
    v8::base::MutexGuard private_guard(private_mutexes_[worker_index].get());
    private_queues_[worker_index]->push(task);
  }
  if (sleeping_workers_.load(std::memory_order_seq_cst) > 0) {
    v8::base::MutexGuard guard(&pool_mutex_);
    work_cv_.NotifyAll();
  }
}

ThreadTask* ThreadPool::PopPrivateTask(size_t worker_index) {
  DCHECK_LT(worker_index, pool_size_);
  v8::base::MutexGuard private_guard(private_mutexes_[worker_index].get());
  if (!private_queues_[worker_index]->empty()) {
    ThreadTask* task = private_queues_[worker_index]->front();
    private_queues_[worker_index]->pop();
    return task;
  }
  return nullptr;
}

bool ThreadPool::HasWork(size_t worker_index) {
  {
    v8::base::MutexGuard private_guard(private_mutexes_[worker_index].get());
    if (!private_queues_[worker_index]->empty()) return true;
  }
  {
    v8::base::MutexGuard global_guard(&global_queue_mutex_);
    if (!global_queue_.empty()) return true;
  }
  for (size_t i = 0; i < pool_size_; ++i) {
    if (!deques_[i]->IsEmpty()) return true;
  }
  return false;
}

ThreadTask* ThreadPool::GetTask(size_t worker_index) {
  ThreadTask* task = nullptr;

  while (!terminated_.load(std::memory_order_relaxed)) {
    // 1. Try to pop from private queue (non-stealable tasks first)
    {
      v8::base::MutexGuard private_guard(private_mutexes_[worker_index].get());
      if (!private_queues_[worker_index]->empty()) {
        task = private_queues_[worker_index]->front();
        private_queues_[worker_index]->pop();
        return task;
      }
    }

    // 2. Try to pop from local queue
    if (deques_[worker_index]->Pop(&task)) {
      return task;
    }

    // 3. Try to get from global queue under lock
    {
      v8::base::MutexGuard guard(&global_queue_mutex_);
      if (!global_queue_.empty()) {
        task = global_queue_.front();
        global_queue_.pop();
        return task;
      }
    }

    // 4. Try to steal from other worker threads' deques
    for (size_t i = 0; i < pool_size_; ++i) {
      size_t target_index = (worker_index + 1 + i) % pool_size_;
      if (target_index != worker_index) {
        if (deques_[target_index]->Steal(&task)) {
          return task;
        }
      }
    }

    // 5. No work found, wait on condition variable
    {
      v8::base::MutexGuard guard(&pool_mutex_);
      if (terminated_.load(std::memory_order_relaxed)) {
        break;
      }

      if (!HasWork(worker_index)) {
        sleeping_workers_.fetch_add(1, std::memory_order_seq_cst);
        work_cv_.Wait(&pool_mutex_);
        sleeping_workers_.fetch_sub(1, std::memory_order_seq_cst);
      }
    }
  }

  return nullptr;
}

void ThreadPool::Terminate() {
  if (terminated_.exchange(true)) {
    return;
  }

  {
    v8::base::MutexGuard guard(&pool_mutex_);
    work_cv_.NotifyAll();
  }

  for (auto& thread : threads_) {
    thread->Join();
  }
  threads_.clear();
}

ThreadPool::WorkerThread::WorkerThread(ThreadPool* pool, size_t index)
    : Thread(Options("v8:ThreadWorker")), pool_(pool), worker_index_(index) {}

void ThreadPool::WorkerThread::Run() {
  g_worker_index = static_cast<int>(worker_index_);
  v8::ArrayBuffer::Allocator* allocator = v8::ArrayBuffer::Allocator::NewDefaultAllocator();
  v8::Isolate::CreateParams create_params;
  create_params.array_buffer_allocator = allocator;
  v8::Isolate* isolate = v8::Isolate::New(create_params);

  {
    v8::Locker locker(isolate);
    v8::Isolate::Scope isolate_scope(isolate);

    while (true) {
      ThreadTask* task = pool_->GetTask(worker_index_);
      if (task == nullptr) {
        break;
      }

      ExecuteTask(isolate, task);
      delete task;
    }
  }

  isolate->Dispose();
  delete allocator;
}

void ThreadPool::WorkerThread::ExecuteTask(v8::Isolate* isolate, ThreadTask* task) {
  if (task->IsInternal()) {
    task->RunInternal(isolate);
    return;
  }
  v8::HandleScope handle_scope(isolate);
  v8::Local<v8::Context> context = v8::Context::New(isolate);
  v8::Context::Scope context_scope(context);
  v8::TryCatch try_catch(isolate);

  std::vector<v8::Local<v8::Value>> args;
  const std::vector<uint8_t>& serialized_args = task->serialized_arguments();
  if (!serialized_args.empty()) {
    ThreadingDeserializerDelegate delegate;
    v8::ValueDeserializer deserializer(isolate, serialized_args.data(), serialized_args.size(), &delegate);
    delegate.SetDeserializer(&deserializer);
    bool ok = false;
    if (deserializer.ReadHeader(context).FromMaybe(false)) {
      v8::Local<v8::Value> args_array_val;
      if (deserializer.ReadValue(context).ToLocal(&args_array_val) && args_array_val->IsArray()) {
        v8::Local<v8::Array> args_array = args_array_val.As<v8::Array>();
        uint32_t length = args_array->Length();
        for (uint32_t i = 0; i < length; ++i) {
          v8::Local<v8::Value> arg;
          if (args_array->Get(context, i).ToLocal(&arg)) {
            args.push_back(arg);
          }
        }
        ok = true;
      }
    }
    if (!ok) {
      SerializeAndSetResult(isolate, context, try_catch, task, false);
      return;
    }
  }

  // Compile function source
  std::string code_str = "(" + task->function_source() + ")";
  v8::Local<v8::String> source;
  if (!v8::String::NewFromUtf8(isolate, code_str.c_str(), v8::NewStringType::kNormal).ToLocal(&source)) {
    SerializeAndSetResult(isolate, context, try_catch, task, false);
    return;
  }

  v8::Local<v8::Script> script;
  if (!v8::Script::Compile(context, source).ToLocal(&script)) {
    SerializeAndSetResult(isolate, context, try_catch, task, false);
    return;
  }

  v8::Local<v8::Value> fn_val;
  if (!script->Run(context).ToLocal(&fn_val) || !fn_val->IsFunction()) {
    SerializeAndSetResult(isolate, context, try_catch, task, false);
    return;
  }

  v8::Local<v8::Function> fn = fn_val.As<v8::Function>();
  v8::Local<v8::Value> result;
  if (!fn->Call(context, context->Global(), static_cast<int>(args.size()), args.data()).ToLocal(&result)) {
    SerializeAndSetResult(isolate, context, try_catch, task, false);
  } else {
    if (result->IsPromise()) {
      v8::Local<v8::Promise> promise = result.As<v8::Promise>();
      while (promise->State() == v8::Promise::PromiseState::kPending) {
        isolate->PerformMicrotaskCheckpoint();
        if (promise->State() != v8::Promise::PromiseState::kPending) break;

        ThreadTask* internal_task = nullptr;
        {
          v8::base::MutexGuard private_guard(pool_->private_mutexes_[worker_index_].get());
          if (!pool_->private_queues_[worker_index_]->empty()) {
            internal_task = pool_->private_queues_[worker_index_]->front();
            pool_->private_queues_[worker_index_]->pop();
          }
        }

        if (internal_task) {
          ExecuteTask(isolate, internal_task);
          delete internal_task;
        } else {
          v8::base::OS::Sleep(v8::base::TimeDelta::FromMilliseconds(1));
        }
      }

      if (promise->State() == v8::Promise::PromiseState::kFulfilled) {
        result = promise->Result();
      } else {
        v8::Local<v8::Value> exception = promise->Result();
        ThreadingSerializerDelegate delegate;
        v8::ValueSerializer serializer(isolate, &delegate);
        delegate.SetSerializer(&serializer);
        serializer.WriteHeader();
        if (serializer.WriteValue(context, exception).FromMaybe(false)) {
          std::pair<uint8_t*, size_t> buffer = serializer.Release();
          std::vector<uint8_t> data(buffer.first, buffer.first + buffer.second);
          free(buffer.first);
          task->SetResult(false, std::move(data));
        } else {
          task->SetResult(false, {});
        }
        return;
      }
    }

    v8::TryCatch serialize_try_catch(isolate);
    ThreadingSerializerDelegate delegate;
    v8::ValueSerializer serializer(isolate, &delegate);
    delegate.SetSerializer(&serializer);
    serializer.WriteHeader();
    if (serializer.WriteValue(context, result).FromMaybe(false)) {
      std::pair<uint8_t*, size_t> buffer = serializer.Release();
      std::vector<uint8_t> data(buffer.first, buffer.first + buffer.second);
      free(buffer.first);
      task->SetResult(true, std::move(data));
    } else {
      SerializeAndSetResult(isolate, context, serialize_try_catch, task, false);
    }
  }
}

void ThreadPool::WorkerThread::SerializeAndSetResult(v8::Isolate* isolate,
                                                    v8::Local<v8::Context> context,
                                                    v8::TryCatch& try_catch,
                                                    ThreadTask* task,
                                                    bool success) {
  v8::Local<v8::Value> exception_val;
  if (try_catch.HasCaught()) {
    exception_val = try_catch.Exception();
  } else {
    exception_val = v8::Exception::Error(
        v8::String::NewFromUtf8Literal(isolate, "Unknown execution error"));
  }

  ThreadingSerializerDelegate delegate;
  v8::ValueSerializer serializer(isolate, &delegate);
  delegate.SetSerializer(&serializer);
  serializer.WriteHeader();
  if (serializer.WriteValue(context, exception_val).FromMaybe(false)) {
    std::pair<uint8_t*, size_t> buffer = serializer.Release();
    std::vector<uint8_t> data(buffer.first, buffer.first + buffer.second);
    free(buffer.first);
    task->SetResult(false, std::move(data));
  } else {
    v8::Local<v8::Value> fallback_err = v8::Exception::Error(
        v8::String::NewFromUtf8Literal(isolate, "Failed to serialize task exception"));
    ThreadingSerializerDelegate fallback_delegate;
    v8::ValueSerializer fallback_serializer(isolate, &fallback_delegate);
    fallback_delegate.SetSerializer(&fallback_serializer);
    fallback_serializer.WriteHeader();
    if (fallback_serializer.WriteValue(context, fallback_err).FromMaybe(false)) {
      std::pair<uint8_t*, size_t> buffer = fallback_serializer.Release();
      std::vector<uint8_t> data(buffer.first, buffer.first + buffer.second);
      free(buffer.first);
      task->SetResult(false, std::move(data));
    } else {
      task->SetResult(false, {});
    }
  }
}

}  // namespace threading
}  // namespace internal
}  // namespace v8
