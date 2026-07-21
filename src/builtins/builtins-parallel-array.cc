// Copyright 2026 the V8 project authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifdef V8_ENABLE_MULTITHREADING

#include "src/builtins/builtins-utils-inl.h"
#include "src/builtins/builtins.h"
#include "src/init/v8.h"
#include "src/api/api-inl.h"
#include "src/threading/thread-pool.h"
#include "src/threading/serializer.h"
#include "include/v8-value-serializer.h"
#include "include/v8-promise.h"
#include "include/v8-context.h"
#include "include/v8-exception.h"
#include "include/v8-template.h"
#include "include/v8-function.h"
#include <thread>
#include <vector>
#include <memory>
#include <future>

namespace v8 {
namespace internal {

class ResolveParallelArrayTask : public threading::ThreadTask {
 public:
  ResolveParallelArrayTask(v8::Isolate* isolate,
                           std::vector<threading::TaskResult> chunk_results,
                           v8::Global<v8::Promise::Resolver>* resolver)
      : threading::ThreadTask("", {}),
        isolate_(isolate),
        chunk_results_(std::move(chunk_results)),
        resolver_(resolver) {}

  ~ResolveParallelArrayTask() override {
    if (!threading::ThreadPool::IsDisposing()) {
      delete resolver_;
    }
  }

  bool IsInternal() const override { return true; }

  v8::Isolate* receiver_isolate() const { return isolate_; }

  void RunInternal(v8::Isolate* isolate) override {
    DCHECK_EQ(isolate, isolate_);
    v8::HandleScope handle_scope(isolate);
    v8::Local<v8::Context> context = isolate->GetCurrentContext();
    if (context.IsEmpty()) {
      context = v8::Context::New(isolate);
    }
    v8::Context::Scope context_scope(context);

    v8::Local<v8::Promise::Resolver> res = resolver_->Get(isolate);

    // First check if any chunk failed
    for (const auto& chunk_res : chunk_results_) {
      if (!chunk_res.success) {
        if (chunk_res.data.empty()) {
          res->Reject(context, v8::Exception::Error(v8::String::NewFromUtf8Literal(isolate, "Parallel processing chunk failed"))).FromJust();
        } else {
          threading::ThreadingDeserializerDelegate delegate;
          v8::ValueDeserializer deserializer(isolate, chunk_res.data.data(), chunk_res.data.size(), &delegate);
          delegate.SetDeserializer(&deserializer);
          if (deserializer.ReadHeader(context).FromMaybe(false)) {
            v8::Local<v8::Value> val;
            if (deserializer.ReadValue(context).ToLocal(&val)) {
              res->Reject(context, val).FromJust();
            } else {
              res->Reject(context, v8::Exception::Error(v8::String::NewFromUtf8Literal(isolate, "Parallel processing chunk failed (deserialization error)"))).FromJust();
            }
          } else {
            res->Reject(context, v8::Exception::Error(v8::String::NewFromUtf8Literal(isolate, "Parallel processing chunk failed (header deserialization error)"))).FromJust();
          }
        }
        resolver_->Reset();
        return;
      }
    }

    // Now deserialize all chunks and merge them
    std::vector<v8::Local<v8::Value>> merged_elements;
    for (const auto& chunk_res : chunk_results_) {
      threading::ThreadingDeserializerDelegate delegate;
      v8::ValueDeserializer deserializer(isolate, chunk_res.data.data(), chunk_res.data.size(), &delegate);
      delegate.SetDeserializer(&deserializer);
      if (!deserializer.ReadHeader(context).FromMaybe(false)) {
        res->Reject(context, v8::Exception::Error(v8::String::NewFromUtf8Literal(isolate, "Failed to read chunk header"))).FromJust();
        resolver_->Reset();
        return;
      }
      v8::Local<v8::Value> chunk_val;
      if (!deserializer.ReadValue(context).ToLocal(&chunk_val) || !chunk_val->IsArray()) {
        res->Reject(context, v8::Exception::Error(v8::String::NewFromUtf8Literal(isolate, "Failed to deserialize chunk array"))).FromJust();
        resolver_->Reset();
        return;
      }
      v8::Local<v8::Array> chunk_arr = chunk_val.As<v8::Array>();
      uint32_t len = chunk_arr->Length();
      for (uint32_t i = 0; i < len; ++i) {
        v8::Local<v8::Value> elem;
        if (chunk_arr->Get(context, i).ToLocal(&elem)) {
          merged_elements.push_back(elem);
        }
      }
    }

    // Create the final output array
    v8::Local<v8::Array> result_arr = v8::Array::New(isolate, static_cast<int>(merged_elements.size()));
    for (size_t i = 0; i < merged_elements.size(); ++i) {
      result_arr->Set(context, static_cast<uint32_t>(i), merged_elements[i]).FromJust();
    }

    res->Resolve(context, result_arr).FromJust();
    resolver_->Reset();
  }

 private:
  v8::Isolate* isolate_;
  std::vector<threading::TaskResult> chunk_results_;
  v8::Global<v8::Promise::Resolver>* resolver_;
};

static Tagged<Object> ArrayParallelCommon(BuiltinArguments args, Isolate* isolate, bool is_filter) {
  HandleScope scope(isolate);
  v8::Isolate* v8_isolate = reinterpret_cast<v8::Isolate*>(isolate);
  v8::Local<v8::Context> context = v8_isolate->GetCurrentContext();

  v8::Local<v8::Value> receiver_val = v8::Utils::ToLocal(args.receiver());
  if (!receiver_val->IsArray()) {
    v8_isolate->ThrowException(v8::Exception::TypeError(
        v8::String::NewFromUtf8Literal(v8_isolate, "Parallel array method called on non-array")));
    return ReadOnlyRoots(isolate).exception();
  }

  if (args.length() < 2 || !IsJSFunction(*args.at<Object>(1))) {
    v8_isolate->ThrowException(v8::Exception::TypeError(
        v8::String::NewFromUtf8Literal(v8_isolate, "Callback must be a function")));
    return ReadOnlyRoots(isolate).exception();
  }

  v8::Local<v8::Array> receiver_arr = receiver_val.As<v8::Array>();
  uint32_t length = receiver_arr->Length();

  v8::Local<v8::Promise::Resolver> resolver;
  if (!v8::Promise::Resolver::New(context).ToLocal(&resolver)) {
    return ReadOnlyRoots(isolate).exception();
  }

  if (length == 0) {
    resolver->Resolve(context, v8::Array::New(v8_isolate, 0)).FromJust();
    return *v8::Utils::OpenHandle(*resolver->GetPromise());
  }

  v8::Local<v8::Function> callback = v8::Utils::ToLocal(args.at<JSFunction>(1)).As<v8::Function>();
  v8::Local<v8::String> callback_src;
  if (!callback->ToString(context).ToLocal(&callback_src)) {
    v8_isolate->ThrowException(v8::Exception::Error(
        v8::String::NewFromUtf8Literal(v8_isolate, "Failed to stringify callback")));
    return ReadOnlyRoots(isolate).exception();
  }
  v8::String::Utf8Value callback_src_utf8(v8_isolate, callback_src);
  std::string callback_str(*callback_src_utf8, callback_src_utf8.length());

  size_t pool_size = threading::ThreadPool::GetInstance()->pool_size();
  uint32_t num_chunks = std::min(length, static_cast<uint32_t>(pool_size));
  if (num_chunks == 0) num_chunks = 1;

  std::vector<std::shared_ptr<std::future<threading::TaskResult>>> futures;
  std::string action = is_filter ? "filter" : "map";
  std::string func_source = "function(chunk) { const cb = " + callback_str + "; return chunk." + action + "(cb); }";

  for (uint32_t i = 0; i < num_chunks; ++i) {
    uint32_t start = i * (length / num_chunks);
    uint32_t end = (i == num_chunks - 1) ? length : (i + 1) * (length / num_chunks);
    uint32_t chunk_len = end - start;

    v8::Local<v8::Array> chunk = v8::Array::New(v8_isolate, chunk_len);
    for (uint32_t j = 0; j < chunk_len; ++j) {
      v8::Local<v8::Value> elem;
      if (receiver_arr->Get(context, start + j).ToLocal(&elem)) {
        chunk->Set(context, j, elem).FromJust();
      }
    }

    v8::Local<v8::Array> args_arr = v8::Array::New(v8_isolate, 1);
    args_arr->Set(context, 0, chunk).FromJust();

    threading::ThreadingSerializerDelegate delegate;
    v8::ValueSerializer serializer(v8_isolate, &delegate);
    delegate.SetSerializer(&serializer);
    serializer.WriteHeader();
    if (!serializer.WriteValue(context, args_arr).FromMaybe(false)) {
      v8_isolate->ThrowException(v8::Exception::Error(
          v8::String::NewFromUtf8Literal(v8_isolate, "Failed to serialize chunk")));
      return ReadOnlyRoots(isolate).exception();
    }
    std::pair<uint8_t*, size_t> buffer = serializer.Release();
    std::vector<uint8_t> serialized_bytes(buffer.first, buffer.first + buffer.second);
    free(buffer.first);

    auto* task = new threading::ThreadTask(func_source, std::move(serialized_bytes));
    futures.push_back(std::make_shared<std::future<threading::TaskResult>>(task->GetFuture()));
    threading::ThreadPool::GetInstance()->Submit(task);
  }

  auto* resolver_global_ptr = new v8::Global<v8::Promise::Resolver>(v8_isolate, resolver);
  int caller_worker_index = threading::g_worker_index;

  std::thread([futures = std::move(futures), v8_isolate, caller_worker_index, resolver_global_ptr]() mutable {
    std::vector<threading::TaskResult> results;
    for (auto& fut : futures) {
      results.push_back(fut->get());
    }

    if (threading::ThreadPool::IsDisposing()) return;

    auto* resolve_task = new ResolveParallelArrayTask(v8_isolate, std::move(results), resolver_global_ptr);

    if (caller_worker_index != -1) {
      threading::ThreadPool::GetInstance()->SubmitToWorker(caller_worker_index, resolve_task);
    } else {
      class ForegroundResolveParallelArrayTask : public v8::Task {
       public:
        explicit ForegroundResolveParallelArrayTask(ResolveParallelArrayTask* task) : task_(task) {}
        void Run() override {
          task_->RunInternal(task_->receiver_isolate());
          delete task_;
        }
       private:
        ResolveParallelArrayTask* task_;
      };

      auto task_runner = V8::GetCurrentPlatform()->GetForegroundTaskRunner(v8_isolate);
      if (task_runner) {
        task_runner->PostTask(std::make_unique<ForegroundResolveParallelArrayTask>(resolve_task));
      } else {
        delete resolve_task;
      }
    }
  }).detach();

  return *v8::Utils::OpenHandle(*resolver->GetPromise());
}

BUILTIN(ArrayParallelMap) {
  return ArrayParallelCommon(args, isolate, false);
}

BUILTIN(ArrayParallelFilter) {
  return ArrayParallelCommon(args, isolate, true);
}

}  // namespace internal
}  // namespace v8

#endif  // V8_ENABLE_MULTITHREADING
