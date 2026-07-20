// Copyright 2026 the V8 project authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifdef V8_ENABLE_MULTITHREADING

#include "src/builtins/builtins-utils-inl.h"
#include "src/builtins/builtins.h"
#include "src/init/v8.h"
#include "src/api/api-inl.h"
#include "src/threading/thread-pool.h"
#include "src/threading/channel.h"
#include "src/threading/mutex.h"
#include "src/threading/serializer.h"
#include "include/v8-value-serializer.h"
#include "include/v8-promise.h"
#include "include/v8-context.h"
#include "include/v8-exception.h"
#include "include/v8-template.h"
#include "include/v8-function.h"
#include <thread>
#include <chrono>

namespace v8 {
namespace internal {

// GC Tracking helpers
template <typename T>
struct WrappedGCData {
  v8::Persistent<v8::Object> persistent;
  std::shared_ptr<T>* shared_ptr_val;
};

template <typename T>
void WrappedWeakCallback(const v8::WeakCallbackInfo<WrappedGCData<T>>& data) {
  WrappedGCData<T>* gc_data = data.GetParameter();
  delete gc_data->shared_ptr_val;
  gc_data->persistent.Reset();
  delete gc_data;
}

template <typename T>
v8::Local<v8::Object> WrapSharedPtr(v8::Isolate* v8_isolate, v8::Local<v8::Context> context, std::shared_ptr<T> shared_val, v8::Local<v8::ObjectTemplate> templ, const char* type_name = nullptr) {
  v8::Local<v8::Object> obj;
  if (!templ->NewInstance(context).ToLocal(&obj)) {
    return v8::Local<v8::Object>();
  }
  auto* heap_ptr = new std::shared_ptr<T>(std::move(shared_val));
  obj->SetAlignedPointerInInternalField(0, heap_ptr, v8::kEmbedderDataTypeTagDefault);

  auto* gc_data = new WrappedGCData<T>();
  gc_data->shared_ptr_val = heap_ptr;
  gc_data->persistent.Reset(v8_isolate, obj);
  gc_data->persistent.SetWeak(gc_data, WrappedWeakCallback<T>, v8::WeakCallbackType::kParameter);

  if (type_name) {
    v8::Local<v8::String> type_key = v8::String::NewFromUtf8(v8_isolate, "__v8_threading::type", v8::NewStringType::kNormal).ToLocalChecked();
    v8::Local<v8::String> type_val = v8::String::NewFromUtf8(v8_isolate, type_name, v8::NewStringType::kNormal).ToLocalChecked();
    obj->CreateDataProperty(context, type_key, type_val).FromJust();
  }

  return obj;
}

// Tasks to resolve outcomes back in callers
class ResolveJoinTask : public threading::ThreadTask {
 public:
  ResolveJoinTask(v8::Isolate* isolate,
                  threading::TaskResult result,
                  v8::Global<v8::Promise::Resolver>* resolver)
      : threading::ThreadTask("", {}),
        isolate_(isolate),
        result_(std::move(result)),
        resolver_(resolver) {}

  ~ResolveJoinTask() override {
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

    if (result_.success) {
      if (result_.data.empty()) {
        res->Resolve(context, v8::Undefined(isolate)).FromJust();
      } else {
        threading::ThreadingDeserializerDelegate delegate;
        v8::ValueDeserializer deserializer(isolate, result_.data.data(), result_.data.size(), &delegate);
        delegate.SetDeserializer(&deserializer);
        if (deserializer.ReadHeader(context).FromMaybe(false)) {
          v8::Local<v8::Value> val;
          if (deserializer.ReadValue(context).ToLocal(&val)) {
            res->Resolve(context, val).FromJust();
          } else {
            res->Reject(context, v8::Exception::Error(v8::String::NewFromUtf8Literal(isolate, "Failed to deserialize joined result"))).FromJust();
          }
        } else {
          res->Reject(context, v8::Exception::Error(v8::String::NewFromUtf8Literal(isolate, "Failed to deserialize joined result header"))).FromJust();
        }
      }
    } else {
      if (result_.data.empty()) {
        res->Reject(context, v8::Exception::Error(v8::String::NewFromUtf8Literal(isolate, "Thread crashed or threw an error"))).FromJust();
      } else {
        threading::ThreadingDeserializerDelegate delegate;
        v8::ValueDeserializer deserializer(isolate, result_.data.data(), result_.data.size(), &delegate);
        delegate.SetDeserializer(&deserializer);
        if (deserializer.ReadHeader(context).FromMaybe(false)) {
          v8::Local<v8::Value> val;
          if (deserializer.ReadValue(context).ToLocal(&val)) {
            res->Reject(context, val).FromJust();
          } else {
            res->Reject(context, v8::Exception::Error(v8::String::NewFromUtf8Literal(isolate, "Thread crashed (failed to deserialize error)"))).FromJust();
          }
        } else {
          res->Reject(context, v8::Exception::Error(v8::String::NewFromUtf8Literal(isolate, "Thread crashed (failed to deserialize error header)"))).FromJust();
        }
      }
    }
    resolver_->Reset();
  }

 private:
  v8::Isolate* isolate_;
  threading::TaskResult result_;
  v8::Global<v8::Promise::Resolver>* resolver_;
};

class ResolveSleepTask : public threading::ThreadTask {
 public:
  ResolveSleepTask(v8::Isolate* isolate,
                   v8::Global<v8::Promise::Resolver>* resolver)
      : threading::ThreadTask("", {}),
        isolate_(isolate),
        resolver_(resolver) {}

  ~ResolveSleepTask() override {
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
    res->Resolve(context, v8::Undefined(isolate)).FromJust();
    resolver_->Reset();
  }

 private:
  v8::Isolate* isolate_;
  v8::Global<v8::Promise::Resolver>* resolver_;
};

namespace threading {

v8::Local<v8::Object> CreateChannelSender(v8::Isolate* isolate, std::shared_ptr<ChannelState> state) {
  v8::Local<v8::Context> context = isolate->GetCurrentContext();
  v8::Local<v8::ObjectTemplate> sender_templ = v8::ObjectTemplate::New(isolate);
  sender_templ->SetInternalFieldCount(1);
  sender_templ->Set(isolate, "send", v8::FunctionTemplate::New(isolate, ChannelSender::Send));
  return WrapSharedPtr<ChannelState>(isolate, context, std::move(state), sender_templ, "channel_sender");
}

v8::Local<v8::Object> CreateChannelReceiver(v8::Isolate* isolate, std::shared_ptr<ChannelState> state) {
  v8::Local<v8::Context> context = isolate->GetCurrentContext();
  v8::Local<v8::ObjectTemplate> receiver_templ = v8::ObjectTemplate::New(isolate);
  receiver_templ->SetInternalFieldCount(1);
  receiver_templ->Set(isolate, "recv", v8::FunctionTemplate::New(isolate, ChannelReceiver::Recv));
  return WrapSharedPtr<ChannelState>(isolate, context, std::move(state), receiver_templ, "channel_receiver");
}

v8::Local<v8::Object> CreateMutex(v8::Isolate* isolate, std::shared_ptr<MutexState> state) {
  v8::Local<v8::Context> context = isolate->GetCurrentContext();
  v8::Local<v8::ObjectTemplate> mutex_templ = v8::ObjectTemplate::New(isolate);
  mutex_templ->SetInternalFieldCount(1);
  mutex_templ->Set(isolate, "lock", v8::FunctionTemplate::New(isolate, MutexWrapper::Lock));
  mutex_templ->Set(isolate, "value", v8::FunctionTemplate::New(isolate, MutexWrapper::Value));
  return WrapSharedPtr<MutexState>(isolate, context, std::move(state), mutex_templ, "mutex");
}

}  // namespace threading

// Builtin implementations
BUILTIN(ThreadSpawn) {
  HandleScope scope(isolate);
  v8::Isolate* v8_isolate = reinterpret_cast<v8::Isolate*>(isolate);
  v8::Local<v8::Context> context = v8_isolate->GetCurrentContext();

  if (args.length() < 2 || !IsJSFunction(*args.at<Object>(1))) {
    v8_isolate->ThrowException(v8::Exception::TypeError(
        v8::String::NewFromUtf8Literal(v8_isolate, "First argument to Thread.spawn must be a function")));
    return ReadOnlyRoots(isolate).exception();
  }

  v8::Local<v8::Function> fn = v8::Utils::ToLocal(args.at<JSFunction>(1)).As<v8::Function>();
  v8::Local<v8::String> fn_source;
  if (!fn->ToString(context).ToLocal(&fn_source)) {
    v8_isolate->ThrowException(v8::Exception::Error(
        v8::String::NewFromUtf8Literal(v8_isolate, "Failed to stringify function")));
    return ReadOnlyRoots(isolate).exception();
  }
  v8::String::Utf8Value fn_source_utf8(v8_isolate, fn_source);
  std::string source_str(*fn_source_utf8, fn_source_utf8.length());

  int num_args = args.length() - 2;
  std::vector<std::shared_ptr<v8::BackingStore>> backing_stores;
  threading::ThreadingSerializerDelegate delegate;
  v8::ValueSerializer serializer(v8_isolate, &delegate);
  delegate.SetSerializer(&serializer);

  if (num_args > 0) {
    v8::Local<v8::Value> last_arg_val = v8::Utils::ToLocal(args.at<Object>(num_args + 1));
    if (last_arg_val->IsObject() && !last_arg_val->IsArrayBuffer()) {
      v8::Local<v8::Object> last_arg = last_arg_val.As<v8::Object>();
      v8::Local<v8::String> transfer_key = v8::String::NewFromUtf8Literal(v8_isolate, "transfer");
      v8::Local<v8::Value> transfer_val;
      if (last_arg->Get(context, transfer_key).ToLocal(&transfer_val) && transfer_val->IsArray()) {
        v8::Local<v8::Array> transfer_list = transfer_val.As<v8::Array>();
        for (uint32_t i = 0; i < transfer_list->Length(); ++i) {
          v8::Local<v8::Value> item;
          if (transfer_list->Get(context, i).ToLocal(&item) && item->IsArrayBuffer()) {
            v8::Local<v8::ArrayBuffer> ab = item.As<v8::ArrayBuffer>();
            serializer.TransferArrayBuffer(i, ab);
            backing_stores.push_back(ab->GetBackingStore());
            ab->Detach(v8::Local<v8::Value>()).Check();
          }
        }
        num_args--; // do not serialize the options object as an argument
      }
    }
  }

  v8::Local<v8::Array> args_array = v8::Array::New(v8_isolate, num_args);
  for (int i = 0; i < num_args; ++i) {
    v8::Local<v8::Value> arg_val = v8::Utils::ToLocal(args.at<Object>(i + 2));
    if (args_array->Set(context, i, arg_val).IsNothing()) {
      v8_isolate->ThrowException(v8::Exception::Error(
          v8::String::NewFromUtf8Literal(v8_isolate, "Failed to set argument in array")));
      return ReadOnlyRoots(isolate).exception();
    }
  }

  serializer.WriteHeader();
  if (!serializer.WriteValue(context, args_array).FromMaybe(false)) {
    v8_isolate->ThrowException(v8::Exception::Error(
        v8::String::NewFromUtf8Literal(v8_isolate, "Failed to serialize arguments")));
    return ReadOnlyRoots(isolate).exception();
  }
  std::pair<uint8_t*, size_t> buffer = serializer.Release();
  std::vector<uint8_t> serialized_bytes(buffer.first, buffer.first + buffer.second);
  free(buffer.first);

  auto* task = new threading::ThreadTask(std::move(source_str), std::move(serialized_bytes), std::move(backing_stores));
  std::shared_ptr<std::future<threading::TaskResult>> future_ptr =
      std::make_shared<std::future<threading::TaskResult>>(task->GetFuture());

  threading::ThreadPool::GetInstance()->Submit(task);

  v8::Local<v8::ObjectTemplate> templ = v8::ObjectTemplate::New(v8_isolate);
  templ->SetInternalFieldCount(1);
  v8::Local<v8::Object> join_handle = WrapSharedPtr<std::future<threading::TaskResult>>(
      v8_isolate, context, std::move(future_ptr), templ);

  if (join_handle.IsEmpty()) {
    return ReadOnlyRoots(isolate).exception();
  }

  return *v8::Utils::OpenHandle(*join_handle);
}

BUILTIN(ThreadJoin) {
  HandleScope scope(isolate);
  v8::Isolate* v8_isolate = reinterpret_cast<v8::Isolate*>(isolate);
  v8::Local<v8::Context> context = v8_isolate->GetCurrentContext();

  if (args.length() < 2 || !IsJSObject(*args.at<Object>(1))) {
    v8_isolate->ThrowException(v8::Exception::TypeError(
        v8::String::NewFromUtf8Literal(v8_isolate, "First argument to Thread.join must be a JoinHandle object")));
    return ReadOnlyRoots(isolate).exception();
  }

  v8::Local<v8::Object> holder = v8::Utils::ToLocal(args.at<JSObject>(1));
  if (holder->InternalFieldCount() < 1) {
    v8_isolate->ThrowException(v8::Exception::TypeError(
        v8::String::NewFromUtf8Literal(v8_isolate, "Invalid JoinHandle object")));
    return ReadOnlyRoots(isolate).exception();
  }

  auto* wrapped = static_cast<std::shared_ptr<std::future<threading::TaskResult>>*>(
      holder->GetAlignedPointerFromInternalField(0, v8::kEmbedderDataTypeTagDefault));
  if (!wrapped || !*wrapped) {
    v8_isolate->ThrowException(v8::Exception::TypeError(
        v8::String::NewFromUtf8Literal(v8_isolate, "JoinHandle is already joined or invalid")));
    return ReadOnlyRoots(isolate).exception();
  }

  std::shared_ptr<std::future<threading::TaskResult>> future_ptr = std::move(*wrapped);
  holder->SetAlignedPointerInInternalField(0, nullptr, v8::kEmbedderDataTypeTagDefault);

  v8::Local<v8::Promise::Resolver> resolver;
  if (!v8::Promise::Resolver::New(context).ToLocal(&resolver)) {
    return ReadOnlyRoots(isolate).exception();
  }

  auto* resolver_global_ptr = new v8::Global<v8::Promise::Resolver>(v8_isolate, resolver);
  int caller_worker_index = threading::g_worker_index;

  std::thread([future_ptr, v8_isolate, caller_worker_index, resolver_global_ptr]() mutable {
    threading::TaskResult result = future_ptr->get();

    if (threading::ThreadPool::IsDisposing()) return;

    auto* resolve_task = new ResolveJoinTask(v8_isolate, std::move(result), resolver_global_ptr);

    if (caller_worker_index != -1) {
      threading::ThreadPool::GetInstance()->SubmitToWorker(caller_worker_index, resolve_task);
    } else {
      class ForegroundResolveJoinTask : public v8::Task {
       public:
        explicit ForegroundResolveJoinTask(ResolveJoinTask* task) : task_(task) {}
        void Run() override {
          task_->RunInternal(task_->receiver_isolate());
          delete task_;
        }
       private:
        ResolveJoinTask* task_;
      };

      auto task_runner = V8::GetCurrentPlatform()->GetForegroundTaskRunner(v8_isolate);
      if (task_runner) {
        task_runner->PostTask(std::make_unique<ForegroundResolveJoinTask>(resolve_task));
      } else {
        delete resolve_task;
      }
    }
  }).detach();

  return *v8::Utils::OpenHandle(*resolver->GetPromise());
}

BUILTIN(ThreadSleep) {
  HandleScope scope(isolate);
  v8::Isolate* v8_isolate = reinterpret_cast<v8::Isolate*>(isolate);
  v8::Local<v8::Context> context = v8_isolate->GetCurrentContext();

  int64_t ms = 0;
  if (args.length() >= 2) {
    v8::Local<v8::Value> ms_val = v8::Utils::ToLocal(args.at<Object>(1));
    if (ms_val->IsNumber()) {
      ms = ms_val->IntegerValue(context).FromMaybe(0);
    }
  }

  v8::Local<v8::Promise::Resolver> resolver;
  if (!v8::Promise::Resolver::New(context).ToLocal(&resolver)) {
    return ReadOnlyRoots(isolate).exception();
  }

  auto* resolver_global_ptr = new v8::Global<v8::Promise::Resolver>(v8_isolate, resolver);
  int caller_worker_index = threading::g_worker_index;

  std::thread([v8_isolate, caller_worker_index, resolver_global_ptr, ms]() mutable {
    std::this_thread::sleep_for(std::chrono::milliseconds(ms));

    if (threading::ThreadPool::IsDisposing()) return;

    auto* task = new ResolveSleepTask(v8_isolate, resolver_global_ptr);

    if (caller_worker_index != -1) {
      threading::ThreadPool::GetInstance()->SubmitToWorker(caller_worker_index, task);
    } else {
      class ForegroundSleepTask : public v8::Task {
       public:
        explicit ForegroundSleepTask(ResolveSleepTask* task) : task_(task) {}
        void Run() override {
          task_->RunInternal(task_->receiver_isolate());
          delete task_;
        }
       private:
        ResolveSleepTask* task_;
      };

      auto task_runner = V8::GetCurrentPlatform()->GetForegroundTaskRunner(v8_isolate);
      if (task_runner) {
        task_runner->PostTask(std::make_unique<ForegroundSleepTask>(task));
      } else {
        delete task;
      }
    }
  }).detach();

  return *v8::Utils::OpenHandle(*resolver->GetPromise());
}

BUILTIN(ThreadChannel) {
  HandleScope scope(isolate);
  v8::Isolate* v8_isolate = reinterpret_cast<v8::Isolate*>(isolate);
  v8::Local<v8::Context> context = v8_isolate->GetCurrentContext();

  auto state = std::make_shared<threading::ChannelState>();

  if (args.length() >= 2) {
    v8::Local<v8::Value> cap_val = v8::Utils::ToLocal(args.at<Object>(1));
    if (cap_val->IsNumber()) {
      state->capacity = cap_val->IntegerValue(context).FromMaybe(0);
    }
  }

  v8::Local<v8::Object> sender_obj = threading::CreateChannelSender(v8_isolate, state);
  v8::Local<v8::Object> receiver_obj = threading::CreateChannelReceiver(v8_isolate, state);

  if (sender_obj.IsEmpty() || receiver_obj.IsEmpty()) {
    return ReadOnlyRoots(isolate).exception();
  }

  v8::Local<v8::Array> channel_pair = v8::Array::New(v8_isolate, 2);
  channel_pair->Set(context, 0, sender_obj).FromJust();
  channel_pair->Set(context, 1, receiver_obj).FromJust();

  return *v8::Utils::OpenHandle(*channel_pair);
}

BUILTIN(ThreadMutex) {
  HandleScope scope(isolate);
  v8::Isolate* v8_isolate = reinterpret_cast<v8::Isolate*>(isolate);
  v8::Local<v8::Context> context = v8_isolate->GetCurrentContext();

  auto state = std::make_shared<threading::MutexState>();

  if (args.length() >= 2) {
    v8::Local<v8::Value> initial_val = v8::Utils::ToLocal(args.at<Object>(1));
    threading::ThreadingSerializerDelegate delegate;
    v8::ValueSerializer serializer(v8_isolate, &delegate);
    delegate.SetSerializer(&serializer);
    serializer.WriteHeader();
    if (serializer.WriteValue(context, initial_val).FromMaybe(false)) {
      std::pair<uint8_t*, size_t> buffer = serializer.Release();
      state->serialized_value.assign(buffer.first, buffer.first + buffer.second);
      free(buffer.first);
    }
  }

  v8::Local<v8::Object> mutex_obj = threading::CreateMutex(v8_isolate, state);

  if (mutex_obj.IsEmpty()) {
    return ReadOnlyRoots(isolate).exception();
  }

  return *v8::Utils::OpenHandle(*mutex_obj);
}
BUILTIN(ThreadGetPoolSize) {
  HandleScope scope(isolate);
  v8::Isolate* v8_isolate = reinterpret_cast<v8::Isolate*>(isolate);
  size_t pool_size = threading::ThreadPool::GetInstance()->pool_size();
  return *v8::Utils::OpenHandle(*v8::Number::New(v8_isolate, static_cast<double>(pool_size)));
}

BUILTIN(ThreadSetPoolSize) {
  HandleScope scope(isolate);
  v8::Isolate* v8_isolate = reinterpret_cast<v8::Isolate*>(isolate);
  v8::Local<v8::Context> context = v8_isolate->GetCurrentContext();

  if (args.length() < 2) {
    v8_isolate->ThrowException(v8::Exception::TypeError(
        v8::String::NewFromUtf8Literal(v8_isolate, "Missing pool size argument")));
    return ReadOnlyRoots(isolate).exception();
  }

  v8::Local<v8::Value> size_val = v8::Utils::ToLocal(args.at<Object>(1));
  if (!size_val->IsNumber()) {
    v8_isolate->ThrowException(v8::Exception::TypeError(
        v8::String::NewFromUtf8Literal(v8_isolate, "Pool size must be a number")));
    return ReadOnlyRoots(isolate).exception();
  }

  int new_size = size_val->Int32Value(context).FromMaybe(0);
  if (new_size <= 0 || new_size > 128) {
    v8_isolate->ThrowException(v8::Exception::RangeError(
        v8::String::NewFromUtf8Literal(v8_isolate, "Pool size must be between 1 and 128")));
    return ReadOnlyRoots(isolate).exception();
  }

  threading::ThreadPool::GetInstance()->Resize(new_size);
  return ReadOnlyRoots(isolate).undefined_value();
}
}  // namespace internal
}  // namespace v8

#endif  // V8_ENABLE_MULTITHREADING
