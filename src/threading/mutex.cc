// Copyright 2026 the V8 project authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "src/threading/mutex.h"
#include "include/v8-value-serializer.h"
#include "src/threading/thread-pool.h"
#include "src/threading/serializer.h"
#include "src/base/logging.h"
#include "src/init/v8.h"

namespace v8 {
namespace internal {
namespace threading {

extern thread_local int g_worker_index;

MutexState::~MutexState() {
  while (!pending_locks.empty()) {
    PendingLock p = pending_locks.front();
    pending_locks.pop();
    if (!ThreadPool::IsDisposing()) {
      delete p.callback;
      delete p.resolver;
    }
  }
}

class ResolveMutexTask : public ThreadTask {
 public:
  ResolveMutexTask(std::shared_ptr<MutexState> state,
                   v8::Isolate* isolate,
                   v8::Global<v8::Function>* callback,
                   v8::Global<v8::Promise::Resolver>* resolver)
      : ThreadTask("", {}),
        state_(std::move(state)),
        isolate_(isolate),
        callback_(callback),
        resolver_(resolver) {}

  ~ResolveMutexTask() override {
    if (!ThreadPool::IsDisposing()) {
      delete callback_;
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

    v8::Local<v8::Function> cb = callback_->Get(isolate);
    v8::Local<v8::Promise::Resolver> res = resolver_->Get(isolate);

    state_->mutex.Lock();
    std::vector<uint8_t> serialized_val = state_->serialized_value;
    state_->mutex.Unlock();

    v8::Local<v8::Value> current_val;
    bool deserialize_ok = false;
    if (serialized_val.empty()) {
      current_val = v8::Undefined(isolate);
      deserialize_ok = true;
    } else {
      ThreadingDeserializerDelegate delegate;
      v8::ValueDeserializer deserializer(isolate, serialized_val.data(), serialized_val.size(), &delegate);
      delegate.SetDeserializer(&deserializer);
      if (deserializer.ReadHeader(context).FromMaybe(false)) {
        v8::Local<v8::Value> temp;
        if (deserializer.ReadValue(context).ToLocal(&temp)) {
          current_val = temp;
          deserialize_ok = true;
        }
      }
    }

    if (!deserialize_ok) {
      res->Reject(context, v8::Exception::Error(v8::String::NewFromUtf8Literal(isolate, "Failed to deserialize mutex value"))).FromJust();
      state_->ReleaseLock();
      return;
    }

    v8::TryCatch try_catch(isolate);
    v8::Local<v8::Value> argv[] = { current_val };
    v8::Local<v8::Value> new_val;
    if (!cb->Call(context, context->Global(), 1, argv).ToLocal(&new_val)) {
      res->Reject(context, try_catch.Exception()).FromJust();
      state_->ReleaseLock();
      return;
    }

    ThreadingSerializerDelegate delegate;
    v8::ValueSerializer serializer(isolate, &delegate);
    delegate.SetSerializer(&serializer);
    serializer.WriteHeader();
    if (serializer.WriteValue(context, new_val).FromMaybe(false)) {
      std::pair<uint8_t*, size_t> buffer = serializer.Release();
      state_->mutex.Lock();
      state_->serialized_value.assign(buffer.first, buffer.first + buffer.second);
      state_->mutex.Unlock();
      free(buffer.first);
      res->Resolve(context, new_val).FromJust();
    } else {
      res->Reject(context, v8::Exception::Error(v8::String::NewFromUtf8Literal(isolate, "Failed to serialize updated mutex value"))).FromJust();
    }

    state_->ReleaseLock();
  }

 private:
  std::shared_ptr<MutexState> state_;
  v8::Isolate* isolate_;
  v8::Global<v8::Function>* callback_;
  v8::Global<v8::Promise::Resolver>* resolver_;
};

void MutexState::ReleaseLock() {
  mutex.Lock();
  if (!pending_locks.empty()) {
    PendingLock pending = std::move(pending_locks.front());
    pending_locks.pop();
    mutex.Unlock();

    auto* task = new ResolveMutexTask(shared_from_this(), pending.isolate, pending.callback, pending.resolver);

    if (pending.worker_index != -1) {
      ThreadPool::GetInstance()->SubmitToWorker(pending.worker_index, task);
    } else {
      class ForegroundMutexTask : public v8::Task {
       public:
        explicit ForegroundMutexTask(ResolveMutexTask* task) : task_(task) {}
        void Run() override {
          task_->RunInternal(task_->receiver_isolate());
          delete task_;
        }
       private:
        ResolveMutexTask* task_;
      };

      auto task_runner = V8::GetCurrentPlatform()->GetForegroundTaskRunner(pending.isolate);
      if (task_runner) {
        task_runner->PostTask(std::make_unique<ForegroundMutexTask>(task));
      } else {
        delete task;
      }
    }
  } else {
    locked = false;
    mutex.Unlock();
  }
}

void MutexWrapper::Lock(const v8::FunctionCallbackInfo<v8::Value>& args) {
  v8::Isolate* isolate = args.GetIsolate();
  v8::HandleScope handle_scope(isolate);
  v8::Local<v8::Context> context = isolate->GetCurrentContext();

  if (args.Length() < 1 || !args[0]->IsFunction()) {
    isolate->ThrowException(v8::Exception::TypeError(v8::String::NewFromUtf8Literal(isolate, "Callback function required")));
    return;
  }
  v8::Local<v8::Function> callback = args[0].As<v8::Function>();

  v8::Local<v8::Object> holder = args.This();
  if (holder->InternalFieldCount() < 1) {
    isolate->ThrowException(v8::Exception::TypeError(v8::String::NewFromUtf8Literal(isolate, "Invalid mutex object")));
    return;
  }
  auto* wrapped = static_cast<std::shared_ptr<MutexState>*>(holder->GetAlignedPointerFromInternalField(0, v8::kEmbedderDataTypeTagDefault));
  if (!wrapped || !*wrapped) {
    isolate->ThrowException(v8::Exception::TypeError(v8::String::NewFromUtf8Literal(isolate, "Mutex is destroyed")));
    return;
  }
  std::shared_ptr<MutexState> state = *wrapped;

  v8::Local<v8::Promise::Resolver> resolver;
  if (!v8::Promise::Resolver::New(context).ToLocal(&resolver)) return;

  state->mutex.Lock();
  if (!state->locked) {
    state->locked = true;
    std::vector<uint8_t> serialized_val = state->serialized_value;
    state->mutex.Unlock();

    v8::Local<v8::Value> current_val;
    bool deserialize_ok = false;
    if (serialized_val.empty()) {
      current_val = v8::Undefined(isolate);
      deserialize_ok = true;
    } else {
      ThreadingDeserializerDelegate delegate;
      v8::ValueDeserializer deserializer(isolate, serialized_val.data(), serialized_val.size(), &delegate);
      delegate.SetDeserializer(&deserializer);
      if (deserializer.ReadHeader(context).FromMaybe(false)) {
        v8::Local<v8::Value> temp;
        if (deserializer.ReadValue(context).ToLocal(&temp)) {
          current_val = temp;
          deserialize_ok = true;
        }
      }
    }

    if (!deserialize_ok) {
      resolver->Reject(context, v8::Exception::Error(v8::String::NewFromUtf8Literal(isolate, "Failed to deserialize mutex value"))).FromJust();
      state->ReleaseLock();
      args.GetReturnValue().Set(resolver->GetPromise());
      return;
    }

    v8::TryCatch try_catch(isolate);
    v8::Local<v8::Value> argv[] = { current_val };
    v8::Local<v8::Value> new_val;
    if (!callback->Call(context, context->Global(), 1, argv).ToLocal(&new_val)) {
      resolver->Reject(context, try_catch.Exception()).FromJust();
      state->ReleaseLock();
      args.GetReturnValue().Set(resolver->GetPromise());
      return;
    }

    ThreadingSerializerDelegate delegate;
    v8::ValueSerializer serializer(isolate, &delegate);
    delegate.SetSerializer(&serializer);
    serializer.WriteHeader();
    if (serializer.WriteValue(context, new_val).FromMaybe(false)) {
      std::pair<uint8_t*, size_t> buffer = serializer.Release();
      state->mutex.Lock();
      state->serialized_value.assign(buffer.first, buffer.first + buffer.second);
      state->mutex.Unlock();
      free(buffer.first);
      resolver->Resolve(context, new_val).FromJust();
    } else {
      resolver->Reject(context, v8::Exception::Error(v8::String::NewFromUtf8Literal(isolate, "Failed to serialize updated mutex value"))).FromJust();
    }

    state->ReleaseLock();
    args.GetReturnValue().Set(resolver->GetPromise());
  } else {
    PendingLock pending;
    pending.isolate = isolate;
    pending.worker_index = g_worker_index;
    pending.callback = new v8::Global<v8::Function>(isolate, callback);
    pending.resolver = new v8::Global<v8::Promise::Resolver>(isolate, resolver);

    state->pending_locks.push(std::move(pending));
    state->mutex.Unlock();

    args.GetReturnValue().Set(resolver->GetPromise());
  }
}

void MutexWrapper::Value(const v8::FunctionCallbackInfo<v8::Value>& args) {
  v8::Isolate* isolate = args.GetIsolate();
  v8::HandleScope handle_scope(isolate);
  v8::Local<v8::Context> context = isolate->GetCurrentContext();

  v8::Local<v8::Object> holder = args.This();
  if (holder->InternalFieldCount() < 1) {
    isolate->ThrowException(v8::Exception::TypeError(v8::String::NewFromUtf8Literal(isolate, "Invalid mutex object")));
    return;
  }
  auto* wrapped = static_cast<std::shared_ptr<MutexState>*>(holder->GetAlignedPointerFromInternalField(0, v8::kEmbedderDataTypeTagDefault));
  if (!wrapped || !*wrapped) {
    isolate->ThrowException(v8::Exception::TypeError(v8::String::NewFromUtf8Literal(isolate, "Mutex is destroyed")));
    return;
  }
  std::shared_ptr<MutexState> state = *wrapped;

  state->mutex.Lock();
  std::vector<uint8_t> serialized_val = state->serialized_value;
  state->mutex.Unlock();

  if (serialized_val.empty()) {
    args.GetReturnValue().Set(v8::Undefined(isolate));
    return;
  }

  ThreadingDeserializerDelegate delegate;
  v8::ValueDeserializer deserializer(isolate, serialized_val.data(), serialized_val.size(), &delegate);
  delegate.SetDeserializer(&deserializer);
  if (deserializer.ReadHeader(context).FromMaybe(false)) {
    v8::Local<v8::Value> val;
    if (deserializer.ReadValue(context).ToLocal(&val)) {
      args.GetReturnValue().Set(val);
      return;
    }
  }

  isolate->ThrowException(v8::Exception::Error(v8::String::NewFromUtf8Literal(isolate, "Failed to deserialize value")));
}

}  // namespace threading
}  // namespace internal
}  // namespace v8
