// Copyright 2026 the V8 project authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "src/threading/channel.h"
#include "include/v8-value-serializer.h"
#include "src/threading/thread-pool.h"
#include "src/threading/serializer.h"
#include "src/base/logging.h"
#include "src/init/v8.h"

namespace v8 {
namespace internal {
namespace threading {

// Thread-local variable to track worker thread index in the pool.
extern thread_local int g_worker_index;

ChannelState::~ChannelState() {
  while (!pending_receivers.empty()) {
    PendingReceiver p = pending_receivers.front();
    pending_receivers.pop();
    if (!ThreadPool::IsDisposing()) {
      delete p.resolver;
    }
  }
  while (!pending_senders.empty()) {
    PendingSender p = std::move(pending_senders.front());
    pending_senders.pop();
    if (!ThreadPool::IsDisposing()) {
      delete p.resolver;
    }
  }
}

class ResolveSenderTask : public ThreadTask {
 public:
  ResolveSenderTask(v8::Isolate* sender_isolate,
                    v8::Global<v8::Promise::Resolver>* resolver)
      : ThreadTask("", {}),
        sender_isolate_(sender_isolate),
        resolver_(resolver) {}

  ~ResolveSenderTask() override {
    if (!ThreadPool::IsDisposing()) {
      delete resolver_;
    }
  }

  bool IsInternal() const override { return true; }

  v8::Isolate* receiver_isolate() const { return sender_isolate_; }

  void RunInternal(v8::Isolate* isolate) override {
    DCHECK_EQ(isolate, sender_isolate_);
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
  v8::Isolate* sender_isolate_;
  v8::Global<v8::Promise::Resolver>* resolver_;
};

class ResolveChannelTask : public ThreadTask {
 public:
  ResolveChannelTask(v8::Isolate* receiver_isolate,
                     ChannelMessage data,
                     v8::Global<v8::Promise::Resolver>* resolver)
      : ThreadTask("", {}),
        receiver_isolate_(receiver_isolate),
        data_(std::move(data)),
        resolver_(resolver) {}

  ~ResolveChannelTask() override {
    if (!ThreadPool::IsDisposing()) {
      delete resolver_;
    }
  }

  bool IsInternal() const override { return true; }

  v8::Isolate* receiver_isolate() const { return receiver_isolate_; }

  void RunInternal(v8::Isolate* isolate) override {
    DCHECK_EQ(isolate, receiver_isolate_);
    v8::HandleScope handle_scope(isolate);
    v8::Local<v8::Context> context = isolate->GetCurrentContext();
    if (context.IsEmpty()) {
      context = v8::Context::New(isolate);
    }
    v8::Context::Scope context_scope(context);

    v8::Local<v8::Promise::Resolver> res = resolver_->Get(isolate);
    ThreadingDeserializerDelegate delegate;
    v8::ValueDeserializer deserializer(isolate, data_.data.data(), data_.data.size(), &delegate);
    delegate.SetDeserializer(&deserializer);

    for (uint32_t i = 0; i < data_.backing_stores.size(); ++i) {
      v8::Local<v8::ArrayBuffer> ab = v8::ArrayBuffer::New(isolate, data_.backing_stores[i]);
      deserializer.TransferArrayBuffer(i, ab);
    }
    if (deserializer.ReadHeader(context).FromMaybe(false)) {
      v8::Local<v8::Value> val;
      if (deserializer.ReadValue(context).ToLocal(&val)) {
        res->Resolve(context, val).FromJust();
      } else {
        res->Reject(context, v8::Exception::Error(v8::String::NewFromUtf8Literal(isolate, "Failed to deserialize message"))).FromJust();
      }
    } else {
      res->Reject(context, v8::Exception::Error(v8::String::NewFromUtf8Literal(isolate, "Failed to deserialize message header"))).FromJust();
    }
    resolver_->Reset();
  }

 private:
  v8::Isolate* receiver_isolate_;
  ChannelMessage data_;
  v8::Global<v8::Promise::Resolver>* resolver_;
};

void ChannelSender::Send(const v8::FunctionCallbackInfo<v8::Value>& args) {
  v8::Isolate* isolate = args.GetIsolate();
  v8::HandleScope handle_scope(isolate);
  v8::Local<v8::Context> context = isolate->GetCurrentContext();

  if (args.Length() < 1) {
    isolate->ThrowException(v8::Exception::TypeError(v8::String::NewFromUtf8Literal(isolate, "Message required")));
    return;
  }

  // Retrieve ChannelSender pointer
  v8::Local<v8::Object> holder = args.This();
  if (holder->InternalFieldCount() < 1) {
    isolate->ThrowException(v8::Exception::TypeError(v8::String::NewFromUtf8Literal(isolate, "Invalid receiver object")));
    return;
  }
  auto* wrapped = static_cast<std::shared_ptr<ChannelState>*>(holder->GetAlignedPointerFromInternalField(0, v8::kEmbedderDataTypeTagDefault));
  if (!wrapped || !*wrapped) {
    isolate->ThrowException(v8::Exception::TypeError(v8::String::NewFromUtf8Literal(isolate, "Channel is closed")));
    return;
  }
  std::shared_ptr<ChannelState> state = *wrapped;

  ThreadingSerializerDelegate delegate;
  v8::ValueSerializer serializer(isolate, &delegate);
  delegate.SetSerializer(&serializer);
  std::vector<std::shared_ptr<v8::BackingStore>> backing_stores;
  if (args.Length() > 1 && args[1]->IsArray()) {
    v8::Local<v8::Array> transfer_list = args[1].As<v8::Array>();
    for (uint32_t i = 0; i < transfer_list->Length(); ++i) {
      v8::Local<v8::Value> item;
      if (transfer_list->Get(context, i).ToLocal(&item) && item->IsArrayBuffer()) {
        v8::Local<v8::ArrayBuffer> ab = item.As<v8::ArrayBuffer>();
        serializer.TransferArrayBuffer(i, ab);
        backing_stores.push_back(ab->GetBackingStore());
        ab->Detach(v8::Local<v8::Value>()).Check();
      }
    }
  }

  serializer.WriteHeader();
  if (!serializer.WriteValue(context, args[0]).FromMaybe(false)) {
    isolate->ThrowException(v8::Exception::Error(v8::String::NewFromUtf8Literal(isolate, "Failed to serialize message")));
    return;
  }

  std::pair<uint8_t*, size_t> buffer = serializer.Release();
  ChannelMessage msg;
  msg.data.assign(buffer.first, buffer.first + buffer.second);
  msg.backing_stores = std::move(backing_stores);
  free(buffer.first);

  v8::Local<v8::Promise::Resolver> resolver;
  if (!v8::Promise::Resolver::New(context).ToLocal(&resolver)) return;

  state->mutex.Lock();
  if (!state->pending_receivers.empty()) {
    PendingReceiver pending = std::move(state->pending_receivers.front());
    state->pending_receivers.pop();
    state->mutex.Unlock();

    auto* resolve_task = new ResolveChannelTask(pending.isolate, std::move(msg), new v8::Global<v8::Promise::Resolver>(pending.isolate, pending.resolver->Pass()));

    if (pending.worker_index != -1) {
      ThreadPool::GetInstance()->SubmitToWorker(pending.worker_index, resolve_task);
    } else {
      class ForegroundResolveTask : public v8::Task {
       public:
        explicit ForegroundResolveTask(ResolveChannelTask* task) : task_(task) {}
        void Run() override {
          task_->RunInternal(task_->receiver_isolate());
          delete task_;
        }
       private:
        ResolveChannelTask* task_;
      };

      auto task_runner = V8::GetCurrentPlatform()->GetForegroundTaskRunner(pending.isolate);
      if (task_runner) {
        task_runner->PostTask(std::make_unique<ForegroundResolveTask>(resolve_task));
      } else {
        delete resolve_task;
      }
    }

    resolver->Resolve(context, v8::Undefined(isolate)).FromJust();
    args.GetReturnValue().Set(resolver->GetPromise());
  } else if (state->capacity > 0 && state->messages.size() >= state->capacity) {
    PendingSender pending;
    pending.msg = std::move(msg);
    pending.isolate = isolate;
    pending.worker_index = g_worker_index;
    pending.resolver = new v8::Global<v8::Promise::Resolver>(isolate, resolver);
    state->pending_senders.push(std::move(pending));
    state->mutex.Unlock();

    args.GetReturnValue().Set(resolver->GetPromise());
  } else {
    state->messages.push(std::move(msg));
    state->mutex.Unlock();
    resolver->Resolve(context, v8::Undefined(isolate)).FromJust();
    args.GetReturnValue().Set(resolver->GetPromise());
  }
}

void ChannelReceiver::Recv(const v8::FunctionCallbackInfo<v8::Value>& args) {
  v8::Isolate* isolate = args.GetIsolate();
  v8::HandleScope handle_scope(isolate);
  v8::Local<v8::Context> context = isolate->GetCurrentContext();

  v8::Local<v8::Promise::Resolver> resolver;
  if (!v8::Promise::Resolver::New(context).ToLocal(&resolver)) return;

  v8::Local<v8::Object> holder = args.This();
  if (holder->InternalFieldCount() < 1) {
    isolate->ThrowException(v8::Exception::TypeError(v8::String::NewFromUtf8Literal(isolate, "Invalid receiver object")));
    return;
  }
  auto* wrapped = static_cast<std::shared_ptr<ChannelState>*>(holder->GetAlignedPointerFromInternalField(0, v8::kEmbedderDataTypeTagDefault));
  if (!wrapped || !*wrapped) {
    isolate->ThrowException(v8::Exception::TypeError(v8::String::NewFromUtf8Literal(isolate, "Channel is closed")));
    return;
  }
  std::shared_ptr<ChannelState> state = *wrapped;

  state->mutex.Lock();
  if (!state->messages.empty()) {
    ChannelMessage msg = std::move(state->messages.front());
    state->messages.pop();

    PendingSender pending_sender;
    bool has_pending_sender = false;
    if (!state->pending_senders.empty()) {
      pending_sender = std::move(state->pending_senders.front());
      state->pending_senders.pop();
      state->messages.push(std::move(pending_sender.msg));
      has_pending_sender = true;
    }
    state->mutex.Unlock();

    if (has_pending_sender) {
      auto* resolve_sender_task = new ResolveSenderTask(pending_sender.isolate, new v8::Global<v8::Promise::Resolver>(pending_sender.isolate, pending_sender.resolver->Pass()));
      if (pending_sender.worker_index != -1) {
        ThreadPool::GetInstance()->SubmitToWorker(pending_sender.worker_index, resolve_sender_task);
      } else {
        class ForegroundResolveSenderTask : public v8::Task {
         public:
          explicit ForegroundResolveSenderTask(ResolveSenderTask* task) : task_(task) {}
          void Run() override {
            task_->RunInternal(task_->receiver_isolate());
            delete task_;
          }
         private:
          ResolveSenderTask* task_;
        };

        auto task_runner = V8::GetCurrentPlatform()->GetForegroundTaskRunner(pending_sender.isolate);
        if (task_runner) {
          task_runner->PostTask(std::make_unique<ForegroundResolveSenderTask>(resolve_sender_task));
        } else {
          delete resolve_sender_task;
        }
      }
    }

    ThreadingDeserializerDelegate delegate;
    v8::ValueDeserializer deserializer(isolate, msg.data.data(), msg.data.size(), &delegate);
    delegate.SetDeserializer(&deserializer);

    for (uint32_t i = 0; i < msg.backing_stores.size(); ++i) {
      v8::Local<v8::ArrayBuffer> ab = v8::ArrayBuffer::New(isolate, msg.backing_stores[i]);
      deserializer.TransferArrayBuffer(i, ab);
    }
    if (deserializer.ReadHeader(context).FromMaybe(false)) {
      v8::Local<v8::Value> val;
      if (deserializer.ReadValue(context).ToLocal(&val)) {
        resolver->Resolve(context, val).FromJust();
      } else {
        resolver->Reject(context, v8::Exception::Error(v8::String::NewFromUtf8Literal(isolate, "Failed to deserialize message"))).FromJust();
      }
    } else {
      resolver->Reject(context, v8::Exception::Error(v8::String::NewFromUtf8Literal(isolate, "Failed to deserialize message header"))).FromJust();
    }
    args.GetReturnValue().Set(resolver->GetPromise());
    return;
  }

  PendingReceiver pending;
  pending.isolate = isolate;
  pending.worker_index = g_worker_index;
  pending.resolver = new v8::Global<v8::Promise::Resolver>(isolate, resolver);

  state->pending_receivers.push(std::move(pending));
  state->mutex.Unlock();

  args.GetReturnValue().Set(resolver->GetPromise());
}

}  // namespace threading
}  // namespace internal
}  // namespace v8
