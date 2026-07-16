// Copyright 2018 the V8 project authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "src/execution/microtask-queue.h"

#include <algorithm>
#include <cstddef>
#include <optional>

#include "src/api/api-inl.h"
#include "src/base/logging.h"
#include "src/execution/isolate.h"
#include "src/handles/handles-inl.h"
#include "src/objects/microtask-inl.h"
#include "src/objects/visitors.h"
#include "src/roots/roots-inl.h"
#include "src/tracing/trace-event.h"

#ifdef V8_ENABLE_MULTITHREADING
#include "src/objects/promise-inl.h"
#include "src/objects/js-promise-inl.h"
#include "src/objects/js-function-inl.h"
#include "src/objects/contexts-inl.h"
#include "src/threading/thread-pool.h"
#include "src/threading/task.h"
#include "src/threading/serializer.h"
#include "include/v8-value-serializer.h"
#include "include/v8-promise.h"
#include "include/v8-context.h"
#include "include/v8-exception.h"
#include "include/v8-function.h"
#include "src/init/v8.h"

namespace v8 {
namespace internal {
namespace threading {
extern thread_local int g_worker_index;
}
}
}
#endif

namespace v8 {
namespace internal {

const size_t MicrotaskQueue::kRingBufferOffset =
    OFFSET_OF(MicrotaskQueue, ring_buffer_);
const size_t MicrotaskQueue::kCapacityOffset =
    OFFSET_OF(MicrotaskQueue, capacity_);
const size_t MicrotaskQueue::kSizeOffset = OFFSET_OF(MicrotaskQueue, size_);
const size_t MicrotaskQueue::kStartOffset = OFFSET_OF(MicrotaskQueue, start_);
const size_t MicrotaskQueue::kFinishedMicrotaskCountOffset =
    OFFSET_OF(MicrotaskQueue, finished_microtask_count_);

#ifdef V8_ENABLE_MULTITHREADING
namespace {

class ResolveAutoParallelPromiseTask : public threading::ThreadTask {
 public:
  ResolveAutoParallelPromiseTask(v8::Isolate* isolate,
                                 threading::TaskResult result,
                                 v8::Global<v8::Value> promise_or_capability)
      : threading::ThreadTask("", {}),
        isolate_(isolate),
        result_(std::move(result)),
        promise_or_capability_(std::move(promise_or_capability)) {}

  bool IsInternal() const override { return true; }

  v8::Isolate* receiver_isolate() const { return isolate_; }

  void RunInternal(v8::Isolate* isolate) override {
    DCHECK_EQ(isolate, isolate_);
    v8::HandleScope handle_scope(isolate);
    v8::Local<v8::Context> context = isolate->GetCurrentContext();
    if (context.IsEmpty()) {
      Isolate* internal_isolate = reinterpret_cast<Isolate*>(isolate);
      context = v8::Utils::ToLocal(direct_handle(internal_isolate->native_context(), internal_isolate));
    }
    v8::Context::Scope context_scope(context);

    v8::Local<v8::Value> pc_val = promise_or_capability_.Get(isolate);
    if (pc_val.IsEmpty() || pc_val->IsUndefined()) {
      promise_or_capability_.Reset();
      return;
    }

    Isolate* internal_isolate = reinterpret_cast<Isolate*>(isolate);
    HandleScope internal_scope(internal_isolate);

    Handle<Object> pc_handle = v8::Utils::OpenHandle(*pc_val);

    // Deserialize result
    Handle<Object> res_handle;
    if (result_.data.empty()) {
      res_handle = internal_isolate->factory()->undefined_value();
    } else {
      threading::ThreadingDeserializerDelegate delegate;
      v8::ValueDeserializer deserializer(isolate, result_.data.data(), result_.data.size(), &delegate);
      delegate.SetDeserializer(&deserializer);
      if (deserializer.ReadHeader(context).FromMaybe(false)) {
        v8::Local<v8::Value> local_res;
        if (deserializer.ReadValue(context).ToLocal(&local_res)) {
          res_handle = v8::Utils::OpenHandle(*local_res);
        } else {
          result_.success = false;
          res_handle = v8::Utils::OpenHandle(*v8::Exception::Error(v8::String::NewFromUtf8Literal(isolate, "Failed to deserialize automatic parallel task result")));
        }
      } else {
        result_.success = false;
        res_handle = v8::Utils::OpenHandle(*v8::Exception::Error(v8::String::NewFromUtf8Literal(isolate, "Failed to deserialize automatic parallel task result header")));
      }
    }

    if (IsJSPromise(*pc_handle)) {
      Handle<JSPromise> promise = Cast<JSPromise>(pc_handle);
      if (result_.success) {
        Handle<Object> unused_resolved;
        bool resolved = JSPromise::Resolve(promise, res_handle).ToHandle(&unused_resolved);
        USE(resolved);
      } else {
        JSPromise::Reject(promise, res_handle);
      }
    } else if (IsPromiseCapability(*pc_handle)) {
      Handle<PromiseCapability> capability = Cast<PromiseCapability>(pc_handle);
      Handle<Object> resolve_or_reject_fn = result_.success
          ? Handle<Object>(capability->resolve(), internal_isolate)
          : Handle<Object>(capability->reject(), internal_isolate);
      if (!IsUndefined(*resolve_or_reject_fn, internal_isolate)) {
        DirectHandle<Object> argv[] = {res_handle};
        Handle<Object> unused_result;
        bool called = Execution::Call(internal_isolate, resolve_or_reject_fn, internal_isolate->factory()->undefined_value(), base::VectorOf(argv))
            .ToHandle(&unused_result);
        USE(called);
      }
    }

    promise_or_capability_.Reset();
  }

 private:
  v8::Isolate* isolate_;
  threading::TaskResult result_;
  v8::Global<v8::Value> promise_or_capability_;
};

class AutoParallelThreadTask : public threading::ThreadTask {
 public:
  AutoParallelThreadTask(v8::Isolate* originator_isolate,
                         std::string function_source,
                         std::vector<uint8_t> serialized_arguments,
                         int originator_worker_index,
                         v8::Global<v8::Value> promise_or_capability)
      : threading::ThreadTask(std::move(function_source), std::move(serialized_arguments)),
        originator_isolate_(originator_isolate),
        originator_worker_index_(originator_worker_index),
        promise_or_capability_(std::move(promise_or_capability)) {}

  bool IsInternal() const override { return true; }

  void RunInternal(v8::Isolate* isolate) override {
    v8::HandleScope handle_scope(isolate);
    v8::Local<v8::Context> context = v8::Context::New(isolate);
    v8::Context::Scope context_scope(context);
    v8::TryCatch try_catch(isolate);

    // Deserialize argument
    std::vector<v8::Local<v8::Value>> args;
    const std::vector<uint8_t>& serialized_args = serialized_arguments();
    if (!serialized_args.empty()) {
      threading::ThreadingDeserializerDelegate delegate;
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
        PostResultBack(isolate, false, {});
        return;
      }
    }

    // Compile function source
    std::string code_str = "(" + function_source() + ")";
    v8::Local<v8::String> source;
    if (!v8::String::NewFromUtf8(isolate, code_str.c_str(), v8::NewStringType::kNormal).ToLocal(&source)) {
      PostResultBack(isolate, false, {});
      return;
    }

    v8::Local<v8::Script> script;
    if (!v8::Script::Compile(context, source).ToLocal(&script)) {
      PostResultBack(isolate, false, {});
      return;
    }

    v8::Local<v8::Value> fn_val;
    if (!script->Run(context).ToLocal(&fn_val) || !fn_val->IsFunction()) {
      PostResultBack(isolate, false, {});
      return;
    }

    v8::Local<v8::Function> fn = fn_val.As<v8::Function>();
    v8::Local<v8::Value> result;
    if (!fn->Call(context, context->Global(), static_cast<int>(args.size()), args.data()).ToLocal(&result)) {
      SerializeAndPostException(isolate, context, try_catch);
    } else {
      if (result->IsPromise()) {
        v8::Local<v8::Promise> promise = result.As<v8::Promise>();
        while (promise->State() == v8::Promise::PromiseState::kPending) {
          isolate->PerformMicrotaskCheckpoint();
          if (promise->State() != v8::Promise::PromiseState::kPending) break;

          threading::ThreadTask* internal_task = nullptr;
          {
            int worker_index = threading::g_worker_index;
            if (worker_index != -1) {
              internal_task = threading::ThreadPool::GetInstance()->PopPrivateTask(worker_index);
            }
          }

          if (internal_task) {
            internal_task->RunInternal(isolate);
            delete internal_task;
          } else {
            v8::base::OS::Sleep(v8::base::TimeDelta::FromMilliseconds(1));
          }
        }

        if (promise->State() == v8::Promise::PromiseState::kFulfilled) {
          result = promise->Result();
        } else {
          v8::Local<v8::Value> exception = promise->Result();
          threading::ThreadingSerializerDelegate delegate;
          v8::ValueSerializer serializer(isolate, &delegate);
          delegate.SetSerializer(&serializer);
          serializer.WriteHeader();
          if (serializer.WriteValue(context, exception).FromMaybe(false)) {
            std::pair<uint8_t*, size_t> buffer = serializer.Release();
            std::vector<uint8_t> data(buffer.first, buffer.first + buffer.second);
            free(buffer.first);
            PostResultBack(isolate, false, std::move(data));
          } else {
            PostResultBack(isolate, false, {});
          }
          return;
        }
      }

      v8::TryCatch serialize_try_catch(isolate);
      threading::ThreadingSerializerDelegate delegate;
      v8::ValueSerializer serializer(isolate, &delegate);
      delegate.SetSerializer(&serializer);
      serializer.WriteHeader();
      if (serializer.WriteValue(context, result).FromMaybe(false)) {
        std::pair<uint8_t*, size_t> buffer = serializer.Release();
        std::vector<uint8_t> data(buffer.first, buffer.first + buffer.second);
        free(buffer.first);
        PostResultBack(isolate, true, std::move(data));
      } else {
        SerializeAndPostException(isolate, context, serialize_try_catch);
      }
    }
  }

 private:
  void PostResultBack(v8::Isolate* isolate, bool success, std::vector<uint8_t> data) {
    threading::TaskResult task_res;
    task_res.success = success;
    task_res.data = std::move(data);

    auto* resolve_task = new ResolveAutoParallelPromiseTask(
        originator_isolate_, std::move(task_res), std::move(promise_or_capability_));

    if (originator_worker_index_ != -1) {
      threading::ThreadPool::GetInstance()->SubmitToWorker(originator_worker_index_, resolve_task);
    } else {
      class ForegroundResolveAutoParallelTask : public v8::Task {
       public:
        explicit ForegroundResolveAutoParallelTask(ResolveAutoParallelPromiseTask* task) : task_(task) {}
        void Run() override {
          task_->RunInternal(task_->receiver_isolate());
          delete task_;
        }
       private:
        ResolveAutoParallelPromiseTask* task_;
      };

      auto task_runner = V8::GetCurrentPlatform()->GetForegroundTaskRunner(originator_isolate_);
      if (task_runner) {
        task_runner->PostTask(std::make_unique<ForegroundResolveAutoParallelTask>(resolve_task));
      } else {
        delete resolve_task;
      }
    }
  }

  void SerializeAndPostException(v8::Isolate* isolate,
                                 v8::Local<v8::Context> context,
                                 v8::TryCatch& try_catch) {
    v8::Local<v8::Value> exception_val;
    if (try_catch.HasCaught()) {
      exception_val = try_catch.Exception();
    } else {
      exception_val = v8::Exception::Error(
          v8::String::NewFromUtf8Literal(isolate, "Unknown execution error"));
    }

    threading::ThreadingSerializerDelegate delegate;
    v8::ValueSerializer serializer(isolate, &delegate);
    delegate.SetSerializer(&serializer);
    serializer.WriteHeader();
    if (serializer.WriteValue(context, exception_val).FromMaybe(false)) {
      std::pair<uint8_t*, size_t> buffer = serializer.Release();
      std::vector<uint8_t> data(buffer.first, buffer.first + buffer.second);
      free(buffer.first);
      PostResultBack(isolate, false, std::move(data));
    } else {
      v8::Local<v8::Value> fallback_err = v8::Exception::Error(
          v8::String::NewFromUtf8Literal(isolate, "Failed to serialize task exception"));
      threading::ThreadingSerializerDelegate fallback_delegate;
      v8::ValueSerializer fallback_serializer(isolate, &fallback_delegate);
      fallback_delegate.SetSerializer(&fallback_serializer);
      fallback_serializer.WriteHeader();
      if (fallback_serializer.WriteValue(context, fallback_err).FromMaybe(false)) {
        std::pair<uint8_t*, size_t> buffer = fallback_serializer.Release();
        std::vector<uint8_t> data(buffer.first, buffer.first + buffer.second);
        free(buffer.first);
        PostResultBack(isolate, false, std::move(data));
      } else {
        PostResultBack(isolate, false, {});
      }
    }
  }

  v8::Isolate* originator_isolate_;
  int originator_worker_index_;
  v8::Global<v8::Value> promise_or_capability_;
};

bool TryInterceptAutoParallelMicrotask(Tagged<Microtask> microtask) {
  if (!IsPromiseReactionJobTask(microtask)) return false;

  auto reaction_job = Cast<PromiseReactionJobTask>(microtask);

  // Must have a handler
  Tagged<Object> handler_obj = reaction_job->handler();
  if (!IsJSFunction(handler_obj)) return false;

  Tagged<JSFunction> handler = Cast<JSFunction>(handler_obj);

  // Verify the handler context chain has no closures (only ScriptContext or NativeContext)
  Tagged<Context> ctx = handler->context();
  while (true) {
    if (IsNativeContext(ctx)) {
      break;
    }
    if (ctx->IsScriptContext()) {
      ctx = ctx->previous();
      continue;
    }
    return false;
  }

  // Get current isolate
  Isolate* isolate = Isolate::TryGetCurrent();
  if (!isolate) return false;

  // We should also make sure the originator promise_or_capability is present and valid
  Handle<Object> pc_handle(reaction_job->promise_or_capability(), isolate);
  if (!IsJSPromise(*pc_handle) && !IsPromiseCapability(*pc_handle)) {
    return false;
  }

  v8::Isolate* v8_isolate = reinterpret_cast<v8::Isolate*>(isolate);
  v8::HandleScope scope(v8_isolate);
  v8::Local<v8::Context> v8_context = v8_isolate->GetCurrentContext();
  if (v8_context.IsEmpty()) {
    v8_context = v8::Utils::ToLocal(direct_handle(isolate->native_context(), isolate));
  }

  // Stringify function source
  Handle<JSFunction> handler_handle(handler, isolate);
  v8::Local<v8::Function> fn = v8::Utils::ToLocal(handler_handle).As<v8::Function>();
  v8::Local<v8::String> fn_source;
  if (!fn->ToString(v8_context).ToLocal(&fn_source)) {
    return false;
  }
  v8::String::Utf8Value fn_source_utf8(v8_isolate, fn_source);
  std::string source_str(*fn_source_utf8, fn_source_utf8.length());

  // Wrap the single argument in an array so it can be serialized and passed to fn.Call on worker
  int num_args = 1;
  v8::Local<v8::Array> args_array = v8::Array::New(v8_isolate, num_args);
  v8::Local<v8::Value> arg_val = v8::Utils::ToLocal(Handle<Object>(reaction_job->argument(), isolate));
  if (args_array->Set(v8_context, 0, arg_val).IsNothing()) {
    return false;
  }

  threading::ThreadingSerializerDelegate delegate;
  v8::ValueSerializer serializer(v8_isolate, &delegate);
  delegate.SetSerializer(&serializer);
  serializer.WriteHeader();
  if (!serializer.WriteValue(v8_context, args_array).FromMaybe(false)) {
    return false;
  }
  std::pair<uint8_t*, size_t> buffer = serializer.Release();
  std::vector<uint8_t> serialized_bytes(buffer.first, buffer.first + buffer.second);
  free(buffer.first);

  // Keep a persistent global handle to the promise_or_capability so the worker thread can resolve it
  v8::Global<v8::Value> promise_or_capability_global(v8_isolate, v8::Utils::ToLocal(pc_handle));

  int caller_worker_index = threading::g_worker_index;

  auto* task = new AutoParallelThreadTask(
      v8_isolate,
      std::move(source_str),
      std::move(serialized_bytes),
      caller_worker_index,
      std::move(promise_or_capability_global));

  threading::ThreadPool::GetInstance()->Submit(task);
  return true;
}

} // namespace
#endif

const intptr_t MicrotaskQueue::kMinimumCapacity = 8;

// static
void MicrotaskQueue::SetUpDefaultMicrotaskQueue(Isolate* isolate) {
  DCHECK_NULL(isolate->default_microtask_queue());

  MicrotaskQueue* microtask_queue = new MicrotaskQueue;
  microtask_queue->next_ = microtask_queue;
  microtask_queue->prev_ = microtask_queue;
  isolate->set_default_microtask_queue(microtask_queue);
}

// static
std::unique_ptr<MicrotaskQueue> MicrotaskQueue::New(Isolate* isolate) {
  DCHECK_NOT_NULL(isolate->default_microtask_queue());

  std::unique_ptr<MicrotaskQueue> microtask_queue(new MicrotaskQueue);

  // Insert the new instance to the next of last MicrotaskQueue instance.
  MicrotaskQueue* last = isolate->default_microtask_queue()->prev_;
  microtask_queue->next_ = last->next_;
  microtask_queue->prev_ = last;
  last->next_->prev_ = microtask_queue.get();
  last->next_ = microtask_queue.get();

  return microtask_queue;
}

MicrotaskQueue::MicrotaskQueue() = default;

MicrotaskQueue::~MicrotaskQueue() {
  if (next_ != this) {
    DCHECK_NE(prev_, this);
    next_->prev_ = prev_;
    prev_->next_ = next_;
  }
  delete[] ring_buffer_;
}

// static
Address MicrotaskQueue::CallEnqueueMicrotask(Isolate* isolate,
                                             intptr_t microtask_queue_pointer,
                                             Address raw_microtask) {
  Tagged<Microtask> microtask = Cast<Microtask>(Tagged<Object>(raw_microtask));
  reinterpret_cast<MicrotaskQueue*>(microtask_queue_pointer)
      ->EnqueueMicrotask(microtask);
  return Smi::zero().ptr();
}

void MicrotaskQueue::EnqueueMicrotask(v8::Isolate* v8_isolate,
                                      v8::Local<Function> function) {
  Isolate* isolate = reinterpret_cast<Isolate*>(v8_isolate);
  HandleScope scope(isolate);
  DirectHandle<CallableTask> microtask = isolate->factory()->NewCallableTask(
      Utils::OpenDirectHandle(*function), isolate->native_context());
  EnqueueMicrotask(*microtask);
}

void MicrotaskQueue::EnqueueMicrotask(v8::Isolate* v8_isolate,
                                      v8::MicrotaskCallback callback,
                                      void* data) {
  Isolate* isolate = reinterpret_cast<Isolate*>(v8_isolate);
  HandleScope scope(isolate);
  DirectHandle<CallbackTask> microtask = isolate->factory()->NewCallbackTask(
      isolate->factory()->NewForeign<kMicrotaskCallbackTag>(
          reinterpret_cast<Address>(callback)),
      isolate->factory()->NewForeign<kMicrotaskCallbackDataTag>(
          reinterpret_cast<Address>(data)));
  EnqueueMicrotask(*microtask);
}

void MicrotaskQueue::EnqueueMicrotask(Tagged<Microtask> microtask) {
#ifdef V8_ENABLE_MULTITHREADING
  if (TryInterceptAutoParallelMicrotask(microtask)) {
    return;
  }
#endif

  if (size_ == capacity_) {
    // Keep the capacity of |ring_buffer_| power of 2, so that the JIT
    // implementation can calculate the modulo easily.
    intptr_t new_capacity = std::max(kMinimumCapacity, capacity_ << 1);
    ResizeBuffer(new_capacity);
  }

  DCHECK_LT(size_, capacity_);
  ring_buffer_[(start_ + size_) % capacity_] = microtask.ptr();
  ++size_;
}

void MicrotaskQueue::PerformCheckpointInternal(v8::Isolate* v8_isolate) {
  DCHECK(ShouldPerfomCheckpoint());
  std::optional<MicrotasksScope> microtasks_scope;
  if (microtasks_policy_ == v8::MicrotasksPolicy::kScoped) {
    // If we're using microtask scopes to schedule microtask execution, V8
    // API calls will check that there's always a microtask scope on the
    // stack. As the microtasks we're about to execute could invoke embedder
    // callbacks which then calls back into V8, we create an artificial
    // microtask scope here to avoid running into the CallDepthScope check.
    microtasks_scope.emplace(v8_isolate, this,
                             v8::MicrotasksScope::kDoNotRunMicrotasks);
  }
  Isolate* isolate = reinterpret_cast<Isolate*>(v8_isolate);
  RunMicrotasks(isolate);
  isolate->ClearKeptObjects();
}

namespace {

class SetIsRunningMicrotasks {
 public:
  explicit SetIsRunningMicrotasks(bool* flag) : flag_(flag) {
    DCHECK(!*flag_);
    *flag_ = true;
  }

  ~SetIsRunningMicrotasks() {
    DCHECK(*flag_);
    *flag_ = false;
  }

 private:
  bool* flag_;
};

}  // namespace

int MicrotaskQueue::RunMicrotasks(Isolate* isolate) {
  SetIsRunningMicrotasks scope(&is_running_microtasks_);
  v8::Isolate::SuppressMicrotaskExecutionScope suppress(
      reinterpret_cast<v8::Isolate*>(isolate), this);

  if (!size()) {
    OnCompleted(isolate);
    return 0;
  }

  // We should not enter V8 if it's marked for termination.
  DCHECK_IMPLIES(v8_flags.strict_termination_checks,
                 !isolate->is_execution_terminating());

  intptr_t base_count = finished_microtask_count_;
  HandleScope handle_scope(isolate);
  MaybeDirectHandle<Object> maybe_result;

#ifdef V8_ENABLE_CONTINUATION_PRESERVED_EMBEDDER_DATA
  DirectHandle<Object> continuation_preserved_embedder_data(
      isolate->isolate_data()->continuation_preserved_embedder_data(), isolate);
  isolate->isolate_data()->set_continuation_preserved_embedder_data(
      ReadOnlyRoots(isolate).undefined_value());
#endif  // V8_ENABLE_CONTINUATION_PRESERVED_EMBEDDER_DATA

  int processed_microtask_count;
  {
    HandleScopeImplementer::EnteredContextRewindScope rewind_scope(
        isolate->handle_scope_implementer());
    TRACE_EVENT_BEGIN("v8.execute", "RunMicrotasks");
    {
      TRACE_EVENT_CALL_STATS_SCOPED(isolate, "v8", "V8.RunMicrotasks");
      maybe_result = Execution::TryRunMicrotasks(isolate, this);
      processed_microtask_count =
          static_cast<int>(finished_microtask_count_ - base_count);
    }
    TRACE_EVENT_END("v8.execute", "microtask_count", processed_microtask_count);
  }

#ifdef V8_ENABLE_CONTINUATION_PRESERVED_EMBEDDER_DATA
  isolate->isolate_data()->set_continuation_preserved_embedder_data(
      *continuation_preserved_embedder_data);
#endif  // V8_ENABLE_CONTINUATION_PRESERVED_EMBEDDER_DATA

  if (isolate->is_execution_terminating()) {
    DCHECK(isolate->has_exception());
    DCHECK(maybe_result.is_null());
    delete[] ring_buffer_;
    ring_buffer_ = nullptr;
    capacity_ = 0;
    size_ = 0;
    start_ = 0;
    isolate->OnTerminationDuringRunMicrotasks();
    OnCompleted(isolate);
    return -1;
  }

  DCHECK_EQ(0, size());
  OnCompleted(isolate);

  return processed_microtask_count;
}

void MicrotaskQueue::IterateMicrotasks(RootVisitor* visitor) {
  if (size_) {
    // Iterate pending Microtasks as root objects to avoid the write barrier for
    // all single Microtask. If this hurts the GC performance, use a FixedArray.
    visitor->VisitRootPointers(
        Root::kMicroTasks, nullptr, FullObjectSlot(ring_buffer_ + start_),
        FullObjectSlot(ring_buffer_ + std::min(start_ + size_, capacity_)));
    visitor->VisitRootPointers(
        Root::kMicroTasks, nullptr, FullObjectSlot(ring_buffer_),
        FullObjectSlot(ring_buffer_ + std::max(start_ + size_ - capacity_,
                                               static_cast<intptr_t>(0))));
  }

  if (capacity_ <= kMinimumCapacity) {
    return;
  }

  intptr_t new_capacity = capacity_;
  while (new_capacity > 2 * size_) {
    new_capacity >>= 1;
  }
  new_capacity = std::max(new_capacity, kMinimumCapacity);
  if (new_capacity < capacity_) {
    ResizeBuffer(new_capacity);
  }
}

void MicrotaskQueue::AddMicrotasksCompletedCallback(
    MicrotasksCompletedCallbackWithData callback, void* data) {
  std::vector<CallbackWithData>* microtasks_completed_callbacks =
      &microtasks_completed_callbacks_;
  if (is_running_completed_callbacks_) {
    if (!microtasks_completed_callbacks_cow_.has_value()) {
      microtasks_completed_callbacks_cow_.emplace(
          microtasks_completed_callbacks_);
    }
    // Use the COW vector if we are iterating the callbacks right now.
    microtasks_completed_callbacks =
        &microtasks_completed_callbacks_cow_.value();
  }

  CallbackWithData callback_with_data(callback, data);
  const auto pos =
      std::find(microtasks_completed_callbacks->begin(),
                microtasks_completed_callbacks->end(), callback_with_data);
  if (pos != microtasks_completed_callbacks->end()) {
    return;
  }
  microtasks_completed_callbacks->push_back(callback_with_data);
}

void MicrotaskQueue::RemoveMicrotasksCompletedCallback(
    MicrotasksCompletedCallbackWithData callback, void* data) {
  std::vector<CallbackWithData>* microtasks_completed_callbacks =
      &microtasks_completed_callbacks_;
  if (is_running_completed_callbacks_) {
    if (!microtasks_completed_callbacks_cow_.has_value()) {
      microtasks_completed_callbacks_cow_.emplace(
          microtasks_completed_callbacks_);
    }
    // Use the COW vector if we are iterating the callbacks right now.
    microtasks_completed_callbacks =
        &microtasks_completed_callbacks_cow_.value();
  }

  CallbackWithData callback_with_data(callback, data);
  const auto pos =
      std::find(microtasks_completed_callbacks->begin(),
                microtasks_completed_callbacks->end(), callback_with_data);
  if (pos == microtasks_completed_callbacks->end()) {
    return;
  }
  microtasks_completed_callbacks->erase(pos);
}

void MicrotaskQueue::OnCompleted(Isolate* isolate) {
  is_running_completed_callbacks_ = true;
  for (auto& callback : microtasks_completed_callbacks_) {
    callback.first(reinterpret_cast<v8::Isolate*>(isolate), callback.second);
  }
  is_running_completed_callbacks_ = false;
  if (V8_UNLIKELY(microtasks_completed_callbacks_cow_.has_value())) {
    microtasks_completed_callbacks_ =
        std::move(microtasks_completed_callbacks_cow_.value());
    microtasks_completed_callbacks_cow_.reset();
  }
}

Tagged<Microtask> MicrotaskQueue::get(intptr_t index) const {
  DCHECK_LT(index, size_);
  Tagged<Object> microtask(ring_buffer_[(index + start_) % capacity_]);
  return Cast<Microtask>(microtask);
}

void MicrotaskQueue::ResizeBuffer(intptr_t new_capacity) {
  DCHECK_LE(size_, new_capacity);
  Address* new_ring_buffer = new Address[new_capacity];
  for (intptr_t i = 0; i < size_; ++i) {
    new_ring_buffer[i] = ring_buffer_[(start_ + i) % capacity_];
  }

  delete[] ring_buffer_;
  ring_buffer_ = new_ring_buffer;
  capacity_ = new_capacity;
  start_ = 0;
}

}  // namespace internal
}  // namespace v8
