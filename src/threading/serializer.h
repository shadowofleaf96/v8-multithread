// Copyright 2026 the V8 project authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef V8_THREADING_SERIALIZER_H_
#define V8_THREADING_SERIALIZER_H_

#include <memory>
#include "include/v8-value-serializer.h"
#include "include/v8-context.h"
#include "include/v8-exception.h"
#include "include/v8-local-handle.h"
#include "include/v8-object.h"
#include "include/v8-primitive.h"
#include "src/threading/channel.h"
#include "src/threading/mutex.h"

namespace v8 {
namespace internal {
namespace threading {

class ThreadingSerializerDelegate : public v8::ValueSerializer::Delegate {
 public:
  ThreadingSerializerDelegate(std::vector<std::shared_ptr<v8::BackingStore>>* shared_array_buffers = nullptr)
      : shared_array_buffers_(shared_array_buffers) {}
  ~ThreadingSerializerDelegate() override = default;

  void SetSerializer(v8::ValueSerializer* serializer) { serializer_ = serializer; }

  void ThrowDataCloneError(v8::Local<v8::String> message) override {}

  bool HasCustomHostObject(v8::Isolate* isolate) override { return true; }
  v8::Maybe<bool> IsHostObject(v8::Isolate* isolate, v8::Local<v8::Object> object) override;
  v8::Maybe<bool> WriteHostObject(v8::Isolate* isolate, v8::Local<v8::Object> object) override;

  v8::Maybe<uint32_t> GetSharedArrayBufferId(v8::Isolate* isolate, v8::Local<v8::SharedArrayBuffer> shared_array_buffer) override;

 private:
  v8::ValueSerializer* serializer_ = nullptr;
  std::vector<std::shared_ptr<v8::BackingStore>>* shared_array_buffers_ = nullptr;
};

class ThreadingDeserializerDelegate : public v8::ValueDeserializer::Delegate {
 public:
  ThreadingDeserializerDelegate(const std::vector<std::shared_ptr<v8::BackingStore>>* shared_array_buffers = nullptr)
      : shared_array_buffers_(shared_array_buffers) {}
  ~ThreadingDeserializerDelegate() override = default;

  void SetDeserializer(v8::ValueDeserializer* deserializer) { deserializer_ = deserializer; }

  v8::MaybeLocal<v8::Object> ReadHostObject(v8::Isolate* isolate) override;

  v8::MaybeLocal<v8::SharedArrayBuffer> GetSharedArrayBufferFromId(v8::Isolate* isolate, uint32_t clone_id) override;

 private:
  v8::ValueDeserializer* deserializer_ = nullptr;
  const std::vector<std::shared_ptr<v8::BackingStore>>* shared_array_buffers_ = nullptr;
};

// Helper functions to wrap state objects back into JS objects.
// These are implemented in builtins-thread.cc.
v8::Local<v8::Object> CreateChannelSender(v8::Isolate* isolate, std::shared_ptr<ChannelState> state);
v8::Local<v8::Object> CreateChannelReceiver(v8::Isolate* isolate, std::shared_ptr<ChannelState> state);
v8::Local<v8::Object> CreateMutex(v8::Isolate* isolate, std::shared_ptr<MutexState> state);

}  // namespace threading
}  // namespace internal
}  // namespace v8

#endif  // V8_THREADING_SERIALIZER_H_
