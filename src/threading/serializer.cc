// Copyright 2026 the V8 project authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "src/threading/serializer.h"

namespace v8 {
namespace internal {
namespace threading {

v8::Maybe<bool> ThreadingSerializerDelegate::IsHostObject(v8::Isolate* isolate, v8::Local<v8::Object> object) {
  v8::Local<v8::Context> context = isolate->GetCurrentContext();
  v8::Local<v8::String> type_key = v8::String::NewFromUtf8(isolate, "__v8_threading::type", v8::NewStringType::kNormal).ToLocalChecked();
  v8::Local<v8::Value> type_val;
  if (object->HasOwnProperty(context, type_key).FromMaybe(false) &&
      object->Get(context, type_key).ToLocal(&type_val) &&
      type_val->IsString()) {
    return v8::Just(true);
  }
  return v8::Just(false);
}

v8::Maybe<bool> ThreadingSerializerDelegate::WriteHostObject(v8::Isolate* isolate, v8::Local<v8::Object> object) {
  if (!serializer_) {
    return v8::Nothing<bool>();
  }

  v8::Local<v8::Context> context = isolate->GetCurrentContext();
  v8::Local<v8::String> type_key = v8::String::NewFromUtf8(isolate, "__v8_threading::type", v8::NewStringType::kNormal).ToLocalChecked();
  v8::Local<v8::Value> type_val;
  if (!object->Get(context, type_key).ToLocal(&type_val) || !type_val->IsString()) {
    return v8::Nothing<bool>();
  }

  v8::String::Utf8Value type_str(isolate, type_val);
  std::string type(*type_str, type_str.length());

  if (type == "channel_sender") {
    auto* wrapped = static_cast<std::shared_ptr<ChannelState>*>(object->GetAlignedPointerFromInternalField(0, v8::kEmbedderDataTypeTagDefault));
    if (wrapped && *wrapped) {
      serializer_->WriteUint32(0x01);
      auto* heap_ptr = new std::shared_ptr<ChannelState>(*wrapped);
      serializer_->WriteUint64(reinterpret_cast<uint64_t>(heap_ptr));
      return v8::Just(true);
    }
  } else if (type == "channel_receiver") {
    auto* wrapped = static_cast<std::shared_ptr<ChannelState>*>(object->GetAlignedPointerFromInternalField(0, v8::kEmbedderDataTypeTagDefault));
    if (wrapped && *wrapped) {
      serializer_->WriteUint32(0x02);
      auto* heap_ptr = new std::shared_ptr<ChannelState>(*wrapped);
      serializer_->WriteUint64(reinterpret_cast<uint64_t>(heap_ptr));
      return v8::Just(true);
    }
  } else if (type == "mutex") {
    auto* wrapped = static_cast<std::shared_ptr<MutexState>*>(object->GetAlignedPointerFromInternalField(0, v8::kEmbedderDataTypeTagDefault));
    if (wrapped && *wrapped) {
      serializer_->WriteUint32(0x03);
      auto* heap_ptr = new std::shared_ptr<MutexState>(*wrapped);
      serializer_->WriteUint64(reinterpret_cast<uint64_t>(heap_ptr));
      return v8::Just(true);
    }
  }

  return v8::Nothing<bool>();
}

v8::MaybeLocal<v8::Object> ThreadingDeserializerDelegate::ReadHostObject(v8::Isolate* isolate) {
  if (!deserializer_) {
    return v8::MaybeLocal<v8::Object>();
  }

  uint32_t tag = 0;
  uint64_t address = 0;
  if (!deserializer_->ReadUint32(&tag) || !deserializer_->ReadUint64(&address)) {
    return v8::MaybeLocal<v8::Object>();
  }

  if (tag == 0x01) {
    auto* ptr = reinterpret_cast<std::shared_ptr<ChannelState>*>(address);
    std::shared_ptr<ChannelState> state = *ptr;
    delete ptr;
    return CreateChannelSender(isolate, std::move(state));
  } else if (tag == 0x02) {
    auto* ptr = reinterpret_cast<std::shared_ptr<ChannelState>*>(address);
    std::shared_ptr<ChannelState> state = *ptr;
    delete ptr;
    return CreateChannelReceiver(isolate, std::move(state));
  } else if (tag == 0x03) {
    auto* ptr = reinterpret_cast<std::shared_ptr<MutexState>*>(address);
    std::shared_ptr<MutexState> state = *ptr;
    delete ptr;
    return CreateMutex(isolate, std::move(state));
  }

  return v8::MaybeLocal<v8::Object>();
}

}  // namespace threading
}  // namespace internal
}  // namespace v8
