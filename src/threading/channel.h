// Copyright 2026 the V8 project authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef V8_THREADING_CHANNEL_H_
#define V8_THREADING_CHANNEL_H_

#include <queue>
#include <memory>
#include <vector>
#include "include/v8.h"
#include "src/base/platform/mutex.h"
#include "src/threading/task.h"

namespace v8 {
namespace internal {
namespace threading {

struct PendingReceiver {
  v8::Isolate* isolate;
  int worker_index;
  v8::Global<v8::Promise::Resolver>* resolver;
};

struct ChannelState {
  v8::base::Mutex mutex;
  std::queue<std::vector<uint8_t>> messages;
  std::queue<PendingReceiver> pending_receivers;

  ~ChannelState();
};

class ChannelSender {
 public:
  explicit ChannelSender(std::shared_ptr<ChannelState> state) : state_(std::move(state)) {}
  static void Send(const v8::FunctionCallbackInfo<v8::Value>& args);
  std::shared_ptr<ChannelState> state() const { return state_; }

 private:
  std::shared_ptr<ChannelState> state_;
};

class ChannelReceiver {
 public:
  explicit ChannelReceiver(std::shared_ptr<ChannelState> state) : state_(std::move(state)) {}
  static void Recv(const v8::FunctionCallbackInfo<v8::Value>& args);
  std::shared_ptr<ChannelState> state() const { return state_; }

 private:
  std::shared_ptr<ChannelState> state_;
};

}  // namespace threading
}  // namespace internal
}  // namespace v8

#endif  // V8_THREADING_CHANNEL_H_
