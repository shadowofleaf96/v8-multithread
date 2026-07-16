// Copyright 2026 the V8 project authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef V8_THREADING_MUTEX_H_
#define V8_THREADING_MUTEX_H_

#include <queue>
#include <memory>
#include <vector>
#include "include/v8.h"
#include "src/base/platform/mutex.h"
#include "src/threading/task.h"

namespace v8 {
namespace internal {
namespace threading {

struct PendingLock {
  v8::Isolate* isolate;
  int worker_index;
  v8::Global<v8::Function>* callback;
  v8::Global<v8::Promise::Resolver>* resolver;
};

class MutexState : public std::enable_shared_from_this<MutexState> {
 public:
  v8::base::Mutex mutex;
  std::vector<uint8_t> serialized_value;
  bool locked = false;
  std::queue<PendingLock> pending_locks;

  ~MutexState();

  void ReleaseLock();
};

class MutexWrapper {
 public:
  explicit MutexWrapper(std::shared_ptr<MutexState> state) : state_(std::move(state)) {}
  static void Lock(const v8::FunctionCallbackInfo<v8::Value>& args);
  static void Value(const v8::FunctionCallbackInfo<v8::Value>& args);
  std::shared_ptr<MutexState> state() const { return state_; }

 private:
  std::shared_ptr<MutexState> state_;
};

}  // namespace threading
}  // namespace internal
}  // namespace v8

#endif  // V8_THREADING_MUTEX_H_
