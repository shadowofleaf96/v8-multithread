// Copyright 2026 the V8 project authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef V8_THREADING_TASK_H_
#define V8_THREADING_TASK_H_

#include <string>
#include <vector>
#include <future>
#include <memory>
#include "include/v8-array-buffer.h"

namespace v8 {
class Isolate;
namespace internal {
namespace threading {

struct TaskResult {
  bool success = true;
  std::vector<uint8_t> data;
};

class ThreadTask {
 public:
  ThreadTask(std::string function_source, std::vector<uint8_t> serialized_arguments, std::vector<std::shared_ptr<v8::BackingStore>> backing_stores = {})
      : function_source_(std::move(function_source)),
        serialized_arguments_(std::move(serialized_arguments)),
        backing_stores_(std::move(backing_stores)) {}

  virtual ~ThreadTask() = default;

  virtual bool IsInternal() const { return false; }
  virtual void RunInternal(v8::Isolate* isolate) {}

  const std::string& function_source() const { return function_source_; }
  const std::vector<uint8_t>& serialized_arguments() const { return serialized_arguments_; }
  const std::vector<std::shared_ptr<v8::BackingStore>>& backing_stores() const { return backing_stores_; }

  std::future<TaskResult> GetFuture() {
    return promise_.get_future();
  }

  void SetResult(bool success, std::vector<uint8_t> data) {
    TaskResult result;
    result.success = success;
    result.data = std::move(data);
    promise_.set_value(std::move(result));
  }

 private:
  std::string function_source_;
  std::vector<uint8_t> serialized_arguments_;
  std::vector<std::shared_ptr<v8::BackingStore>> backing_stores_;
  std::promise<TaskResult> promise_;
};

}  // namespace threading
}  // namespace internal
}  // namespace v8

#endif  // V8_THREADING_TASK_H_
