//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void LimitExecutor::Init() {
  // throw NotImplementedException("LimitExecutor is not implemented");
  child_executor_->Init();
  Tuple tuple{};
  RID rid{};
  auto limit = plan_->GetLimit();
  size_t count = 0;
  while (child_executor_->Next(&tuple, &rid)) {
    tuples_.emplace_back(tuple);
    ++count;
    if (count == limit) {
      break;
    }
  }
  if (!tuples_.empty()) {
    iter_ = tuples_.begin();
  }
}

auto LimitExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_ != tuples_.end()) {
    *tuple = *iter_;
    ++iter_;
    return true;
  }

  return false;
}
}  // namespace bustub
