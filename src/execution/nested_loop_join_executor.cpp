//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), left_executor_(std::move(left_executor)), right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() { 
  // throw NotImplementedException("NestedLoopJoinExecutor is not implemented"); 
  left_executor_->Init();
  right_executor_->Init();

  if_left_join_ = false;
  if_right_join_ = false;
  // is_joined_ = false;
  match_ = false;
  }

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
  Tuple right_tuple{};
  RID right_rid{};
  // Tuple left_tuple{};
  // RID left_rid{};
  while (true) {
    if (!if_left_join_) {
      if (!left_executor_->Next(&left_tuple_, &left_rid_)) {
        return false;
      }
      if_left_join_ = true;
    }
    if (!if_right_join_) {
      right_executor_->Init();
      if_right_join_ = true;
    }
    while (right_executor_->Next(&right_tuple, &right_rid)) {
      auto value = plan_->Predicate()->EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(),
                                                    &right_tuple, right_executor_->GetOutputSchema());
      if (!value.IsNull() && value.GetAs<bool>()) {
        std::vector<Value> values{};
        values.reserve(GetOutputSchema().GetColumnCount());
        for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); ++i) {
            //一个元组对应多个值，需要全部放进去。
            values.emplace_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), i));
          }
        for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); ++i) {
          values.emplace_back(right_tuple.GetValue(&right_executor_->GetOutputSchema(), i));
        }
        *tuple = Tuple(values, &GetOutputSchema());
        match_ = true;
        return true;
      }
    }
      // 不完全匹配的情况，需要添加null值
      if (!match_ && plan_->GetJoinType() == JoinType::LEFT) {
        std::vector<Value> values{};
        values.reserve(GetOutputSchema().GetColumnCount());
        for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); ++i) {
            //一个元组对应多个值，需要全部放进去。
            values.emplace_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), i));
          }
        for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); ++i) {
          values.emplace_back(ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(i).GetType()));
        }
        *tuple = Tuple(values, &GetOutputSchema());
        if_right_join_ = false;
        if_left_join_ = false;
        match_ = false;
        return true;
      }
      // 内连接的情况，不需要添加数据，注意while循环是每个right的next值
      if_right_join_ = false;
      match_ = false;
      if_left_join_ = false;
    }
    return false;
  }


}  // namespace bustub
