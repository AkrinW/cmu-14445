//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "type/value_factory.h"
namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  // throw NotImplementedException("HashJoinExecutor is not implemented");
  left_child_->Init();
  right_child_->Init();

  left_bool_ = left_child_->Next(&left_tuple_, &left_rid_);
  jht_ = std::make_unique<SimpleHashJoinHashTable>();
  // 不能在HashJoinExecutor执行器的next中完成，因为执行器需要先从子执行器中获取所有数据，然后对这些数据进行join，最后才能产生输出结果
  while (right_child_->Next(&right_tuple_, &right_rid_)) {
    jht_->InsertKey(GetRightJoinKey(&right_tuple_), right_tuple_);
  }
  // 获取左tuple的key
  auto left_key = GetLeftJoinKey(&left_tuple_);
  right_tuple_vector_ = jht_->GetValue(left_key);
  if (right_tuple_vector_ == nullptr) {
    // 用于左连接没有匹配到的情况
    if_hashjoined_ = false;
  } else {
    iter_ = right_tuple_vector_->begin();
    if_hashjoined_ = true;
  }
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // 类似nestloopjoin的思路，循环查找左右匹配，如果内连接且不匹配，就不需要输出任何值
  while (true) {
    // 一个左边可能匹配多个右边
    if (right_tuple_vector_ != nullptr && iter_ != right_tuple_vector_->end()) {
      std::vector<Value> values;
      for (uint32_t i = 0; i < left_child_->GetOutputSchema().GetColumnCount(); ++i) {
        values.emplace_back(left_tuple_.GetValue(&left_child_->GetOutputSchema(), i));
      }
      auto right_tuple = *iter_;
      for (uint32_t i = 0; i < right_child_->GetOutputSchema().GetColumnCount(); ++i) {
        values.emplace_back(right_tuple.GetValue(&right_child_->GetOutputSchema(), i));
      }
      *tuple = Tuple{values, &GetOutputSchema()};
      ++iter_;
      return true;
    }
    // 如果right_tuple_为空，或者jht_iterator_遍历完，且为左连接
    // 如果has_done_为false，则说明左连接没有匹配的元组，需要输出右元组为null的情况
    if (plan_->GetJoinType() == JoinType::LEFT && !if_hashjoined_) {
      std::vector<Value> values;
      for (uint32_t i = 0; i < left_child_->GetOutputSchema().GetColumnCount(); ++i) {
        values.emplace_back(left_tuple_.GetValue(&left_child_->GetOutputSchema(), i));
      }
      // 右边元组值均为null
      for (uint32_t i = 0; i < right_child_->GetOutputSchema().GetColumnCount(); ++i) {
        values.emplace_back(ValueFactory::GetNullValueByType(right_child_->GetOutputSchema().GetColumn(i).GetType()));
      }
      *tuple = Tuple{values, &GetOutputSchema()};
      if_hashjoined_ = true;
      return true;
    }
    // 如果不是左连接，或者为左连接，但有有效输出，则继续遍历下一个左元组进行匹配
    // 如果left_bool_为false，左边找完了
    if_hashjoined_ = left_child_->Next(&left_tuple_, &left_rid_);
    if (!if_hashjoined_) {
      return false;
    }
    // 重置右边的元组，更新迭代器
    auto left_key = GetLeftJoinKey(&left_tuple_);
    right_tuple_vector_ = jht_->GetValue(left_key);
    if (right_tuple_vector_ != nullptr) {
      iter_ = right_tuple_vector_->begin();
      if_hashjoined_ = true;
    } else {
      if_hashjoined_ = false;
    }
  }
  return false;
}

auto HashJoinExecutor::GetLeftJoinKey(const Tuple *tuple) -> bustub::HashJoinKey {
  HashJoinKey key{};
  for (const auto &expr : plan_->LeftJoinKeyExpressions()) {
    key.AddKey(expr->Evaluate(tuple, left_child_->GetOutputSchema()));
  }
  return key;
}

auto HashJoinExecutor::GetRightJoinKey(const Tuple *tuple) -> bustub::HashJoinKey {
  HashJoinKey key{};
  for (const auto &expr : plan_->RightJoinKeyExpressions()) {
    key.AddKey(expr->Evaluate(tuple, right_child_->GetOutputSchema()));
  }
  return key;
}

}  // namespace bustub