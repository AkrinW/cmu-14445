//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      aht_(plan_->GetAggregates(), plan_->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  child_executor_->Init();
  //   auto aggregate_expr = plan_->GetAggregates();
  //   auto aggregate_type = plan_->GetAggregateTypes();
  // 构建聚合htable
  //   aht_ = std::make_shared<SimpleAggregationHashTable>(aggregate_expr, aggregate_type);
  // 必须在init里完成对子执行器的数据收集。
  // 在next里完成数据聚合
  aht_.Clear();
  Tuple child_tuple{};
  RID child_rid{};
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    //将子执行器的数据插入htable
    auto aggregate_key = MakeAggregateKey(&child_tuple);
    auto aggregate_value = MakeAggregateValue(&child_tuple);
    aht_.InsertCombine(aggregate_key, aggregate_value);
  }
  aht_iterator_ = aht_.Begin();
  is_aggregated_ = false;
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // 需要特别处理空表且count(*)的情形
  if (aht_.Begin() == aht_.End() && !is_aggregated_ && plan_->group_bys_.empty()) {
    *tuple = Tuple(aht_.GenerateInitialAggregateValue().aggregates_, &GetOutputSchema());
    is_aggregated_ = true;
    return true;
  }
  while (aht_iterator_ != aht_.End()) {
    auto aggregate_key = aht_iterator_.Key();
    auto aggregate_value = aht_iterator_.Val();
    std::vector<Value> values{};
    values.reserve(aggregate_key.group_bys_.size() + aggregate_value.aggregates_.size());
    for (auto &group_value : aggregate_key.group_bys_) {
      values.emplace_back(group_value);
    }
    for (auto &aggregate_val : aggregate_value.aggregates_) {
      values.emplace_back(aggregate_val);
    }
    *tuple = Tuple{values, &GetOutputSchema()};
    ++aht_iterator_;
    return true;
  }
  return false;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub