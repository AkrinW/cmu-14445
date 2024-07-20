//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  // throw NotImplementedException("SeqScanExecutor is not implemented"); s
  auto table_oid = plan_->GetTableOid();
  auto catalog = exec_ctx_->GetCatalog();
  auto table_info = catalog->GetTable(table_oid);
  auto &table = table_info->table_;
  iter_ = std::make_unique<TableIterator>(table->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_->IsEnd()) {
    return false;
  }
  TupleMeta meta = {INVALID_TXN_ID, false};
  auto schema = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid())->schema_;
  while (true) {
    if (iter_->IsEnd()) {
      return false;
    }
    meta = iter_->GetTuple().first;
    if (!meta.is_deleted_) {
      *tuple = iter_->GetTuple().second;
      *rid = iter_->GetRID();
    }
    ++*iter_;
    if (!meta.is_deleted_ &&
        (plan_->filter_predicate_ == nullptr ||
         plan_->filter_predicate_
             ->Evaluate(tuple, GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid())->schema_)
             .GetAs<bool>())) {
      break;
    }
  }
  return true;
}
}  // namespace bustub