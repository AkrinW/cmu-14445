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
#include "execution/execution_common.h"
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
  cur_ts_rd_ = exec_ctx_->GetTransaction()->GetReadTs();
  cur_ts_txn_ = exec_ctx_->GetTransaction()->GetTransactionId();
  cur_schema_ = table_info->schema_;
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  //in project4
  if (iter_->IsEnd()) {
    return false;
  }
  // TupleMeta meta{cur_ts_, false};
  while (true) {
    if (iter_->IsEnd()) {
      return false;
    }
    auto pair = iter_->GetTuple();
    auto tuple_time = pair.first.ts_;
    *rid = pair.second.GetRid();
    if (tuple_time == cur_ts_txn_ || tuple_time <= cur_ts_rd_) {
      *tuple = pair.second;
    } else {
      // 需要当前版本对txn不可见，需要寻找历史版本
      auto undologs = CreateUndolog(*rid, cur_ts_rd_, exec_ctx_->GetTransactionManager());
    }
    ++*iter_;
  }
  //in project3
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