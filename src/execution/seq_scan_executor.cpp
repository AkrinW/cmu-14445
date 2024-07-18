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
    : AbstractExecutor(exec_ctx), plan_(plan) {
  // table_oid_t oid = plan_->GetTableOid();
  // table_info_ = exec_ctx_->GetCatalog()->GetTable(oid);
  // auto iter = table_info_->table_->MakeIterator();
}

// SeqScanExecutor::~SeqScanExecutor() {
//     if (iter_ != nullptr) {
//         delete iter_;
//     }
//     iter_ = nullptr;
// }

void SeqScanExecutor::Init() {
  // throw NotImplementedException("SeqScanExecutor is not implemented"); s
  auto table_oid_ = plan_->GetTableOid();
  auto catalog = exec_ctx_->GetCatalog();
  auto table_info = catalog->GetTable(table_oid_);
  auto &table = table_info->table_;
  iter_ = std::make_unique<TableIterator>(table->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_->IsEnd()) {
    return false;
  }
  while (!iter_->IsEnd()) {
    *rid = iter_->GetRID();
    auto [meta, new_tuple] = iter_->GetTuple();
    ++(*iter_);
    if (!meta.is_deleted_) {
      if (plan_->filter_predicate_) {  // 处理优化器将filter下推到seq_scan的情况
        auto value = plan_->filter_predicate_->Evaluate(tuple, GetOutputSchema());
        if (value.IsNull() || !value.GetAs<bool>()) {
          return false;
        }
      }
      *tuple = new_tuple;
      return true;
    }
  }

  return true;
}
}  // namespace bustub
