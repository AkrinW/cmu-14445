//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {
        auto table_oid = plan_->table_oid_;
        auto index_oid = plan_->index_oid_;
        auto catalog = exec_ctx_->GetCatalog();
        table_info_ = catalog->GetTable(table_oid);
        index_info_ = catalog->GetIndex(index_oid);
        table_heap_ = table_info_->table_.get();
        htable_ = dynamic_cast<HashTableIndexForTwoIntegerColumn*>(index_info_->index_.get());
    }

void IndexScanExecutor::Init() { 
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    if (is_scaned_) {
        return false;
    }
    is_scaned_ = true;
    auto table_schema = index_info_->key_schema_;
    auto key = plan_->pred_key_;
    auto value = key->val_;
    std::vector<Value> values{value};
    Tuple index_key(values, &table_schema);
    result_rid_.clear();
    htable_->ScanKey(index_key, &result_rid_, exec_ctx_->GetTransaction());
    if (result_rid_.empty()) {
        return false;
    }
    TupleMeta meta{};
    meta = table_heap_->GetTuple(*result_rid_.begin()).first;
    if (!meta.is_deleted_) {
        *tuple = table_heap_->GetTuple(*result_rid_.begin()).second;
        *rid = *result_rid_.begin();
    }
    return true;
    }

}  // namespace bustub
