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
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() { 
    // throw NotImplementedException("IndexScanExecutor is not implemented"); 
    //获取表信息table_info, schema, index
    auto table_oid = plan_->table_oid_;
    auto index_oid = plan_->GetIndexOid();
    auto catalog = exec_ctx_->GetCatalog();
    auto table_info = catalog->GetTable(table_oid);
    auto index_info = catalog->GetIndex(index_oid);
    table_heap_ = table_info->table_.get();

    htable_ = dynamic_cast<HashTableIndexForTwoIntegerColumn*>(index_info->index_.get());
    auto index_schema = index_info->key_schema_;
    // 获取键值
    auto key = plan_->pred_key_;
    auto value = key->val_;
    std::vector<Value> values{value};
    // 用索引的键模式创建键
    Tuple index_key{values, &index_schema};
    result_rid_.clear();
    htable_->ScanKey(index_key, &result_rid_, exec_ctx_->GetTransaction());
    }

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    //获取表信息table_info, schema, index
    // auto table_oid = plan_->table_oid_;
    // auto index_oid = plan_->GetIndexOid();
    // auto catalog = exec_ctx_->GetCatalog();
    // auto table_info = catalog->GetTable(table_oid);
    // auto index_info = catalog->GetIndex(index_oid);
    // auto table_schema = table_info->schema_;
    // auto table_name = table_info->name_;
    // auto table_heap = table_info->table_.get();
    // auto table_indexes = catalog->GetTableIndexes(table_name);
    // htable_ = dynamic_cast<HashTableIndexForTwoIntegerColumn*>(index_info->index_.get());

    // 用索引的键模式创建键
    // Tuple index_key{values, &index_schema};
    // std::vector<RID> result_rid;
    // htable_->ScanKey(index_key, &result_rid, exec_ctx_->GetTransaction());

    if (result_rid_.empty()){
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
