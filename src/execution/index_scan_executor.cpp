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
#include "execution/execution_common.h"


namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  auto table_oid = plan_->table_oid_;
  auto index_oid = plan_->index_oid_;
  auto catalog = exec_ctx_->GetCatalog();
  table_info_ = catalog->GetTable(table_oid);
  index_info_ = catalog->GetIndex(index_oid);
  table_heap_ = table_info_->table_.get();
  htable_ = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(index_info_->index_.get());
}

// init里的东西不能随便挪到next中。原因和执行器的执行方式有关系。
// 测试执行的方式是一次只输入一个key值，因此直接在init里获取值。
void IndexScanExecutor::Init() {
  auto table_schema = index_info_->key_schema_;
  auto key = plan_->pred_key_;
  auto value = key->val_;
  std::vector<Value> values{value};
  Tuple index_key(values, &table_schema);
  result_rid_.clear();
  htable_->ScanKey(index_key, &result_rid_, exec_ctx_->GetTransaction());
  cur_ts_rd_ = exec_ctx_->GetTransaction()->GetReadTs();
  cur_ts_txn_ = exec_ctx_->GetTransaction()->GetTransactionId();
  is_scaned_ = false;
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // 模仿project3 和 seqscan修改index，具体而言，获取到rid后，检查undolog即可
  if (is_scaned_) {
    return false;
  }
  is_scaned_ = true;

  if (result_rid_.empty()) {
    return false;
  }
  *rid = result_rid_.front();
  auto [meta, get_tuple] = table_heap_->GetTuple(*rid);
  if (meta.ts_ == cur_ts_txn_ || meta.ts_ <= cur_ts_rd_) {
    *tuple = get_tuple;
    if (meta.is_deleted_) {
      return false;
    }
  } else {
    std::vector<UndoLog> undologs{};
    CreateUndolog(*rid, cur_ts_rd_, exec_ctx_->GetTransactionManager(), undologs);
    // 无法构建，跳过tuple
    if (undologs.empty()) {
      return false;
    }
    auto new_tuple = ReconstructTuple(&table_info_->schema_,get_tuple,meta,undologs);
    if (!new_tuple.has_value()) {
      // 没有查到或者是已删除的，跳过这个tuple
      return false;
    }
    *tuple = std::move(new_tuple.value());
  }
  return true;
  // // project 3
  // TupleMeta meta{};
  // meta = table_heap_->GetTuple(*result_rid_.begin()).first;
  // if (!meta.is_deleted_) {
  //   *tuple = table_heap_->GetTuple(*result_rid_.begin()).second;
  //   *rid = *result_rid_.begin();
  // }
  // return true;
}

}  // namespace bustub