//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  // throw NotImplementedException("InsertExecutor is not implemented");
  child_executor_->Init();
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  //获取表信息table_info, schema, index
  auto table_oid = plan_->GetTableOid();
  auto catalog = exec_ctx_->GetCatalog();
  auto table_info = catalog->GetTable(table_oid);
  auto table_schema = table_info->schema_;
  auto table_name = table_info->name_;
  //   auto table_index = catalog->GetIndex(table_oid);
  auto table_indexes = catalog->GetTableIndexes(table_name);

  if (is_done_) {
    return false;
  }
  int row_num = 0;
  TupleMeta meta = {INVALID_TXN_ID, false};

  //   auto table_index = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);

  while (child_executor_->Next(tuple, rid)) {
    // try: insert (tuple from child) to table
    auto new_tuple_rid = table_info->table_->InsertTuple(meta, *tuple);
    if (!new_tuple_rid.has_value()) {
      break;
    }
    *rid = new_tuple_rid.value();
    // try: insert (index of this tuple) to (b_plus_index_tree of table)
    for (auto &index : table_indexes) {
      auto key = tuple->KeyFromTuple(table_schema, index->key_schema_, index->index_->GetKeyAttrs());
      index->index_->InsertEntry(key, *rid, exec_ctx_->GetTransaction());
    }
    // iteration
    ++row_num;
  }

  is_done_ = true;
  std::vector<Value> result = {{TypeId::INTEGER, row_num}};
  *tuple = Tuple(result, &GetOutputSchema());
  return true;
}
}  // namespace bustub
