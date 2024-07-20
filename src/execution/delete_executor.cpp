//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  // throw NotImplementedException("DeleteExecutor is not implemented");
  child_executor_->Init();
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  //获取表信息table_info, schema, index
  auto table_oid = plan_->GetTableOid();
  auto catalog = exec_ctx_->GetCatalog();
  auto table_info = catalog->GetTable(table_oid);
  auto table_schema = table_info->schema_;
  auto table_name = table_info->name_;
  // auto table_index = catalog->GetIndex(table_oid);
  auto table_indexes = catalog->GetTableIndexes(table_name);

  if (is_deleted_) {
    return false;
  }

  int count = 0;
  Tuple child_tuple{};
  RID child_rid{};
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    ++count;
    table_info->table_->UpdateTupleMeta(TupleMeta{INVALID_TXN_ID, true}, child_rid);
    //更新索引
    for (auto &index_info : table_indexes) {
      auto index = index_info->index_.get();
      auto key_attrs = index->GetKeyAttrs();
      auto key_schema = index->GetKeySchema();
      auto old_key = child_tuple.KeyFromTuple(table_schema, *key_schema, key_attrs);
      index->DeleteEntry(old_key, child_rid, exec_ctx_->GetTransaction());
    }
  }
  is_deleted_ = true;
  std::vector<Value> result = {{TypeId::INTEGER, count}};
  *tuple = Tuple(result, &GetOutputSchema());
  return true;
}

}  // namespace bustub
