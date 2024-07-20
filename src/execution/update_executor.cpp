//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
  auto table_oid = plan_->GetTableOid();
  auto catalog = exec_ctx_->GetCatalog();
  table_info_ = catalog->GetTable(table_oid);
}

void UpdateExecutor::Init() {
  //  throw NotImplementedException("UpdateExecutor is not implemented");
  child_executor_->Init();
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  //获取表信息table_info, schema, index
  // auto table_oid = plan_->GetTableOid();
  auto catalog = exec_ctx_->GetCatalog();
  // auto table_info = catalog->GetTable(table_oid);
  auto table_schema = table_info_->schema_;
  auto table_name = table_info_->name_;
  //   auto table_index = catalog->GetIndex(table_oid);
  auto table_indexes = catalog->GetTableIndexes(table_name);

  if (is_updated_) {
    return false;
  }
  int count = 0;
  // TupleMeta = {INVALID_TXN_ID, false};
  Tuple child_tuple{};
  RID child_rid{};  // 使用指针时，未初始化分配内存空间导致错误。
  // 分配内存后能够通过测试，但是无法通过santizer，所以还是直接设置成对象更合适。
  while (child_executor_->Next(&child_tuple, &child_rid)) {  //问题出在这里，用指针传参时出错。必须用对象的引用
    ++count;
    table_info_->table_->UpdateTupleMeta(TupleMeta{INVALID_TXN_ID, true}, child_rid);
    std::vector<Value> new_values{};
    new_values.reserve(plan_->target_expressions_.size());
    for (const auto &expr : plan_->target_expressions_) {
      new_values.push_back(expr->Evaluate(&child_tuple, child_executor_->GetOutputSchema()));
    }
    auto update_tuple = Tuple{new_values, &table_schema};
    // 插入新元素
    auto update_rid_optional = table_info_->table_->InsertTuple(TupleMeta{INVALID_TXN_ID, false}, update_tuple);
    if (!update_rid_optional.has_value()) {
      break;
    }
    auto update_rid = update_rid_optional.value();
    // 更新索引
    for (auto &index_info : table_indexes) {
      auto index = index_info->index_.get();
      auto key_attrs = index->GetKeyAttrs();
      auto key_schema = index->GetKeySchema();
      auto old_key = child_tuple.KeyFromTuple(table_schema, *key_schema, key_attrs);
      auto new_key = update_tuple.KeyFromTuple(table_schema, *key_schema, key_attrs);
      index->DeleteEntry(old_key, child_rid, exec_ctx_->GetTransaction());
      index->InsertEntry(new_key, update_rid, exec_ctx_->GetTransaction());
    }
  }

  is_updated_ = true;
  std::vector<Value> result = {{TypeId::INTEGER, count}};
  *tuple = Tuple(result, &GetOutputSchema());
  return true;
}

}  // namespace bustub
