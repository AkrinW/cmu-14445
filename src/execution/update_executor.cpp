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

#include "execution/execution_common.h"
#include "execution/executors/update_executor.h"
namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_info_(exec_ctx->GetCatalog()->GetTable(plan->GetTableOid())),
      child_executor_(std::move(child_executor)),
      cur_txn_(exec_ctx->GetTransaction()),
      txn_mgr_(exec_ctx->GetTransactionManager()) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.

  // auto table_oid = plan_->GetTableOid();
  // auto catalog = exec_ctx_->GetCatalog();
  // table_info_ = catalog->GetTable(table_oid);
}

// project 4
void UpdateExecutor::Init() {
  // pipeline breaker,需要在Init()中获取全部的tuple。
  child_executor_->Init();
  Tuple child_tuple{};
  RID child_rid{};
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    ++count_;
    child_rids_.push_back(child_rid);
    // 检查写写冲突。
    if (IsWriteWriteConflict(table_info_, child_rid, cur_txn_)) {
      // 忘记把txn设置为tainted了
      cur_txn_->SetTainted();
      throw ExecutionException("Exist writewrite conflict.");
    }
  }
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_updated_) {
    return false;
  }
  // 执行更新操作。
  for (auto &child_rid : child_rids_) {
    auto old_meta = table_info_->table_->GetTupleMeta(child_rid);
    auto old_tuple = table_info_->table_->GetTuple(child_rid).second;
    // 获取更新的tuple
    std::vector<Value> new_values{};
    new_values.reserve(plan_->target_expressions_.size());
    for (const auto &expr : plan_->target_expressions_) {
      new_values.push_back(expr->Evaluate(&old_tuple, child_executor_->GetOutputSchema()));
    }
    auto update_tuple = Tuple{new_values, &table_info_->schema_};
    if (old_meta.ts_ != cur_txn_->GetTransactionTempTs()) {
      // 前一次更改是不同的txn
      // 直接进行更新。
      // 应该对前tuple是delete的情况也进行修改。
      // 直接把tuplemeta的信息全部加到undolog里，按要求，undolog里需要尽可能保留多的信息。
      auto undolog =
          GenerateUpdateUndolog(update_tuple, child_rid, old_meta, old_tuple, table_info_, cur_txn_, txn_mgr_);
      auto undolink = cur_txn_->AppendUndoLog(undolog);
      txn_mgr_->UpdateUndoLink(child_rid, std::make_optional<UndoLink>(undolink));
    } else {
      // 相同txn情况。
      auto first_undolink = txn_mgr_->GetUndoLink(child_rid);
      if (first_undolink.has_value()) {
        // 有上一个版本，对undolog进行增量更新
        auto undolog = txn_mgr_->GetUndoLog(first_undolink.value());
        // 简单来说，undolog里保存的一定是最原始的版本，只需要比较modified_fields即可
        // auto new_tuple = ReconstructTuple(&table_info_->schema_, old_tuple, old_meta, undologs);
        auto new_undolog = IncrementalUpdateUndolog(update_tuple, undolog, child_rid, old_meta, old_tuple, table_info_,
                                                    cur_txn_, txn_mgr_);
        // GenerateDeleteUndolog(child_rid, undologs[0].ts_, new_tuple.value(), table_info_, cur_txn_, txn_mgr_);
        // 直接修改上一个UndoLog的信息，不需要进行增删
        cur_txn_->ModifyUndoLog(first_undolink.value().prev_log_idx_, new_undolog);
      } else {
        // 不存在上一个版本，是新插入的情况。
        //  什么都不需要做，直接跳出更新
      }
    }
    // 接下来对tuple和txn都进行更新
    cur_txn_->AppendWriteSet(table_info_->oid_, child_rid);
    // table_info_->table_->UpdateTupleMeta(TupleMeta{cur_txn_->GetTransactionTempTs(), false},child_rid);
    table_info_->table_->UpdateTupleInPlace(TupleMeta{cur_txn_->GetTransactionTempTs(), false}, update_tuple,
                                            child_rid);
    // todo: 更新index
  }
  is_updated_ = true;
  std::vector<Value> result = {{TypeId::INTEGER, count_}};
  *tuple = Tuple(result, &GetOutputSchema());
  return true;
}
// // project 3
// void UpdateExecutor::Init() {
//   //  throw NotImplementedException("UpdateExecutor is not implemented");
//   child_executor_->Init();
// }

// auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
//   //获取表信息table_info, schema, index
//   // auto table_oid = plan_->GetTableOid();
//   auto catalog = exec_ctx_->GetCatalog();
//   // auto table_info = catalog->GetTable(table_oid);
//   auto table_schema = table_info_->schema_;
//   auto table_name = table_info_->name_;
//   //   auto table_index = catalog->GetIndex(table_oid);
//   auto table_indexes = catalog->GetTableIndexes(table_name);

//   if (is_updated_) {
//     return false;
//   }
//   int count = 0;
//   // TupleMeta = {INVALID_TXN_ID, false};
//   Tuple child_tuple{};
//   RID child_rid{};  // 使用指针时，未初始化分配内存空间导致错误。
//   // 分配内存后能够通过测试，但是无法通过santizer，所以还是直接设置成对象更合适。
//   while (child_executor_->Next(&child_tuple, &child_rid)) {  //问题出在这里，用指针传参时出错。必须用对象的引用
//     ++count;
//     table_info_->table_->UpdateTupleMeta(TupleMeta{INVALID_TXN_ID, true}, child_rid);
//     std::vector<Value> new_values{};
//     new_values.reserve(plan_->target_expressions_.size());
//     for (const auto &expr : plan_->target_expressions_) {
//       new_values.push_back(expr->Evaluate(&child_tuple, child_executor_->GetOutputSchema()));
//     }
//     auto update_tuple = Tuple{new_values, &table_schema};
//     // 插入新元素
//     auto update_rid_optional = table_info_->table_->InsertTuple(TupleMeta{INVALID_TXN_ID, false}, update_tuple);
//     if (!update_rid_optional.has_value()) {
//       break;
//     }
//     auto update_rid = update_rid_optional.value();
//     // 更新索引
//     for (auto &index_info : table_indexes) {
//       auto index = index_info->index_.get();
//       auto key_attrs = index->GetKeyAttrs();
//       auto key_schema = index->GetKeySchema();
//       auto old_key = child_tuple.KeyFromTuple(table_schema, *key_schema, key_attrs);
//       auto new_key = update_tuple.KeyFromTuple(table_schema, *key_schema, key_attrs);
//       index->DeleteEntry(old_key, child_rid, exec_ctx_->GetTransaction());
//       index->InsertEntry(new_key, update_rid, exec_ctx_->GetTransaction());
//     }
//   }

//   is_updated_ = true;
//   std::vector<Value> result = {{TypeId::INTEGER, count}};
//   *tuple = Tuple(result, &GetOutputSchema());
//   return true;
// }

}  // namespace bustub
