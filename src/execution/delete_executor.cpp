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

#include "execution/execution_common.h"
#include "execution/executors/delete_executor.h"
namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      table_info_(exec_ctx->GetCatalog()->GetTable(plan->GetTableOid())),
      cur_txn_(exec_ctx->GetTransaction()),
      txn_mgr_(exec_ctx->GetTransactionManager()) {}

// project 4
void DeleteExecutor::Init() {
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

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_deleted_) {
    return false;
  }
  // 执行删除操作。
  for (auto &child_rid : child_rids_) {
    auto old_meta = table_info_->table_->GetTupleMeta(child_rid);
    auto old_tuple = table_info_->table_->GetTuple(child_rid).second;
    if (old_meta.ts_ != cur_txn_->GetTransactionTempTs()) {
      // 前一次更改是不同的txn
      // 把当前tuple作为undolog，添加到txnmgr和curtxn里。
      auto undolog = GenerateDeleteUndolog(child_rid, old_meta.ts_, old_tuple, table_info_, cur_txn_, txn_mgr_);
      auto undolink = cur_txn_->AppendUndoLog(undolog);
      txn_mgr_->UpdateUndoLink(child_rid, std::make_optional<UndoLink>(undolink));
    } else {
      // todo 是同一个txn的情况
      // 先从undolog里恢复到上一个版本，再进行删除。
      auto first_undolink = txn_mgr_->GetUndoLink(child_rid);
      if (first_undolink.has_value()) {
        // 有上一个版本，获取后重构
        std::vector<UndoLog> undologs{txn_mgr_->GetUndoLog(first_undolink.value())};
        auto new_tuple = ReconstructTuple(&table_info_->schema_, old_tuple, old_meta, undologs);
        auto undolog =
            GenerateDeleteUndolog(child_rid, undologs[0].ts_, new_tuple.value(), table_info_, cur_txn_, txn_mgr_);
          // 有上一个版本的情况，undolink不能连接到自身
          undolog.prev_version_ = undologs[0].prev_version_;
        // 直接修改上一个UndoLog的信息，不需要进行增删
        cur_txn_->ModifyUndoLog(first_undolink.value().prev_log_idx_, undolog);
      } else {
        // 不存在上一个版本，是新插入的情况。
        //  什么都不需要做，直接跳出更新
      }
    }
    cur_txn_->AppendWriteSet(table_info_->oid_, child_rid);
    table_info_->table_->UpdateTupleMeta(TupleMeta{cur_txn_->GetTransactionTempTs(), true}, child_rid);
    // todo 更新index
  }
  is_deleted_ = true;
  std::vector<Value> result = {{TypeId::INTEGER, count_}};
  *tuple = Tuple(result, &GetOutputSchema());
  return true;
}

// // project3
// void DeleteExecutor::Init() {
//   // throw NotImplementedException("DeleteExecutor is not implemented");
//   child_executor_->Init();
// }

// auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
//   //获取表信息table_info, schema, index
//   auto table_oid = plan_->GetTableOid();
//   auto catalog = exec_ctx_->GetCatalog();
//   auto table_info = catalog->GetTable(table_oid);
//   auto table_schema = table_info->schema_;
//   auto table_name = table_info->name_;
//   // auto table_index = catalog->GetIndex(table_oid);
//   auto table_indexes = catalog->GetTableIndexes(table_name);

//   if (is_deleted_) {
//     return false;
//   }

//   int count = 0;
//   Tuple child_tuple{};
//   RID child_rid{};
//   while (child_executor_->Next(&child_tuple, &child_rid)) {
//     ++count;
//     table_info->table_->UpdateTupleMeta(TupleMeta{INVALID_TXN_ID, true}, child_rid);
//     //更新索引
//     for (auto &index_info : table_indexes) {
//       auto index = index_info->index_.get();
//       auto key_attrs = index->GetKeyAttrs();
//       auto key_schema = index->GetKeySchema();
//       auto old_key = child_tuple.KeyFromTuple(table_schema, *key_schema, key_attrs);
//       index->DeleteEntry(old_key, child_rid, exec_ctx_->GetTransaction());
//     }
//   }
//   is_deleted_ = true;
//   std::vector<Value> result = {{TypeId::INTEGER, count}};
//   *tuple = Tuple(result, &GetOutputSchema());
//   return true;
// }

}  // namespace bustub
