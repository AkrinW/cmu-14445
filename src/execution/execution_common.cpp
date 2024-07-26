#include "execution/execution_common.h"
#include "catalog/catalog.h"
#include "common/config.h"
#include "common/macros.h"
#include "concurrency/transaction_manager.h"
#include "fmt/core.h"
#include "storage/table/table_heap.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple> {
  // UNIMPLEMENTED("not implemented");
  auto new_schema = schema;
  std::vector<Value> new_values{};
  new_values.reserve(new_schema->GetColumnCount());
  // if (!base_meta.is_deleted_) {
  for (uint32_t i = 0; i < new_schema->GetColumnCount(); ++i) {
    new_values.push_back(base_tuple.GetValue(new_schema, i));
  }
  // }
  bool flag = base_meta.is_deleted_;
  auto undolog_size = undo_logs.size();
  for (size_t i = 0; i < undolog_size; ++i) {
    auto undolog = undo_logs[i];
    flag = undolog.is_deleted_;
    //  if (undolog.is_deleted_) {
    // new_values.clear();
    //  } else {
    if (!flag) {
      auto undolog_schema = GetUndoLogSchema(new_schema, undolog);
      uint32_t count = 0;
      for (uint32_t i = 0; i < new_schema->GetColumnCount(); ++i) {
        if (undolog.modified_fields_[i]) {
          new_values.at(i) = undolog.tuple_.GetValue(&undolog_schema, count);
          ++count;
        }
      }
    }
  }
  std::optional<Tuple> new_tuple;
  // if (new_values.empty()) {
  if (flag) {
    return std::nullopt;
  }
  new_tuple = std::make_optional<Tuple>({new_values, new_schema});
  return new_tuple;
}

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap) {
  // always use stderr for printing logs...
  fmt::println(stderr, "debug_hook: {}", info);
  for (auto p: txn_mgr->txn_map_) {
    std::cout << p.first  << ' '<< p.second->GetTransactionIdHumanReadable() << '\n';
    std::cout << "readts: " << p.second->GetReadTs() << " committs: " << p.second->GetCommitTs() << '\n';
  }
  for (auto p: txn_mgr->version_info_) {
    std::cout << "RID:" << p.first << '\n';
    auto prev = p.second->prev_version_;
    for (auto q: prev) {
      std::cout << " SLOT: " << q.first << ' ';
      auto pair = table_heap->GetTuple(RID(p.first,q.first));
      auto prev_txn_ = q.second.prev_.prev_txn_;
      auto log_idx = q.second.prev_.prev_log_idx_;
      auto txn = txn_mgr->txn_map_[prev_txn_];
      if (txn->GetCommitTs() != -1) {
        std::cout << "commit_ts: " << txn->GetCommitTs() << ' ';
      } else {
        std::cout << "commit_ts: txn" << txn->GetTransactionIdHumanReadable() << ' ';
      }
      if (pair.first.is_deleted_) {
        std::cout << "<is_deleted>  ";
      }
      std::cout << pair.second.ToString(&table_info->schema_) << '\n';
      std::cout << "  txn: " << txn->GetTransactionIdHumanReadable() << " txnidx: " << log_idx << ' ';
      auto undolog = txn->GetUndoLog(log_idx);
      std::cout << "tstamp: " << undolog.ts_ << ' ';
      if (undolog.is_deleted_) {
        std::cout << "<is_deleted>  ";
      }
      auto schema = GetUndoLogSchema(&table_info->schema_, undolog);
      std::cout << undolog.tuple_.ToString(&schema) << '\n';
      while (undolog.prev_version_.IsValid()) {
        prev_txn_ = undolog.prev_version_.prev_txn_;
        log_idx = undolog.prev_version_.prev_log_idx_;
        txn = txn_mgr->txn_map_[prev_txn_];
        std::cout << "  txn: " << txn->GetTransactionIdHumanReadable() << " txnidx: " << log_idx << ' ';
        undolog = txn->GetUndoLog(log_idx);
        std::cout << "tstamp: " << undolog.ts_ << ' ';
        if (undolog.is_deleted_) {
          std::cout << "<is_deleted>  ";
        }
        auto schema = GetUndoLogSchema(&table_info->schema_, undolog);
        std::cout << undolog.tuple_.ToString(&schema) << '\n';
      }
      // std::cout << "txn: " << undolog.prev_version_.prev_txn_ << " txnidx: " << undolog.prev_version_.prev_log_idx_ << '\n';
      // std::cout << "txn: " << q.second.prev_.prev_txn_ << " txnidx: " << q.second.prev_.prev_log_idx_ <<'\n';
    }
  }
  // fmt::println(
  //     stderr,
  //     "You see this line of text because you have not implemented `TxnMgrDbg`. You should do this once you have "
  //     "finished task 2. Implementing this helper function will save you a lot of time for debugging in later tasks.");

  // We recommend implementing this function as traversing the table heap and print the version chain. An example output
  // of our reference solution:
  //
  // debug_hook: before verify scan
  // RID=0/0 ts=txn8 tuple=(1, <NULL>, <NULL>)
  //   txn8@0 (2, _, _) ts=1
  // RID=0/1 ts=3 tuple=(3, <NULL>, <NULL>)
  //   txn5@0 <del> ts=2
  //   txn3@0 (4, <NULL>, <NULL>) ts=1
  // RID=0/2 ts=4 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn7@0 (5, <NULL>, <NULL>) ts=3
  // RID=0/3 ts=txn6 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn6@0 (6, <NULL>, <NULL>) ts=2
  //   txn3@1 (7, _, _) ts=1
}

auto GetUndoLogSchema(const Schema *schema, const UndoLog &undo_log) -> bustub::Schema {
  auto size = schema->GetColumnCount();
  std::vector<uint32_t> attrs{};
  for (uint32_t i = 0; i < size; ++i) {
    if (undo_log.modified_fields_[i]) {
      attrs.push_back(i);
    }
  }
  return Schema::CopySchema(schema, attrs);
}

}  // namespace bustub
