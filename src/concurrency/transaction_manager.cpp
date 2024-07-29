//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <memory>
#include <mutex>  // NOLINT
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "execution/execution_common.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto TransactionManager::Begin(IsolationLevel isolation_level) -> Transaction * {
  std::unique_lock<std::shared_mutex> l(txn_map_mutex_);
  auto txn_id = next_txn_id_++;
  auto txn = std::make_unique<Transaction>(txn_id, isolation_level);
  auto *txn_ref = txn.get();
  txn_map_.insert(std::make_pair(txn_id, std::move(txn)));

  // TODO(fall2023): set the timestamps here. Watermark updated below.
  txn_ref->read_ts_ = last_commit_ts_.load();

  running_txns_.AddTxn(txn_ref->read_ts_);
  return txn_ref;
}

auto TransactionManager::VerifyTxn(Transaction *txn) -> bool { return true; }

auto TransactionManager::Commit(Transaction *txn) -> bool {
  std::unique_lock<std::mutex> commit_lck(commit_mutex_);

  // TODO(fall2023): acquire commit ts!
  txn->commit_ts_ = last_commit_ts_ + 1;
  // 需要在提交结束后再增加。
  // ++last_commit_ts_;
  if (txn->state_ != TransactionState::RUNNING) {
    throw Exception("txn not in running state");
  }

  if (txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
    if (!VerifyTxn(txn)) {
      commit_lck.unlock();
      Abort(txn);
      return false;
    }
  }

  // TODO(fall2023): Implement the commit logic!

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  // 遍历txn的write_set_，修改每个tuple的ts为commit
  auto write_set = txn->GetWriteSets();
  for (const auto &p : write_set) {
    auto table_oid = p.first;
    auto table_info = catalog_->GetTable(table_oid);
    for (auto rid : p.second) {
      auto meta = table_info->table_->GetTupleMeta(rid);
      TupleMeta new_meta{txn->GetCommitTs(), meta.is_deleted_};
      table_info->table_->UpdateTupleMeta(new_meta, rid);
    }
  }
  // TODO(fall2023): set commit timestamp + update last committed timestamp here.

  txn->state_ = TransactionState::COMMITTED;
  ++last_commit_ts_;
  running_txns_.UpdateCommitTs(txn->GetCommitTs());
  running_txns_.RemoveTxn(txn->GetReadTs());
  return true;
}

void TransactionManager::Abort(Transaction *txn) {
  if (txn->state_ != TransactionState::RUNNING && txn->state_ != TransactionState::TAINTED) {
    throw Exception("txn not in running / tainted state");
  }

  // TODO(fall2023): Implement the abort logic!

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  txn->state_ = TransactionState::ABORTED;
  running_txns_.RemoveTxn(txn->read_ts_);
}

void TransactionManager::GarbageCollection() {
  // UNIMPLEMENTED("not implemented");
  // 垃圾回收考虑所有commit低于watermark的txn，检测txn的undolog是否还在使用，如果没有就可以删除。
  // 遍历version_info里所有的undolog，把没用的标为invalid。
  // 再遍历所有的txn，如果txn的所有undolog都是invalid，就可以删除。
  for (const auto &page_id : version_info_) {
    auto page = page_id.first;
    auto pageversion = page_id.second;
    for (const auto &slot_id : pageversion->prev_version_) {
      auto slot = slot_id.first;
      UndoLink undo_link = slot_id.second.prev_;
      auto rid = RID(page, slot);
      // 获取tuplemeta里存储的时间。
      // 这里发现似乎任意一个tableoid都可以查所有的tablepage，不知道有没有问题。
      auto last_ts = catalog_->GetTable(0)->table_->GetTupleMeta(rid).ts_;
      while (undo_link.IsValid()) {
        auto undolog = GetUndoLog(undo_link);
        auto cur_ts = undolog.ts_;
        if (cur_ts >= running_txns_.watermark_) {
          // 当前的undolog还在使用中。
          last_ts = cur_ts;
          undo_link = undolog.prev_version_;
        } else {
          // 已经落后watermark了。还要检查前一个ts是否等于watermark，如果大于，则这个undolog还不能删除
          if (last_ts > running_txns_.watermark_) {
            last_ts = cur_ts;
            undo_link = undolog.prev_version_;
          } else {
            // 前一个ts≤watermark，那么当前的undolog没有用了，把这个和他后面的undolog都设置成invalid_ts
            // 需要把上一个undolog连接的undolink也设置为invalid
            while (undo_link.IsValid()) {
              undolog = GetUndoLog(undo_link);
              // 这里只改了副本，没有实际修改存在txn里的值
              undolog.ts_ = INVALID_TS;
              // 更新txn里的值
              txn_map_[undo_link.prev_txn_]->ModifyUndoLog(undo_link.prev_log_idx_, undolog);
              undo_link = undolog.prev_version_;
            }
          }
        }
      }
    }
  }
  // 遍历txn，删除全是invalid的txn
  std::vector<txn_id_t> to_deleted{};
  for (const auto &t : txn_map_) {
    if (t.second->GetCommitTs() == INVALID_TS) {
      // 考虑没commit过的txn,需要跳过他们
      continue;
    }
    auto undologs = t.second->undo_logs_;
    // if (undologs.empty()) {
    //   // 没有undolog的txn也不能删除，直接跳过
    //   continue;
    // }
    bool flag = true;
    for (const auto &log : undologs) {
      if (log.ts_ != INVALID_TS) {
        flag = false;
        break;
      }
    }
    if (flag) {
      // 不能直接删除，先把txnid储存起来，在另一个循环里删除。
      // txn_map_.erase(t.first);
      to_deleted.push_back(t.first);
    }
  }
  for (auto t : to_deleted) {
    txn_map_.erase(t);
  }
  // 前面修改时已经调整好了，这里不需要再检查一遍。
  // 删除后重新检查一遍首个节点的值。如果指向的txn被删除了，需要修改为invalid
  for (const auto &page_id : version_info_) {
    auto pageversion = page_id.second;
    for (auto slot_id : pageversion->prev_version_) {
      auto undo_link = slot_id.second.prev_;
      if (undo_link.IsValid() && txn_map_.find(undo_link.prev_txn_) == txn_map_.end()) {
        UpdateUndoLink(RID{page_id.first, static_cast<uint32_t>(slot_id.first)}, UndoLink{INVALID_TXN_ID, 0});
        undo_link.prev_txn_ = INVALID_TXN_ID;
      }
      while (undo_link.IsValid()) {
        auto txn = txn_map_[undo_link.prev_txn_];
        auto txn_idx = undo_link.prev_log_idx_;
        auto undolog = txn->GetUndoLog(txn_idx);
        undo_link = undolog.prev_version_;
        if (undo_link.IsValid() && txn_map_.find(undo_link.prev_txn_) == txn_map_.end()) {
          undolog.prev_version_.prev_txn_ = INVALID_TXN_ID;
          txn->ModifyUndoLog(txn_idx, undolog);
          undo_link.prev_txn_ = INVALID_TXN_ID;
        }
      }
    }
  }
}

}  // namespace bustub
