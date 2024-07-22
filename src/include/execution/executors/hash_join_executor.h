//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>
#include "common/util/hash_util.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"


namespace bustub {
  // copy from aggregate
/** AggregateKey represents a key in an aggregation operation */
struct HashJoinKey {
  /** The group-by values */
  std::vector<Value> group_bys_;
  /**
   * Compares two aggregate keys for equality.
   * @param other the other aggregate key to be compared with
   * @return `true` if both aggregate keys have equivalent group-by expressions, `false` otherwise
   */
  auto operator==(const HashJoinKey &other) const -> bool {
    for (uint32_t i = 0; i < other.group_bys_.size(); i++) {
      if (group_bys_[i].CompareEquals(other.group_bys_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
  HashJoinKey() {
    group_bys_.clear();
  }
  HashJoinKey(const Value & value) {
    group_bys_.push_back(value);
  }
  void AddKey(const Value & value) {
    group_bys_.push_back(value);
  }
  auto GetVector()->std::vector<Value> * {
    return &group_bys_;
  }
};
// copy from aggregate
/** AggregateValue represents a value for each of the running aggregates */
struct HashJoinValue {
  /** The aggregate values */
  std::vector<Tuple> hashjoins_;
  HashJoinValue() {
    hashjoins_.clear();
  }
  HashJoinValue(const Tuple &tuple) {
    hashjoins_.push_back(tuple);
  }
  void AddTuple(const Tuple &tuple) {
    hashjoins_.push_back(tuple);
  }
  auto GetVector() -> std::vector<Tuple>* {
    return &hashjoins_;
  }
};
}  // namespace bustub

namespace std {

/** Implements std::hash on AggregateKey */
template <>
struct hash<bustub::HashJoinKey> {
  auto operator()(const bustub::HashJoinKey &agg_key) const -> std::size_t {
    size_t curr_hash = 0;
    for (const auto &key : agg_key.group_bys_) {
      if (!key.IsNull()) {
        curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&key));
      }
    }
    return curr_hash;
  }
};

}  // namespace std

namespace bustub {



class SimpleHashJoinHashTable {
  public:
    void InsertKey(const HashJoinKey &key, const Tuple &tuple) {
      if (htable_.count(key) == 0) {
        htable_.insert({key, HashJoinValue(tuple)});
      } else {
        htable_.at(key).AddTuple(tuple);
      }
    }
    auto GetValue(const HashJoinKey &key) -> std::vector<Tuple> * {
      if (htable_.find(key) == htable_.end()) {
        return nullptr;
      }
      return htable_.at(key).GetVector();
    }
    void Clear() {
      htable_.clear();
    }
  private:
    std::unordered_map<HashJoinKey, HashJoinValue> htable_;
};



/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join.
   * @param[out] rid The next tuple RID, not used by hash join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  auto GetLeftJoinKey(const Tuple *tuple)-> HashJoinKey;
  auto GetRightJoinKey(const Tuple *tuple)->HashJoinKey;
  /** The HashJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> left_child_;
  std::unique_ptr<AbstractExecutor> right_child_;
  Tuple left_tuple_{};
  RID left_rid_{};
  Tuple right_tuple_{};
  RID right_rid_{};

  std::unique_ptr<SimpleHashJoinHashTable> jht_;
  bool if_hashjoined_{false};
  bool left_bool_{false};
  std::vector<Tuple>* right_tuple_vector_{nullptr};
  std::vector<Tuple>::iterator iter_;
};

}  // namespace bustub