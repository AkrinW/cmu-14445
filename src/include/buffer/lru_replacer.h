//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.h
//
// Identification: src/include/buffer/lru_replacer.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <mutex>  // NOLINT
#include <unordered_map>
#include <vector>
#include "buffer/replacer.h"
#include "common/config.h"

namespace bustub {

/**
 * LRUReplacer implements the Least Recently Used replacement policy.
 */
class LRUReplacer : public Replacer {
 public:
  /**
   * Create a new LRUReplacer.
   * @param num_pages the maximum number of pages the LRUReplacer will be required to store
   */
  explicit LRUReplacer(size_t num_pages);

  /**
   * Destroys the LRUReplacer.
   */
  ~LRUReplacer() override;

  auto Victim(frame_id_t *frame_id) -> bool override;

  void Pin(frame_id_t frame_id) override;

  void Unpin(frame_id_t frame_id) override;

  auto Size() -> size_t override;

 private:
  // TODO(student): implement me!
  struct LRUNode {
    frame_id_t id_;
    LRUNode *next_;
    LRUNode *last_;
    LRUNode(frame_id_t i = 0) : id_(i), next_(nullptr), last_(nullptr) {}
    LRUNode(frame_id_t i, LRUNode *next = nullptr, LRUNode *last = nullptr) : id_(i), next_(next), last_(last) {}
  };
  LRUNode *head, *end;
  std::mutex latch_;
  std::unordered_map<frame_id_t, LRUNode *> node_store_;
  size_t capacity_;
  size_t size_;
};

}  // namespace bustub
