//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {
  capacity_ = num_pages;
  size_ = 0;
  head = new LRUNode();
  end = new LRUNode();
  head->next_ = end;
  end->last_ = head;
}

LRUReplacer::~LRUReplacer() {
  LRUNode *cur = head;
  while (cur != nullptr) {
    LRUNode *p = cur;
    cur = cur->next_;
    delete p;
  }
  node_store_.clear();
}

auto LRUReplacer::Victim(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  if (size_ == 0) {
    *frame_id = -1;
    frame_id = nullptr;
    return false;
  }
  LRUNode *victim_ = head->next_;
  *frame_id = victim_->id_;
  node_store_.erase(victim_->id_);
  head->next_ = victim_->next_;
  victim_->next_->last_ = head;
  --size_;
  delete victim_;
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);
  if (!node_store_.count(frame_id)) {
    return;
  }
  LRUNode *pin_ = node_store_[frame_id];
  pin_->last_->next_ = pin_->next_;
  pin_->next_->last_ = pin_->last_;
  delete pin_;
  --size_;
  node_store_.erase(frame_id);
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);
  if (node_store_.count(frame_id)) {
    return;
  }
  LRUNode *unpin_ = new LRUNode(frame_id, end, end->last_);
  unpin_->last_->next_ = unpin_;
  end->last_ = unpin_;
  ++size_;
  node_store_.emplace(frame_id, unpin_);
}

auto LRUReplacer::Size() -> size_t { return size_; }

}  // namespace bustub
