//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

LRUKNode::LRUKNode(bustub::frame_id_t f, size_t k, bool e) : fid_(f), k_(k), is_evictable_(e) {
  last_ = nullptr;
  next_ = nullptr;
}

void LRUKNode::PushRecord(size_t timestamp_) {
  if (history_.size() == k_) {
    history_.pop_front();
  }
  history_.push_back(timestamp_);
}

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  current_timestamp_ = 0;
  curr_size_ = 0;
  head_ = new LRUKNode(-1, k_);
  end_ = new LRUKNode(-1, k_);
  head_->next_ = end_;
  end_->last_ = head_;
  head_k_ = new LRUKNode(-1, k_);
  end_k_ = new LRUKNode(-1, k_);
  head_k_->next_ = end_k_;
  end_k_->last_ = head_k_;
}

LRUKReplacer::~LRUKReplacer() {
  for (auto node : node_store_) {
    delete node.second;
  }
  node_store_.clear();
  delete head_;
  delete end_;
  delete head_k_;
  delete end_k_;
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  if (curr_size_ == 0) {
    *frame_id = -1;
    frame_id = nullptr;
    return false;
  }
  LRUKNode *p;
  if (head_->next_ != end_) {
    p = head_->next_;
  } else {
    p = head_k_->next_;
  }
  p->last_->next_ = p->next_;
  p->next_->last_ = p->last_;
  --curr_size_;
  *frame_id = p->fid_;
  node_store_.erase(p->fid_);
  delete p;
  return true;
}

// void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
//   std::lock_guard<std::mutex> lock(latch_);
//   if (frame_id > static_cast<int>(replacer_size_)) {
//     throw std::runtime_error("invalid frame_id");
//   }
//   if (node_store_.count(frame_id) == 0) {
//     auto *p = new LRUKNode(frame_id, k_);
//     node_store_.emplace(frame_id, p);
//   }
//   auto p = node_store_[frame_id];
//   p->PushRecord(current_timestamp_);
//   ++current_timestamp_;
//   if (p->is_evictable_) {
//     p->last_->next_ = p->next_;
//     p->next_->last_ = p->last_;
//     if (p->history_.size() < k_) {
//       p->next_ = end_;
//       p->last_ = end_->last_;
//       end_->last_->next_ = p;
//       end_->last_ = p;
//     } else {
//       auto num = p->history_.front();
//       LRUKNode *q = head_k_->next_;
//       while (!q->history_.empty() && q->history_.front() < num) {
//         q = q->next_;
//       }
//       p->next_ = q;
//       p->last_ = q->last_;
//       q->last_->next_ = p;
//       q->last_ = p;
//     }
//   }
// }

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::lock_guard<std::mutex> lock(latch_);
  if (frame_id > static_cast<int>(replacer_size_)) {
    throw std::runtime_error("invalid frame_id");
  }
  if (node_store_.count(frame_id) == 0) {
    auto *p = new LRUKNode(frame_id, k_);
    node_store_.emplace(frame_id, p);
  }
  auto p = node_store_[frame_id];
  p->PushRecord(current_timestamp_);
  ++current_timestamp_;
  if (p->is_evictable_) {
    p->last_->next_ = p->next_;
    p->next_->last_ = p->last_;
    LRUKNode *q;
    auto num = p->history_.front();

    if (p->history_.size() < k_) {
      q = head_->next_;
    } else {
      q = head_k_->next_;
    }
    while (!q->history_.empty() && q->history_.front() < num) {
      q = q->next_;
    }
    p->next_ = q;
    p->last_ = q->last_;
    q->last_->next_ = p;
    q->last_ = p;
  }
}

// void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
//   std::lock_guard<std::mutex> lock(latch_);
//   if (node_store_.count(frame_id) == 0 || frame_id > static_cast<int>(replacer_size_)) {
//     throw std::runtime_error("invalid frame_id");
//   }
//   if (node_store_.at(frame_id)->is_evictable_ == set_evictable) {
//     return;
//   }
//   LRUKNode *p = node_store_.at(frame_id);
//   p->is_evictable_ = set_evictable;
//   if (set_evictable) {
//     ++curr_size_;
//     if (p->history_.size() == k_) {
//       auto num = p->history_.front();
//       LRUKNode *q = head_k_->next_;
//       while (!q->history_.empty() && q->history_.front() < num) {
//         q = q->next_;
//       }
//       p->next_ = q;
//       p->last_ = q->last_;
//       q->last_->next_ = p;
//       q->last_ = p;
//     } else {
//       auto num = p->history_.back();
//       LRUKNode *q = end_->last_;
//       while (!q->history_.empty() && q->history_.back() > num) {
//         q = q->last_;
//       }
//       p->last_ = q;
//       p->next_ = q->next_;
//       q->next_->last_ = p;
//       q->next_ = p;
//     }
//   } else {
//     --curr_size_;
//     p->last_->next_ = p->next_;
//     p->next_->last_ = p->last_;
//   }
// }

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(latch_);
  if (node_store_.count(frame_id) == 0 || frame_id > static_cast<int>(replacer_size_)) {
    throw std::runtime_error("invalid frame_id");
  }
  if (node_store_.at(frame_id)->is_evictable_ == set_evictable) {
    return;
  }
  LRUKNode *p = node_store_.at(frame_id);
  p->is_evictable_ = set_evictable;
  if (set_evictable) {
    ++curr_size_;
    auto num = p->history_.front();
    LRUKNode *q;
    if (p->history_.size() == k_) {
      q = head_k_->next_;
    } else {
      q = head_->next_;
    }
    while (!q->history_.empty() && q->history_.front() < num) {
      q = q->next_;
    }
    p->next_ = q;
    p->last_ = q->last_;
    q->last_->next_ = p;
    q->last_ = p;
  } else {
    --curr_size_;
    p->last_->next_ = p->next_;
    p->next_->last_ = p->last_;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);
  if (node_store_.count(frame_id) == 0) {
    return;
  }
  LRUKNode *p = node_store_.at(frame_id);
  if (!p->is_evictable_) {
    throw std::runtime_error("can't evict frame_id");
  }
  p->last_->next_ = p->next_;
  p->next_->last_ = p->last_;
  delete p;
  node_store_.erase(frame_id);
  --curr_size_;
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub