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

LRUKNode::LRUKNode(bustub::frame_id_t f, size_t k, bool e = false): fid_(f), k_(k), is_evictable_(e) {
    last = nullptr;
    next = nullptr;
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
    head_->next = end_;
    end_->last = head_;
    head_k = new LRUKNode(-1, k_);
    end_k = new LRUKNode(-1, k);
    head_k->next = end_k;
    end_k->last = head_k;
}

LRUKReplacer::~LRUKReplacer() {
    for (auto node: node_store_) {
        delete node.second;
    }
    node_store_.clear();
    delete head_;
    delete end_;
    delete head_k;
    delete end_k;
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
    if (curr_size_ == 0) {
        *frame_id = -1;
        frame_id = nullptr;
        return false;
    }
    LRUKNode *p;
    if (head_->next != end_->next) {
        p = head_->next;
        head_->next = p->next;
        p->next->last = head_;
    } else {
        p = head_k->next;
        head_k->next = p->next;
        p->next->last = head_k;
    }
    
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
    std::lock_guard<std::mutex> lock(latch_);
    if (!node_store_.count(frame_id)) {
        throw std::runtime_error("invalid frame_id");
    }
    if (node_store_.at(frame_id)->is_evictable_ == set_evictable) {
        return;
    }
    LRUKNode *p = node_store_.at(frame_id);
    p->is_evictable_ = set_evictable;
    if (set_evictable) {
        ++curr_size_;
    } else {
        --curr_size_;
    }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
    std::lock_guard<std::mutex> lock(latch_);
    if (!node_store_.count(frame_id)) {
        return;
    }
    LRUKNode *p = node_store_.at(frame_id);
    if (!p->is_evictable_) {
        throw std::runtime_error("can't evict frame_id");
    }
    p->last->next = p->next;
    p->next->last = p->last;
    delete p;
    node_store_.erase(frame_id);
    --curr_size_;
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
