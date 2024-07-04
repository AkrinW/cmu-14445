//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.cpp
//
// Identification: src/buffer/clock_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/clock_replacer.h"

namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages) {
    capacity_ = num_pages;
    size_ = 0;
    sub_ = 0;
    frame_buffer_.resize(capacity_, -1);
    ref_buffer_.resize(capacity_, false);
}

ClockReplacer::~ClockReplacer() {
    frame_buffer_.clear();
    ref_buffer_.clear();
    node_store_.clear();
}

auto ClockReplacer::Victim(frame_id_t *frame_id) -> bool {
    std::lock_guard<std::mutex> lock(latch_);
    if (size_ == 0) {
        *frame_id = -1;
        frame_id = nullptr;
        return false;
    }
    while (true) {
        if (sub_ == capacity_) {
            sub_ = 0;
        }
        if (frame_buffer_[sub_] == -1) {
            ++sub_;
        } else if (ref_buffer_[sub_]) {
            ref_buffer_[sub_] = false;
            ++sub_;
        } else {
            *frame_id = frame_buffer_[sub_];
            node_store_.erase(*frame_id);
            frame_buffer_[sub_] = -1;
            --size_;
            break;
        }
    }
    return true;
}

void ClockReplacer::Pin(frame_id_t frame_id) {
    std::lock_guard<std::mutex> lock(latch_);
    if (!node_store_.count(frame_id)) {
        return;
    }
    int pos_ = node_store_[frame_id];
    frame_buffer_[pos_] = -1;
    --size_;
    node_store_.erase(frame_id);
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
    std::lock_guard<std::mutex> lock(latch_);
    if (node_store_.count(frame_id)) {
        return;
    }
    for (int i = 0; i < capacity_; ++i) {
        if (frame_buffer_[i] == -1) {
            frame_buffer_[i] = frame_id;
            ref_buffer_[i] = true;
            node_store_.emplace(frame_id, i);
            break;
        }
    }
    ++size_;
}

auto ClockReplacer::Size() -> size_t {
    return size_;
}

}  // namespace bustub
