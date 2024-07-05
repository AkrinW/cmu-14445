//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  // 分配页面，只能分配在限量的frame_id里。page_id无限增加，frame_id有限
  frame_id_t frame_id = -1;
  std::lock_guard<std::mutex> lock(latch_);
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    if (!replacer_->Evict(&frame_id)) {
      return nullptr;
    }
  }
  *page_id = AllocatePage();
  auto page = pages_ + frame_id;
  ResetPageNolock(*page_id, frame_id);
  return page;
}

// 无锁版本的页面替换。
void BufferPoolManager::ResetPageNolock(page_id_t new_page_id, frame_id_t frame_id) {
  auto page = pages_ + frame_id;
  if (page->IsDirty()) {
    FlushPageNolock(page->page_id_);
  }
  // 把原表存的page_id删除，添加新的。并修改page和replacer的状态。
  page_table_.erase(page->GetPageId());
  page_table_.emplace(new_page_id, frame_id);
  page->page_id_ = new_page_id;
  page->pin_count_ = 1;
  page->ResetMemory();
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);
  if (page_id == INVALID_PAGE_ID) {
    return nullptr;
  }
  frame_id_t frame_id = -1;
  Page *page;
  if (page_table_.count(page_id) != 0) {
    // 存在page，直接操作并返回。
    frame_id = page_table_.at(page_id);
    page = pages_ + frame_id;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    ++page->pin_count_;  // pin_count_不是01，而是可以有多个线程pin的，驱逐时，必须让所有的都unpin了才可以。
  } else {
    // 没有找到，需要new或者替代一个。
    if (!free_list_.empty()) {
      frame_id = free_list_.front();
      free_list_.pop_front();
    } else {
      if (!replacer_->Evict(&frame_id)) {
        return nullptr;
      }
    }
    page = pages_ + frame_id;
    ResetPageNolock(page_id, frame_id);
    // 因为是新页面，需要从磁盘读取
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    disk_scheduler_->Schedule({false, page->GetData(), page->GetPageId(), std::move(promise)});
    future.get();
  }
  return page;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  // 先查找page_id对应在buffer的frameid
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  std::lock_guard<std::mutex> lock(latch_);
  if (page_table_.count(page_id) == 0) {
    return false;
  }
  // unpin就是把原来pin的变为unpin。replacer对应的状态也要变。
  auto frame_id = page_table_.at(page_id);
  auto page = pages_ + frame_id;
  page->is_dirty_ = (page->is_dirty_ || is_dirty);
  if (page->pin_count_ == 0) {
    return false;
  }
  --page->pin_count_;
  if (page->pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  std::lock_guard<std::mutex> lock(latch_);
  if (page_table_.count(page_id) == 0) {
    return false;
  }
  FlushPageNolock(page_id);
  return true;
}

// 无锁版本的写回磁盘
void BufferPoolManager::FlushPageNolock(page_id_t page_id) {
  auto page = pages_ + page_table_.at(page_id);
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  disk_scheduler_->Schedule({true, page->GetData(), page->GetPageId(), std::move(promise)});
  future.get();
  page->is_dirty_ = false;
}

// 把buffer内的page全部写回
void BufferPoolManager::FlushAllPages() {
  std::lock_guard<std::mutex> lock(latch_);
  for (auto page : page_table_) {
    auto page_id = page.first;
    FlushPageNolock(page_id);
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  if (page_table_.count(page_id) == 0) {
    return true;
  }
  auto frame_id = page_table_.at(page_id);
  auto page = pages_ + frame_id;
  if (page->GetPinCount() != 0) {
    return false;
  }
  page_table_.erase(page_id);
  free_list_.push_back(frame_id);
  replacer_->Remove(frame_id);
  page->ResetMemory();
  page->page_id_ = INVALID_PAGE_ID;
  page->is_dirty_ = false;
  page->pin_count_ = 0;
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }

}  // namespace bustub
