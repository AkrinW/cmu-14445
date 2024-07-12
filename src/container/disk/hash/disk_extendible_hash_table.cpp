//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_extendible_hash_table.cpp
//
// Identification: src/container/disk/hash/disk_extendible_hash_table.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "common/util/hash_util.h"
#include "container/disk/hash/disk_extendible_hash_table.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/extendible_htable_bucket_page.h"
#include "storage/page/extendible_htable_directory_page.h"
#include "storage/page/extendible_htable_header_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

template <typename K, typename V, typename KC>
DiskExtendibleHashTable<K, V, KC>::DiskExtendibleHashTable(const std::string &name, BufferPoolManager *bpm,
                                                           const KC &cmp, const HashFunction<K> &hash_fn,
                                                           uint32_t header_max_depth, uint32_t directory_max_depth,
                                                           uint32_t bucket_max_size)
    : bpm_(bpm),
      cmp_(cmp),
      hash_fn_(std::move(hash_fn)),
      header_max_depth_(header_max_depth),
      directory_max_depth_(directory_max_depth),
      bucket_max_size_(bucket_max_size) {
  index_name_ = name;
  header_page_id_ = INVALID_PAGE_ID;
  auto header_guard = bpm->NewPageGuarded(&header_page_id_);
  auto header_page = header_guard.AsMut<ExtendibleHTableHeaderPage>();
  header_page->Init(header_max_depth_);
  // throw NotImplementedException("DiskExtendibleHashTable is not implemented");
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result, Transaction *transaction) const
    -> bool {
  // hash获取id，如果id非法return false
  auto hash = Hash(key);
  // 获取header_page
  auto header_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_guard.As<ExtendibleHTableHeaderPage>();
  auto directory_index = header_page->HashToDirectoryIndex(hash);
  auto directory_page_id = header_page->GetDirectoryPageId(directory_index);
  if (static_cast<int>(directory_page_id) == INVALID_PAGE_ID) {
    return false;
  }
  header_guard.Drop();

  // 在directory层
  auto directory_guard = bpm_->FetchPageRead(directory_page_id);
  auto directory_page = directory_guard.template As<ExtendibleHTableDirectoryPage>();
  auto bucket_index = directory_page->HashToBucketIndex(hash);
  auto bucket_page_id = directory_page->GetBucketPageId(bucket_index);
  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }
  directory_guard.Drop();

  //在bucket层
  auto bucket_guard = bpm_->FetchPageRead(bucket_page_id);
  auto bucket_page = bucket_guard.template As<ExtendibleHTableBucketPage<K, V, KC>>();
  V value;
  if (bucket_page->Lookup(key, value, cmp_)) {
    result->push_back(value);
    return true;
  }
  bucket_guard.Drop();

  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, Transaction *transaction) -> bool {
  auto hash = Hash(key);
  // header层
  auto header_guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = header_guard.AsMut<ExtendibleHTableHeaderPage>();
  auto directory_index = header_page->HashToDirectoryIndex(hash);
  auto directory_page_id = header_page->GetDirectoryPageId(directory_index);
  header_guard.Drop();

  // directory层
  WritePageGuard directory_guard;
  ExtendibleHTableDirectoryPage *directory_page;
  if (static_cast<int>(directory_page_id) == INVALID_PAGE_ID) {
    directory_guard = bpm_->NewPageGuarded(reinterpret_cast<bustub::page_id_t *>(&directory_page_id)).UpgradeWrite();
    directory_page = directory_guard.AsMut<ExtendibleHTableDirectoryPage>();
    directory_page->Init(directory_max_depth_);
    header_page->SetDirectoryPageId(directory_index, directory_page_id);
  } else {
    directory_guard = bpm_->FetchPageWrite(directory_page_id);
    directory_page = directory_guard.AsMut<ExtendibleHTableDirectoryPage>();
  }
  auto bucket_index = directory_page->HashToBucketIndex(hash);
  auto bucket_page_id = directory_page->GetBucketPageId(bucket_index);

  // bucket层
  WritePageGuard bucket_guard;
  ExtendibleHTableBucketPage<K, V, KC> *bucket_page;
  if (bucket_page_id == INVALID_PAGE_ID) {
    bucket_guard = bpm_->NewPageGuarded(&bucket_page_id).UpgradeWrite();
    bucket_page = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    bucket_page->Init(bucket_max_size_);
    directory_page->SetBucketPageId(bucket_index, bucket_page_id);
  } else {
    bucket_guard = bpm_->FetchPageWrite(bucket_page_id);
    bucket_page = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  }
  V tmp;
  if (bucket_page->Lookup(key, tmp, cmp_)) {
    return false;  //存在已有key
  }
  if (bucket_page->Insert(key, value, cmp_)) {
    return true;  //插入成功，返回
  }

  // 插入失败的情况，需要扩充表
  auto h = 1U << directory_page->GetGlobalDepth();
  if (directory_page->GetLocalDepth(bucket_index) == directory_page->GetGlobalDepth()) {
    if (directory_page->GetGlobalDepth() >= directory_page->GetMaxDepth()) {
      return false;
    }
    directory_page->IncrGlobalDepth();
    //更新目录页
    for (uint32_t i = h; i < (1U << directory_page->GetGlobalDepth()); ++i) {
      auto new_bucket_page_id = directory_page->GetBucketPageId(i - h);
      auto new_local_depth = directory_page->GetLocalDepth(i - h);
      directory_page->SetBucketPageId(i, new_bucket_page_id);
      directory_page->SetLocalDepth(i, new_local_depth);
    }
  }
  directory_page->IncrLocalDepth(bucket_index);
  directory_page->IncrLocalDepth(bucket_index + h);
  // 拆分bucket
  if (!SplitBucket(directory_page, bucket_page, bucket_index)) {
    return false;
  }
  directory_guard.Drop();
  bucket_guard.Drop();
  //重新插入
  return Insert(key, value, transaction);
}

template <class K, class V, class KC>
auto DiskExtendibleHashTable<K, V, KC>::SplitBucket(ExtendibleHTableDirectoryPage *directory,
                                                    ExtendibleHTableBucketPage<K, V, KC> *bucket, u_int32_t bucket_idx)
    -> bool {
  page_id_t split_page_id;
  WritePageGuard split_bucket_guard = bpm_->NewPageGuarded(&split_page_id).UpgradeWrite();
  if (split_page_id == INVALID_PAGE_ID) {
    return false;
  }
  auto split_bucket = split_bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  split_bucket->Init(bucket_max_size_);
  auto split_idx = directory->GetSplitImageIndex(bucket_idx);
  auto local_depth = directory->GetLocalDepth(bucket_idx);
  directory->SetBucketPageId(split_idx, split_page_id);
  directory->SetLocalDepth(split_idx, local_depth);
  // 拆分pageid到两个页
  auto bucket_page_id = directory->GetBucketPageId(bucket_idx);
  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }
  auto size = bucket->Size();
  std::list<std::pair<K, V>> entries;
  for (uint32_t i = 0; i < size; i++) {
    entries.emplace_back(bucket->EntryAt(i));
  }
  bucket->Clear();
  for (auto &entry : entries) {
    auto target_idx = directory->HashToBucketIndex(Hash(entry.first));
    auto target_page_id = directory->GetBucketPageId(target_idx);
    if (target_page_id == bucket_page_id) {
      bucket->Insert(entry.first, entry.second, cmp_);
    } else if (target_page_id == split_page_id) {
      split_bucket->Insert(entry.first, entry.second, cmp_);
    }
  }
  return true;
}

// template <typename K, typename V, typename KC>
// auto DiskExtendibleHashTable<K, V, KC>::InsertToNewDirectory(ExtendibleHTableHeaderPage *header, uint32_t
// directory_idx,
//                                                              uint32_t hash, const K &key, const V &value) -> bool {
//   return false;
// }

// template <typename K, typename V, typename KC>
// auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t
// bucket_idx,
//                                                           const K &key, const V &value) -> bool {
//   return false;
// }

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryMapping(ExtendibleHTableDirectoryPage *directory,
                                                               uint32_t new_bucket_idx, page_id_t new_bucket_page_id,
                                                               uint32_t new_local_depth, uint32_t local_depth_mask) {
  // throw NotImplementedException("DiskExtendibleHashTable is not implemented");
  for (uint32_t i = 0; i < (1U << directory->GetGlobalDepth()); ++i) {
    if (directory->GetBucketPageId(i) == directory->GetBucketPageId(new_bucket_idx)) {
      if ((i & local_depth_mask) != 0U) {
        directory->SetBucketPageId(i, new_bucket_page_id);
      }
      directory->SetLocalDepth(i, new_local_depth);
    }
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  auto hash = Hash(key);
  // header层
  auto header_guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = header_guard.AsMut<ExtendibleHTableHeaderPage>();
  auto directory_index = header_page->HashToDirectoryIndex(hash);
  auto directory_page_id = header_page->GetDirectoryPageId(directory_index);
  header_guard.Drop();

  // directory层
  if (static_cast<int>(directory_page_id) == INVALID_PAGE_ID) {
    return false;
  }
  auto directory_guard = bpm_->FetchPageWrite(directory_page_id);
  auto directory_page = directory_guard.template AsMut<ExtendibleHTableDirectoryPage>();
  auto bucket_index = directory_page->HashToBucketIndex(hash);
  auto bucket_page_id = directory_page->GetBucketPageId(bucket_index);
  directory_guard.Drop();

  // bucket层
  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }
  auto bucket_guard = bpm_->FetchPageWrite(bucket_page_id);
  auto bucket_page = bucket_guard.template AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  if (!bucket_page->Remove(key, cmp_)) {
    return false;
  }
  bucket_guard.Drop();
  // 成功删除，对页表处理
  auto check_page_id = bucket_page_id;
  auto check_guard = bpm_->FetchPageRead(check_page_id);
  auto check_page = check_guard.template As<ExtendibleHTableBucketPage<K, V, KC>>();
  auto local_depth = directory_page->GetLocalDepth(bucket_index);
  auto global_depth = directory_page->GetGlobalDepth();
  while (local_depth > 0) {
    uint32_t convert_mask = 1 << (local_depth - 1);
    uint32_t merge_bucket_index = bucket_index ^ convert_mask;
    uint32_t merge_local_depth = directory_page->GetLocalDepth(merge_bucket_index);
    auto merge_page_id = directory_page->GetBucketPageId(merge_bucket_index);
    auto merge_guard = bpm_->FetchPageRead(merge_page_id);
    auto merge_page = merge_guard.template As<ExtendibleHTableBucketPage<K, V, KC>>();
    if (merge_local_depth != local_depth || (!check_page->IsEmpty() && !merge_page->IsEmpty())) {
      break;
    }
    if (check_page->IsEmpty()) {
      bpm_->DeletePage(check_page_id);
      check_page = merge_page;
      check_page_id = merge_page_id;
      check_guard = std::move(merge_guard);
    } else {
      bpm_->DeletePage(merge_page_id);
    }
    directory_page->DecrLocalDepth(bucket_index);
    local_depth = directory_page->GetLocalDepth(bucket_index);
    uint32_t local_depth_mask = directory_page->GetLocalDepthMask(bucket_index);
    uint32_t mask_idx = bucket_index & local_depth_mask;
    uint32_t update_count = 1 << (global_depth - local_depth);
    for (uint32_t i = 0; i < update_count; ++i) {
      uint32_t tmp_idx = (i << local_depth) + mask_idx;
      UpdateDirectoryMapping(directory_page, tmp_idx, check_page_id, local_depth, 0);
    }
  }
  while (directory_page->CanShrink()) {
    directory_page->DecrGlobalDepth();
  }
  return true;
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
