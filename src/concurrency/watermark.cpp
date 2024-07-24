#include "concurrency/watermark.h"
#include <algorithm>
#include <exception>
#include "common/exception.h"
namespace bustub {

auto Watermark::AddTxn(timestamp_t read_ts) -> void {
  if (read_ts < commit_ts_) {
    throw Exception("read ts < commit ts");
  }
  // TODO(fall2023): implement me!
  auto [iter, inserted] = current_reads_.emplace(read_ts);
  if (inserted) {
    // If the read timestamp was newly inserted, update the watermark.
    if (read_ts < watermark_) {
      watermark_ = read_ts;
    }
  }
}

auto Watermark::RemoveTxn(timestamp_t read_ts) -> void {
  // TODO(fall2023): implement me!
  auto iter = current_reads_.find(read_ts);
  if (iter != current_reads_.end()) {
    current_reads_.erase(iter);
    // After removal, update the watermark.
    if (current_reads_.empty()) {
      watermark_ = commit_ts_;
    } else if (read_ts == watermark_) {
      watermark_ = *current_reads_.begin();  // New minimum.
    }
  }
}

}  // namespace bustub
