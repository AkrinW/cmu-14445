#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
    bpm_ = that.bpm_;
    page_ = that.page_;
    is_dirty_ = that.is_dirty_;
    that.bpm_ = nullptr;
    that.page_ = nullptr;
}

void BasicPageGuard::Drop() {
    if (bpm_ != nullptr && page_ != nullptr) {
        bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
    }
    bpm_ = nullptr;
    page_ = nullptr;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & { 
    Drop();
    bpm_ = that.bpm_;
    page_ = that.page_;
    is_dirty_ = that.is_dirty_;
    that.bpm_ = nullptr;
    that.page_ = nullptr;
    return *this; 
}

BasicPageGuard::~BasicPageGuard(){
    Drop();
};  // NOLINT

auto BasicPageGuard::UpgradeRead()->ReadPageGuard {
    if (page_ != nullptr) {
        page_->RLatch();
    }
    auto read_page_guard = ReadPageGuard(bpm_, page_);
    bpm_ = nullptr;
    page_ = nullptr;
    return read_page_guard;
}

auto BasicPageGuard::UpgradeWrite()->WritePageGuard {
    if (page_ != nullptr) {
        page_->WLatch();
    }
    auto write_page_guard = WritePageGuard(bpm_, page_);
    bpm_ = nullptr;
    page_ = nullptr;
    return write_page_guard;
}


ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept = default;

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & { return *this; }

void ReadPageGuard::Drop() {}

ReadPageGuard::~ReadPageGuard() {}  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept = default;

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & { return *this; }

void WritePageGuard::Drop() {}

WritePageGuard::~WritePageGuard() {}  // NOLINT

}  // namespace bustub
