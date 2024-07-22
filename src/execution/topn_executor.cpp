#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  //初始化存在调用虚函数的问题，所以只好把优先级队列的初始化放在Init()里。
  //   que_ = std::make_unique<std::priority_queue<Tuple, std::vector<Tuple>, Comparator>>(
  //       Comparator(&GetOutputSchema(), plan_->GetOrderBy()));
}

void TopNExecutor::Init() {
  // throw NotImplementedException("TopNExecutor is not implemented");
  que_ = std::make_unique<std::priority_queue<Tuple, std::vector<Tuple>, Comparator>>(
      Comparator(&GetOutputSchema(), plan_->GetOrderBy()));
  child_executor_->Init();
  auto max_size = plan_->GetN();
  Tuple tuple{};
  RID rid{};
  while (child_executor_->Next(&tuple, &rid)) {
    que_->push(tuple);
    ++heap_size_;
    if (heap_size_ > max_size) {
      que_->pop();
      --heap_size_;
    }
  }
  while (!que_->empty()) {
    values_.push_back(que_->top());
    que_->pop();
  }
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (values_.empty()) {
    return false;
  }
  *tuple = values_.back();
  values_.pop_back();

  return true;
}

auto TopNExecutor::GetNumInHeap() -> size_t {
  // throw NotImplementedException("TopNExecutor is not implemented");
  return heap_size_;
};

}  // namespace bustub
