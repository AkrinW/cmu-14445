#include "execution/executors/filter_executor.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

FilterExecutor::FilterExecutor(ExecutorContext *exec_ctx, const FilterPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void FilterExecutor::Init() {
  // Initialize the child executor
  child_executor_->Init();
}

auto FilterExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto filter_expr = plan_->GetPredicate();
  std::cout << filter_expr->ToString() << '\n';
  while (true) {
    // Get the next tuple
    const auto status = child_executor_->Next(tuple, rid);

    if (!status) {
      return false;
    }

    auto value = filter_expr->Evaluate(tuple, child_executor_->GetOutputSchema());
    std::cout << value.ToString() << '\t' << child_executor_->GetOutputSchema().ToString() << '\n';

    if (!value.IsNull() && value.GetAs<bool>()) {
      return true;
    }
  }
}

}  // namespace bustub
