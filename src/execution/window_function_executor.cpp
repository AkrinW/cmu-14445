#include "execution/executors/window_function_executor.h"
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"
#include "execution/executors/sort_executor.h"
namespace bustub {

WindowFunctionExecutor::WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void WindowFunctionExecutor::Init() { 
    // throw NotImplementedException("WindowFunctionExecutor is not implemented"); 
    child_executor_->Init();
    auto window_functions = plan_->window_functions_;
    auto cloumn_size = plan_->columns_.size();
    // 创建各类vector，用来存储window的具体信息
      // 是否需要排序
    std::vector<bool> is_order_by(cloumn_size);
      // 窗口函数表达式
    std::vector<AbstractExpressionRef> window_exprs(cloumn_size);
     // 窗口函数类型
    std::vector<WindowFunctionType> window_function_types(cloumn_size);
      // 分组条件
    std::vector<std::vector<AbstractExpressionRef>> partition_by(cloumn_size);
      // 排序条件
    std::vector<std::vector<std::pair<OrderByType, AbstractExpressionRef>>> order_by(cloumn_size);
    // 是否为函数表达式
    std::vector<bool> is_function_expr(cloumn_size);

    // 获取窗口函数的值存入
    for (uint32_t i= 0; i < cloumn_size; ++i) {
        if (window_functions.find(i) == window_functions.end()) {
            // 纯数值列，无窗口函数
            is_function_expr[i] = false;
            window_exprs[i] = plan_->columns_[i];
            is_order_by[i] = false;
            window_hash_table_.emplace_back(window_function_types[i]);
        } else {
            is_function_expr[i] = true;
            const auto &window_function = window_functions.find(i)->second;
            window_exprs[i] = window_function.function_;
            window_function_types[i] = window_function.type_;
            partition_by[i] = window_function.partition_by_;
            order_by[i] = window_function.order_by_;
            // 即使有窗口函数，也可能不需要排序
            is_order_by[i] = !window_function.order_by_.empty();
            window_hash_table_.emplace_back(window_function_types[i]);
        }
    }

    Tuple child_tuple{};
    RID child_rid{};
    std::vector<Tuple> tuples;
    while (child_executor_->Next(&child_tuple, &child_rid)) {
        tuples.emplace_back(child_tuple);
    }
// 获取order_by_，这里因为文档中说了，所有的窗口函数都只支持一个order_by，所以直接取第一个即可
// const auto &order_by = window_functions.begin()->second.order_by_;
    if (!order_by[0].empty()) {
        //对元组排序
        std::sort(tuples.begin(),tuples.end(), Comparator(&child_executor_->GetOutputSchema(), order_by[0]));
    }
    std::vector<std::vector<AggregateKey>> tuple_keys;
// 获取窗口函数中的聚合函数或者rank函数
  for (const auto &this_tuple : tuples) {
    std::vector<Value> values{};
    std::vector<AggregateKey> keys;
    // 遍历元组列，判断符合条件的列
    for (uint32_t i = 0; i < cloumn_size; ++i) {
      // 如果是函数表达式，则需要处理
      if (is_function_expr[i]) {
        // 获取窗口函数的key
        auto agg_key = MakeWinKey(&this_tuple, partition_by[i]);
        // 如果是rank函数，则需要特殊处理
        if (window_function_types[i] == WindowFunctionType::Rank) {
          // 获取该列的最新值
          auto new_value = order_by[0].second->Evaluate(&this_tuple, this->GetOutputSchema());
          // 这里是rank函数，需要判断该值是否与之前的值相同，如果相同则，rank等级一样
          values.emplace_back(whts_[i].InsertCombine(agg_key, new_value));
          keys.emplace_back(agg_key);
          continue;
        }
        // 聚合函数的情况下，与前面聚合函数的处理一样
        auto agg_val = MakeWinValue(&this_tuple, window_exprs[i]);
        values.emplace_back(whts_[i].InsertCombine(agg_key, agg_val));
        keys.emplace_back(agg_key);
        continue;
      }
      // 对于没有窗口函数的列，直接将列存入vector中
      values.emplace_back(window_exprs[i]->Evaluate(&this_tuple, this->GetOutputSchema()));
      keys.emplace_back();
    }
    // 将更新后的列值存入tuple的vector中
    tuples_.emplace_back(std::move(values));
    // 将更新后的key存入tuple_keys的vector中
    tuple_keys.emplace_back(std::move(keys));
  }
    }

auto WindowFunctionExecutor::Next(Tuple *tuple, RID *rid) -> bool { return false; }
}  // namespace bustub
