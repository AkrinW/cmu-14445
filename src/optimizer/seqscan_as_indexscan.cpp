#include "optimizer/optimizer.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/logic_expression.h"
#include "storage/index/generic_key.h"
namespace bustub {

auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement seq scan with predicate -> index scan optimizer rule
  // The Filter Predicate Pushdown has been enabled for you in optimizer.cpp when forcing starter rule
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSeqScanAsIndexScan(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  // paln计划是顺序扫描，转换成索引
  if (optimized_plan->GetType() == PlanType::SeqScan) {
    const auto &seq_scan_plan = dynamic_cast<const SeqScanPlanNode &>(*optimized_plan);
    auto predicate =seq_scan_plan.filter_predicate_;
    if (predicate != nullptr) {//如果谓词为空，需要顺序扫描，否则进一步优化
      auto table_name = seq_scan_plan.table_name_;
      auto table_index = catalog_.GetTableIndexes(table_name);
      auto logic_expr = std::dynamic_pointer_cast<LogicExpression>(predicate);
      if (!table_index.empty() && !logic_expr) {//没有索引或者有多个谓词条件，返回顺序扫描
        auto equal_expr = std::dynamic_pointer_cast<ComparisonExpression>(predicate);
        // 需要判断是否是条件谓词
        if (equal_expr) {
          auto com_type = equal_expr->comp_type_;
          if (com_type == ComparisonType::Equal) {
            auto table_oid = seq_scan_plan.table_oid_;
            //返回索引表节点
            auto column_expr = dynamic_cast<const ColumnValueExpression &>(*equal_expr->GetChildAt(0));
            // 根据谓词的列获取表的info
            auto column_index = column_expr.GetColIdx();
            auto col_name = this->catalog_.GetTable(table_oid)->schema_.GetColumn(column_index).GetName();
            //存在相关索引，获取索引info
            for (auto *index:table_index) {
              const auto &columns = index->index_->GetKeyAttrs();
              std::vector<uint32_t> column_ids;
              column_ids.push_back(column_index);
              if (columns == column_ids) {
                //获取predkey
                auto pred_key = std::dynamic_pointer_cast<ConstantValueExpression>(equal_expr->GetChildAt(1));
                //从智能指针获取裸指针
                ConstantValueExpression *raw_pred_key = pred_key ? pred_key.get() : nullptr;
                return std::make_shared<IndexScanPlanNode>(seq_scan_plan.output_schema_, table_oid, index->index_oid_,predicate,raw_pred_key);
              }
            }
          }
        }
      }
    }
  }
  return optimized_plan;
}

}  // namespace bustub
