use anyhow::Result;
use tracing::trace;

use crate::{
    cascades::{
        optimizer::{CascadesOptimizer, GroupExprId, RuleId},
        tasks::OptimizeExpressionTask,
    },
    rel_node::RelNodeTyp,
};

use super::Task;

pub struct ApplyRuleTask {
    rule_id: RuleId,
    expr_id: GroupExprId,
}

impl ApplyRuleTask {
    pub fn new(rule_id: RuleId, expr_id: GroupExprId) -> Self {
        Self { rule_id, expr_id }
    }
}

impl<T: RelNodeTyp> Task<T> for ApplyRuleTask {
    fn execute(&self, optimizer: &mut CascadesOptimizer<T>) -> Result<Vec<Box<dyn Task<T>>>> {
        trace!(name: "task_begin", task = "apply_rule", expr_id = %self.expr_id, rule_id = %self.rule_id);
        let group_id = optimizer.get_group_id(self.expr_id);
        let binding_exprs = optimizer.get_group_expr(self.expr_id);
        let rule = optimizer.rules()[self.rule_id].clone();
        let mut tasks = vec![];
        for expr in binding_exprs {
            let applied = rule.apply(expr);
            for expr in applied {
                let (_, expr_id) = optimizer.add_group_expr(expr, Some(group_id));
                trace!(name: "apply_rule", expr_id = %self.expr_id, rule_id = %self.rule_id, new_expr_id = %expr_id);
                tasks
                    .push(Box::new(OptimizeExpressionTask::new(expr_id, true)) as Box<dyn Task<T>>);
            }
        }
        trace!(name: "task_end", task = "apply_rule", expr_id = %self.expr_id, rule_id = %self.rule_id);
        Ok(tasks)
    }
}
