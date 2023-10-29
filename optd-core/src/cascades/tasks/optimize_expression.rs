use anyhow::Result;
use tracing::trace;

use crate::{
    cascades::{
        optimizer::{CascadesOptimizer, GroupExprId},
        tasks::ApplyRuleTask,
    },
    rel_node::RelNodeTyp,
};

use super::Task;

pub struct OptimizeExpressionTask {
    expr_id: GroupExprId,
    exploring: bool,
}

impl OptimizeExpressionTask {
    pub fn new(expr_id: GroupExprId, exploring: bool) -> Self {
        Self { expr_id, exploring }
    }
}

impl<T: RelNodeTyp> Task<T> for OptimizeExpressionTask {
    fn execute(&self, optimizer: &mut CascadesOptimizer<T>) -> Result<Vec<Box<dyn Task<T>>>> {
        let expr = optimizer.get_group_expr_memo(self.expr_id);
        trace!(event = "task_begin", task = "optimize_expr", expr_id = %self.expr_id, expr = %expr);
        let mut tasks = vec![];
        for (rule_id, rule) in optimizer.rules().iter().enumerate() {
            if optimizer.is_rule_fired(self.expr_id, rule_id) {
                continue;
            }
            optimizer.mark_rule_fired(self.expr_id, rule_id);
            if rule.matches(expr.typ, expr.data.clone()) {
                tasks.push(Box::new(ApplyRuleTask::new(rule_id, self.expr_id)) as Box<dyn Task<T>>);
            }
        }
        trace!(event = "task_end", task = "optimize_expr", expr_id = %self.expr_id);
        Ok(tasks)
    }
}
