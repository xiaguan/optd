use anyhow::Result;
use tracing::debug;

use crate::cascades::{
    optimizer::GroupId, tasks::optimize_expression::OptimizeExpressionTask, CascadesOptimizer,
};

use super::Task;

pub struct OptimizeGroupTask {
    group_id: GroupId,
}

impl OptimizeGroupTask {
    pub fn new(group_id: GroupId) -> Self {
        Self { group_id }
    }
}

impl Task for OptimizeGroupTask {
    fn execute(&self, optimizer: &mut CascadesOptimizer) -> Result<Vec<Box<dyn Task>>> {
        debug!(name: "task_begin", task = "optimize_group", group_id = %self.group_id);
        let exprs = optimizer.get_group_exprs(self.group_id);
        let mut tasks = vec![];
        for expr in exprs {
            tasks.push(Box::new(OptimizeExpressionTask::new(expr)) as Box<dyn Task>);
        }
        Ok(tasks)
    }
}
