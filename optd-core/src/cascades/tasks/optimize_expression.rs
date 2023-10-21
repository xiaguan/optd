use anyhow::Result;
use tracing::trace;

use crate::{
    cascades::optimizer::{CascadesOptimizer, GroupExprId},
    rel_node::RelNodeTyp,
};

use super::Task;

pub struct OptimizeExpressionTask {
    expr_id: GroupExprId,
}

impl OptimizeExpressionTask {
    pub fn new(expr_id: GroupExprId) -> Self {
        Self { expr_id }
    }
}

impl<T: RelNodeTyp> Task<T> for OptimizeExpressionTask {
    fn execute(&self, optimizer: &mut CascadesOptimizer<T>) -> Result<Vec<Box<dyn Task<T>>>> {
        trace!(name: "task_begin", task = "optimize_expr", expr_id = %self.expr_id);
        Ok(vec![])
    }
}
