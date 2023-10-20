use anyhow::Result;

use crate::cascades::optimizer::{CascadesOptimizer, GroupExprId};

use super::Task;

pub struct OptimizeExpressionTask {
    expr_id: GroupExprId,
}

impl OptimizeExpressionTask {
    pub fn new(expr_id: GroupExprId) -> Self {
        Self { expr_id }
    }
}

impl Task for OptimizeExpressionTask {
    fn execute(&self, optimizer: &mut CascadesOptimizer) -> Result<Vec<Box<dyn Task>>> {
        Ok(vec![])
    }
}
