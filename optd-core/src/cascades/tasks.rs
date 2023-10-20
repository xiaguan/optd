use anyhow::Result;

use super::CascadesOptimizer;

mod apply_rule;
mod explore_expression;
mod explore_group;
mod optimize_expression;
mod optimize_group;
mod optimize_inputs;

use apply_rule::ApplyRuleTask;
use explore_expression::ExploreExpressionTask;
use explore_group::ExploreGroupTask;
use optimize_expression::OptimizeExpressionTask;
use optimize_group::OptimizeGroupTask;
use optimize_inputs::OptimizeInputsTask;

trait Task {
    fn execute(&self, optimizer: &mut CascadesOptimizer) -> Result<Vec<Box<dyn Task>>>;
}
