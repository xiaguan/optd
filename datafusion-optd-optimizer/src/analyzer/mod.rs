// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{TreeNode, VisitRecursion};
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::expr::Exists;
use datafusion_expr::expr::InSubquery;
use datafusion_expr::utils::inspect_expr_pre;
use datafusion_expr::{Expr, LogicalPlan};
use log::debug;
use std::sync::Arc;
use std::time::Instant;

/// [`AnalyzerRule`]s transform [`LogicalPlan`]s in some way to make
/// the plan valid prior to the rest of the DataFusion optimization process.
///
/// For example, it may resolve [`Expr`]s into more specific forms such
/// as a subquery reference, to do type coercion to ensure the types
/// of operands are correct.
///
/// This is different than an [`OptimizerRule`](crate::OptimizerRule)
/// which should preserve the semantics of the LogicalPlan but compute
/// it the same result in some more optimal way.
pub trait AnalyzerRule {
    /// Rewrite `plan`
    fn analyze(&self, plan: LogicalPlan, config: &ConfigOptions) -> Result<LogicalPlan>;

    /// A human readable name for this analyzer rule
    fn name(&self) -> &str;
}

/// A rule-based Analyzer.
#[derive(Clone)]
pub struct Analyzer {
    /// All rules to apply
    pub rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>>,
}

impl Default for Analyzer {
    fn default() -> Self {
        Self::new()
    }
}

impl Analyzer {
    /// Create a new analyzer using the recommended list of rules
    pub fn new() -> Self {
        Self::with_rules(vec![])
    }

    /// Create a new analyzer with the given rules
    pub fn with_rules(rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>>) -> Self {
        Self { rules }
    }

    /// Analyze the logical plan by applying analyzer rules, and
    /// do necessary check and fail the invalid plans
    pub fn execute_and_check<F>(
        &self,
        plan: &LogicalPlan,
        config: &ConfigOptions,
        mut observer: F,
    ) -> Result<LogicalPlan>
    where
        F: FnMut(&LogicalPlan, &dyn AnalyzerRule),
    {
        let start_time = Instant::now();
        let mut new_plan = plan.clone();

        // TODO: add common rule executor for Analyzer and Optimizer
        for rule in &self.rules {
            new_plan = rule
                .analyze(new_plan, config)
                .map_err(|e| DataFusionError::Context(rule.name().to_string(), Box::new(e)))?;
            observer(&new_plan, rule.as_ref());
        }
        Ok(new_plan)
    }
}
