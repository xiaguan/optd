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

//! Query optimizer traits

use chrono::{DateTime, Utc};
use datafusion_common::alias::AliasGenerator;
use datafusion_common::config::ConfigOptions;
use datafusion_common::Result;
use datafusion_expr::logical_plan::LogicalPlan;
use std::sync::Arc;

pub enum ApplyOrder {
    TopDown,
    BottomUp,
}

/// `OptimizerRule` transforms one [`LogicalPlan`] into another which
/// computes the same results, but in a potentially more efficient
/// way. If there are no suitable transformations for the input plan,
/// the optimizer can simply return it as is.
pub trait OptimizerRule {
    /// Try and rewrite `plan` to an optimized form, returning None if the plan cannot be
    /// optimized by this rule.
    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>>;

    /// A human readable name for this optimizer rule
    fn name(&self) -> &str;

    /// How should the rule be applied by the optimizer? See comments on [`ApplyOrder`] for details.
    ///
    /// If a rule use default None, it should traverse recursively plan inside itself
    fn apply_order(&self) -> Option<ApplyOrder> {
        None
    }
}

/// Options to control the DataFusion Optimizer.
pub trait OptimizerConfig {
    /// Return the time at which the query execution started. This
    /// time is used as the value for now()
    fn query_execution_start_time(&self) -> DateTime<Utc>;

    /// Return alias generator used to generate unique aliases for sub-queries
    fn alias_generator(&self) -> Arc<AliasGenerator>;

    fn options(&self) -> &ConfigOptions;
}

/// A standalone [`OptimizerConfig`] that can be used independently
/// of DataFusion's config management
#[derive(Debug)]
pub struct OptimizerContext {
    /// Query execution start time that can be used to rewrite
    /// expressions such as `now()` to use a literal value instead
    query_execution_start_time: DateTime<Utc>,

    /// Alias generator used to generate unique aliases for sub-queries
    alias_generator: Arc<AliasGenerator>,

    options: ConfigOptions,
}

impl OptimizerContext {
    /// Create optimizer config
    pub fn new() -> Self {
        let mut options = ConfigOptions::default();
        options.optimizer.filter_null_join_keys = true;

        Self {
            query_execution_start_time: Utc::now(),
            alias_generator: Arc::new(AliasGenerator::new()),
            options,
        }
    }

    /// Specify whether to enable the filter_null_keys rule
    pub fn filter_null_keys(mut self, filter_null_keys: bool) -> Self {
        self.options.optimizer.filter_null_join_keys = filter_null_keys;
        self
    }

    /// Specify the query execution start time for this plan
    pub fn with_query_execution_start_time(
        mut self,
        query_execution_start_time: DateTime<Utc>,
    ) -> Self {
        self.query_execution_start_time = query_execution_start_time;
        self
    }

    /// Specify whether the optimizer should skip rules that produce
    /// errors, or fail the query
    pub fn with_skip_failing_rules(mut self, b: bool) -> Self {
        self.options.optimizer.skip_failed_rules = b;
        self
    }

    /// Specify how many times to attempt to optimize the plan
    pub fn with_max_passes(mut self, v: u8) -> Self {
        self.options.optimizer.max_passes = v as usize;
        self
    }
}

impl Default for OptimizerContext {
    /// Create optimizer config
    fn default() -> Self {
        Self::new()
    }
}

impl OptimizerConfig for OptimizerContext {
    fn query_execution_start_time(&self) -> DateTime<Utc> {
        self.query_execution_start_time
    }

    fn alias_generator(&self) -> Arc<AliasGenerator> {
        self.alias_generator.clone()
    }

    fn options(&self) -> &ConfigOptions {
        &self.options
    }
}

/// A rule-based optimizer.
#[derive(Clone)]
pub struct Optimizer {
    /// All optimizer rules to apply
    pub rules: Vec<Arc<dyn OptimizerRule + Send + Sync>>,
}

impl Default for Optimizer {
    fn default() -> Self {
        Self::new()
    }
}

impl Optimizer {
    /// Create a new optimizer using the recommended list of rules
    pub fn new() -> Self {
        Self::with_rules(vec![])
    }

    /// Create a new optimizer with the given rules
    pub fn with_rules(rules: Vec<Arc<dyn OptimizerRule + Send + Sync>>) -> Self {
        Self { rules }
    }

    /// Optimizes the logical plan by applying optimizer rules, and
    /// invoking observer function after each call
    pub fn optimize<F>(
        &self,
        plan: &LogicalPlan,
        config: &dyn OptimizerConfig,
        mut observer: F,
    ) -> Result<LogicalPlan>
    where
        F: FnMut(&LogicalPlan, &dyn OptimizerRule),
    {
        println!("datafusion-optd-optimizer currently does nothing.");
        Ok(plan.clone())
    }
}
