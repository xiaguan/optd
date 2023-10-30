use std::sync::Arc;

use optd_core::{
    cascades::CascadesOptimizer,
    plan_nodes::{
        explain, ConstantExpr, JoinType, LogicalFilter, LogicalJoin, LogicalScan, OptRelNode,
        PlanNode,
    },
    rel_node::Value,
    rules::{FilterJoinRule, JoinCommuteRule},
};
use pretty_xmlish::PrettyConfig;
use tracing::Level;

pub fn main() {
    tracing_subscriber::fmt()
        .with_max_level(Level::TRACE)
        .with_target(false)
        .init();

    let mut optimizer = CascadesOptimizer::new_with_rules(vec![
        Arc::new(JoinCommuteRule {}),
        Arc::new(FilterJoinRule {}),
    ]);
    let scan1 = LogicalScan::new("t1".into());
    let filter_cond = ConstantExpr::new(Value::Bool(true));
    let filter1 = LogicalFilter::new(scan1.0, filter_cond.0);
    let scan2 = LogicalScan::new("t2".into());
    let join_cond = ConstantExpr::new(Value::Bool(true));
    let scan3 = LogicalScan::new("t3".into());
    let join_filter = LogicalJoin::new(filter1.0, scan2.0, join_cond.clone().0, JoinType::Inner);
    // let fnal = LogicalJoin::new(scan3.0, join_filter.0, join_cond.0, JoinType::Inner);
    let result = optimizer.optimize(join_filter.0.into_rel_node()).unwrap();
    let mut config = PrettyConfig::default();
    config.need_boundaries = false;
    config.reduced_spaces = true;
    for node in result {
        let pretty = explain(node);
        let mut out = String::new();
        config.unicode(&mut out, &pretty);
        println!("{}", out);
    }
}
