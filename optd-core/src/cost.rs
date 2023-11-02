use std::collections::HashMap;

use itertools::Itertools;

use crate::{
    plan_nodes::OptRelNodeTyp,
    rel_node::{RelNode, RelNodeTyp, Value},
};

fn compute_plan_node_cost<T: RelNodeTyp, C: CostModel<T>>(
    cost: &C,
    node: &RelNode<T>,
    total_cost: &mut f64,
) -> f64 {
    let children = node
        .children
        .iter()
        .map(|child| compute_plan_node_cost(cost, child, total_cost))
        .collect_vec();
    let (cost, row_cnt) = cost.compute_cost(&node.typ, &node.data, children);
    *total_cost += cost;
    row_cnt
}

pub trait CostModel<T: RelNodeTyp>: 'static + Send + Sync {
    fn compute_cost(&self, node: &T, data: &Option<Value>, children: Vec<f64>) -> (f64, f64);

    fn compute_plan_node_cost(&self, node: &RelNode<T>) -> f64;
}

pub struct OptCostModel {
    table_stat: HashMap<String, usize>,
}

impl CostModel<OptRelNodeTyp> for OptCostModel {
    fn compute_cost(
        &self,
        node: &OptRelNodeTyp,
        data: &Option<Value>,
        children: Vec<f64>,
    ) -> (f64, f64) {
        match node {
            OptRelNodeTyp::PhysicalScan => {
                let table_name = data.as_ref().unwrap().as_str();
                let row_cnt = self.table_stat.get(table_name.as_ref()).copied().unwrap();
                (row_cnt as f64, row_cnt as f64)
            }
            OptRelNodeTyp::PhysicalFilter => (children[0], children[0] * 0.01),
            OptRelNodeTyp::PhysicalNestedLoopJoin(_) => {
                (children[0] * children[1], children[0] * children[1] * 0.1)
            }
            _ => (1.0, 1.0),
        }
    }

    fn compute_plan_node_cost(&self, node: &RelNode<OptRelNodeTyp>) -> f64 {
        let mut cost = 0.0;
        compute_plan_node_cost(self, node, &mut cost);
        cost
    }
}

impl OptCostModel {
    pub fn new(table_stat: HashMap<String, usize>) -> Self {
        Self { table_stat }
    }
}
