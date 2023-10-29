use core::fmt;
use std::fmt::Display;

use num_derive::FromPrimitive;
use num_traits::FromPrimitive;
use pretty_xmlish::Pretty;

use crate::rel_node::{RelNode, Value};

use super::{Expr, OptRelNode, OptRelNodeRef, OptRelNodeTyp, PlanNode};

#[derive(FromPrimitive, Debug)]
pub enum JoinType {
    Inner = 1,
    FullOuter,
    LeftOuter,
    RightOuter,
}

impl Display for JoinType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Clone, Debug)]
pub struct LogicalJoin(pub PlanNode);

impl OptRelNode for LogicalJoin {
    fn into_rel_node(self) -> OptRelNodeRef {
        self.0.into_rel_node()
    }

    fn from_rel_node(rel_node: OptRelNodeRef) -> Option<Self> {
        if rel_node.typ != OptRelNodeTyp::Join {
            return None;
        }
        PlanNode::from_rel_node(rel_node).map(Self)
    }

    fn dispatch_explain(&self) -> Pretty<'static> {
        Pretty::simple_record(
            "LogicalJoin",
            vec![
                ("typ", self.join_type().to_string().into()),
                ("cond", self.cond().explain()),
            ],
            vec![self.left_child().explain(), self.right_child().explain()],
        )
    }
}

impl LogicalJoin {
    pub fn new(left: PlanNode, right: PlanNode, cond: Expr, join_type: JoinType) -> LogicalJoin {
        LogicalJoin(PlanNode(
            RelNode {
                typ: OptRelNodeTyp::Join,
                children: vec![
                    left.into_rel_node(),
                    right.into_rel_node(),
                    cond.into_rel_node(),
                ],
                data: Some(Value::Int(join_type as i64)),
                is_logical: true,
            }
            .into(),
        ))
    }

    pub fn left_child(&self) -> PlanNode {
        PlanNode::from_rel_node(self.clone().into_rel_node().children[0].clone()).unwrap()
    }

    pub fn right_child(&self) -> PlanNode {
        PlanNode::from_rel_node(self.clone().into_rel_node().children[1].clone()).unwrap()
    }

    pub fn cond(&self) -> Expr {
        Expr::from_rel_node(self.clone().into_rel_node().children[2].clone()).unwrap()
    }

    pub fn join_type(&self) -> JoinType {
        JoinType::from_i64(self.0 .0.data.as_ref().unwrap().as_i64()).unwrap()
    }
}
