use crate::rel_node::{RelNode, Value};

use super::{OptRelNode, OptRelNodeRef, OptRelNodeTyp, PlanNode};

#[derive(Clone, Debug)]
pub struct LogicalScan(pub PlanNode);

impl OptRelNode for LogicalScan {
    fn into_rel_node(self) -> OptRelNodeRef {
        self.0.into_rel_node()
    }
    fn from_rel_node(rel_node: OptRelNodeRef) -> Option<Self> {
        if rel_node.typ != OptRelNodeTyp::Join {
            return None;
        }
        PlanNode::from_rel_node(rel_node).map(Self)
    }
}

impl LogicalScan {
    pub fn new(table: String) -> LogicalScan {
        LogicalScan(PlanNode(
            RelNode {
                typ: OptRelNodeTyp::Scan,
                children: vec![],
                data: Some(Value::String(table.into())),
            }
            .into(),
        ))
    }
}
