use crate::{
    plan_nodes::{OptRelNodeRef, OptRelNodeTyp},
    rel_node::{RelNode, Value},
};

use super::Rule;

pub struct JoinCommuteRule {}

impl Rule<OptRelNodeTyp> for JoinCommuteRule {
    fn matches(&self, typ: OptRelNodeTyp, data: Option<Value>) -> bool {
        return typ == OptRelNodeTyp::Join;
    }

    fn apply(&self, input: OptRelNodeRef) -> Vec<OptRelNodeRef> {
        if input.typ != OptRelNodeTyp::Join {
            unreachable!()
        }
        let new_node = RelNode::<OptRelNodeTyp> {
            typ: OptRelNodeTyp::Join,
            children: vec![input.children[1].clone(), input.children[0].clone()],
            data: input.data.clone(), // TODO: inner join
        };
        return vec![new_node.into()];
    }

    fn name(&self) -> &'static str {
        "join_commute"
    }
}
