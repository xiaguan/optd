use std::collections::HashMap;

use crate::plan_nodes::{JoinType, OptRelNodeTyp};

use super::{
    ir::{OneOrMany, RuleMatcher},
    RelRuleNode, Rule,
};

pub struct FilterJoinPullUpRule {
    matcher: RuleMatcher<OptRelNodeTyp>,
}

const LEFT_CHILD: usize = 0;
const RIGHT_CHILD: usize = 1;
const JOIN_COND: usize = 2;
const FILTER_COND: usize = 3;

impl FilterJoinPullUpRule {
    pub fn new() -> Self {
        Self {
            matcher: RuleMatcher::MatchNode {
                typ: OptRelNodeTyp::Join(JoinType::Inner),
                children: vec![
                    RuleMatcher::MatchNode {
                        typ: OptRelNodeTyp::Filter,
                        children: vec![
                            RuleMatcher::PickOne {
                                pick_to: LEFT_CHILD,
                            },
                            RuleMatcher::PickOne {
                                pick_to: FILTER_COND,
                            },
                        ],
                    },
                    RuleMatcher::PickOne {
                        pick_to: RIGHT_CHILD,
                    },
                    RuleMatcher::PickOne { pick_to: JOIN_COND },
                ],
            },
        }
    }
}

impl Rule<OptRelNodeTyp> for FilterJoinPullUpRule {
    fn matcher(&self) -> &RuleMatcher<OptRelNodeTyp> {
        &self.matcher
    }

    fn apply(
        &self,
        mut input: HashMap<usize, OneOrMany<RelRuleNode<OptRelNodeTyp>>>,
    ) -> Vec<RelRuleNode<OptRelNodeTyp>> {
        let left_child = input.remove(&LEFT_CHILD).unwrap().as_one();
        let right_child: RelRuleNode<OptRelNodeTyp> = input.remove(&RIGHT_CHILD).unwrap().as_one();
        let join_cond = input.remove(&JOIN_COND).unwrap().as_one();
        let filter_cond = input.remove(&FILTER_COND).unwrap().as_one();
        let join_node = RelRuleNode::Node {
            typ: OptRelNodeTyp::Join(JoinType::Inner),
            children: vec![left_child, right_child, join_cond],
            data: None,
        };
        let filter_node = RelRuleNode::Node {
            typ: OptRelNodeTyp::Filter,
            children: vec![join_node, filter_cond],
            data: None,
        };
        vec![filter_node]
    }

    fn name(&self) -> &'static str {
        "join_commute"
    }
}
