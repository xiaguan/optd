use std::collections::HashMap;

use itertools::Itertools;

use crate::{
    plan_nodes::{JoinType, LogicalJoin, OptRelNode, OptRelNodeRef, OptRelNodeTyp},
    rel_node::Value,
};

use super::{
    ir::{OneOrMany, RuleMatcher},
    RelRuleNode, Rule,
};

/// Implements A join (B join C) = (A join B) join C
pub struct JoinAssocLeftRule {
    matcher: RuleMatcher<OptRelNodeTyp>,
}

/// Implements (A join B) join C = A join (B join C)
pub struct JoinAssocRightRule {
    matcher: RuleMatcher<OptRelNodeTyp>,
}

const CHILD_A: usize = 0;
const CHILD_B: usize = 1;
const CHILD_C: usize = 2;
const COND_AB: usize = 3;
const COND_BC: usize = 4;
const JOIN_NODE_TOP: usize = 5;
const JOIN_NODE_DOWN: usize = 6;

impl JoinAssocLeftRule {
    pub fn new() -> Self {
        Self {
            matcher: RuleMatcher::MatchAndPickNode {
                typ: OptRelNodeTyp::Join(JoinType::Inner),
                pick_to: JOIN_NODE_TOP,
                children: vec![
                    RuleMatcher::PickOne { pick_to: CHILD_A },
                    RuleMatcher::MatchAndPickNode {
                        typ: OptRelNodeTyp::Join(JoinType::Inner),
                        children: vec![
                            RuleMatcher::PickOne { pick_to: CHILD_B },
                            RuleMatcher::PickOne { pick_to: CHILD_C },
                            RuleMatcher::PickOne { pick_to: COND_BC },
                        ],
                        pick_to: JOIN_NODE_DOWN,
                    },
                    RuleMatcher::PickOne { pick_to: COND_AB },
                ],
            },
        }
    }
}

impl JoinAssocRightRule {
    pub fn new() -> Self {
        Self {
            matcher: RuleMatcher::MatchAndPickNode {
                typ: OptRelNodeTyp::Join(JoinType::Inner),
                pick_to: JOIN_NODE_TOP,
                children: vec![
                    RuleMatcher::MatchAndPickNode {
                        typ: OptRelNodeTyp::Join(JoinType::Inner),
                        children: vec![
                            RuleMatcher::PickOne { pick_to: CHILD_A },
                            RuleMatcher::PickOne { pick_to: CHILD_B },
                            RuleMatcher::PickOne { pick_to: COND_AB },
                        ],
                        pick_to: JOIN_NODE_DOWN,
                    },
                    RuleMatcher::PickOne { pick_to: CHILD_C },
                    RuleMatcher::PickOne { pick_to: COND_BC },
                ],
            },
        }
    }
}

impl Rule<OptRelNodeTyp> for JoinAssocLeftRule {
    fn matcher(&self) -> &RuleMatcher<OptRelNodeTyp> {
        &self.matcher
    }

    fn apply(
        &self,
        mut input: HashMap<usize, OneOrMany<RelRuleNode<OptRelNodeTyp>>>,
    ) -> Vec<RelRuleNode<OptRelNodeTyp>> {
        let RelRuleNode::Node {
            typ: top_typ,
            data: top_data,
            ..
        } = input.remove(&JOIN_NODE_TOP).unwrap().as_one()
        else {
            unreachable!()
        };
        let RelRuleNode::Node {
            typ: down_typ,
            data: down_data,
            ..
        } = input.remove(&JOIN_NODE_DOWN).unwrap().as_one()
        else {
            unreachable!()
        };
        let child_a = input.remove(&CHILD_A).unwrap().as_one();
        let child_b = input.remove(&CHILD_B).unwrap().as_one();
        let child_c = input.remove(&CHILD_C).unwrap().as_one();
        let cond_ab = input.remove(&COND_AB).unwrap().as_one();
        let cond_bc = input.remove(&COND_BC).unwrap().as_one();
        let node = RelRuleNode::Node {
            typ: top_typ,
            children: vec![
                RelRuleNode::Node {
                    typ: down_typ,
                    children: vec![child_a, child_b, cond_ab],
                    data: down_data,
                },
                child_c,
                cond_bc,
            ],
            data: top_data,
        };
        vec![node]
    }

    fn name(&self) -> &'static str {
        "join_assoc_rotate_left"
    }
}

impl Rule<OptRelNodeTyp> for JoinAssocRightRule {
    fn matcher(&self) -> &RuleMatcher<OptRelNodeTyp> {
        &self.matcher
    }

    fn apply(
        &self,
        mut input: HashMap<usize, OneOrMany<RelRuleNode<OptRelNodeTyp>>>,
    ) -> Vec<RelRuleNode<OptRelNodeTyp>> {
        let RelRuleNode::Node {
            typ: top_typ,
            data: top_data,
            ..
        } = input.remove(&JOIN_NODE_TOP).unwrap().as_one()
        else {
            unreachable!()
        };
        let RelRuleNode::Node {
            typ: down_typ,
            data: down_data,
            ..
        } = input.remove(&JOIN_NODE_DOWN).unwrap().as_one()
        else {
            unreachable!()
        };
        let child_a = input.remove(&CHILD_A).unwrap().as_one();
        let child_b = input.remove(&CHILD_B).unwrap().as_one();
        let child_c = input.remove(&CHILD_C).unwrap().as_one();
        let cond_ab = input.remove(&COND_AB).unwrap().as_one();
        let cond_bc = input.remove(&COND_BC).unwrap().as_one();
        let node = RelRuleNode::Node {
            typ: top_typ,
            children: vec![
                child_a,
                RelRuleNode::Node {
                    typ: down_typ,
                    children: vec![child_b, child_c, cond_bc],
                    data: down_data,
                },
                cond_ab,
            ],
            data: top_data,
        };
        vec![node]
    }

    fn name(&self) -> &'static str {
        "join_assoc_rotate_right"
    }
}
