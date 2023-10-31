use std::collections::HashMap;

use crate::plan_nodes::OptRelNodeTyp;

use super::{
    ir::{OneOrMany, RuleMatcher},
    RelRuleNode, Rule,
};

pub struct PhysicalConversionRule {
    matcher: RuleMatcher<OptRelNodeTyp>,
}

impl PhysicalConversionRule {
    pub fn new(logical_typ: OptRelNodeTyp) -> Self {
        Self {
            matcher: RuleMatcher::MatchAndPickNode {
                typ: logical_typ,
                pick_to: 0,
                children: vec![RuleMatcher::IgnoreMany],
            },
        }
    }
}

impl Rule<OptRelNodeTyp> for PhysicalConversionRule {
    fn matcher(&self) -> &RuleMatcher<OptRelNodeTyp> {
        &self.matcher
    }

    fn apply(
        &self,
        mut input: HashMap<usize, OneOrMany<RelRuleNode<OptRelNodeTyp>>>,
    ) -> Vec<RelRuleNode<OptRelNodeTyp>> {
        let RelRuleNode::Node {
            typ,
            data,
            children,
        } = input.remove(&0).unwrap().as_one()
        else {
            unimplemented!()
        };

        match typ {
            OptRelNodeTyp::Apply(x) => {
                let node = RelRuleNode::Node {
                    typ: OptRelNodeTyp::PhysicalNestedLoopJoin(x.to_join_type()),
                    children,
                    data,
                };
                vec![node]
            }
            OptRelNodeTyp::Join(x) => {
                let node = RelRuleNode::Node {
                    typ: OptRelNodeTyp::PhysicalNestedLoopJoin(x),
                    children,
                    data,
                };
                vec![node]
            }
            OptRelNodeTyp::Scan => {
                let node = RelRuleNode::Node {
                    typ: OptRelNodeTyp::Scan,
                    children,
                    data,
                };
                vec![node]
            }
            OptRelNodeTyp::Filter => {
                let node = RelRuleNode::Node {
                    typ: OptRelNodeTyp::Filter,
                    children,
                    data,
                };
                vec![node]
            }
            _ => vec![],
        }
    }

    fn is_impl_rule(&self) -> bool {
        true
    }

    fn name(&self) -> &'static str {
        "physical_conversion"
    }
}
