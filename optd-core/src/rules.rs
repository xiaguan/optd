mod ir;
mod join_assoc;
mod join_commute;
mod physical;

use std::collections::HashMap;

use crate::{
    cascades::GroupId,
    rel_node::{RelNodeTyp, Value},
};

pub use ir::{OneOrMany, RuleMatcher};
pub use join_assoc::{JoinAssocLeftRule, JoinAssocRightRule};
pub use join_commute::JoinCommuteRule;
pub use physical::PhysicalConversionRule;

pub trait Rule<T: RelNodeTyp> {
    fn matcher(&self) -> &RuleMatcher<T>;
    fn apply(&self, input: HashMap<usize, OneOrMany<RelRuleNode<T>>>) -> Vec<RelRuleNode<T>>;
    fn name(&self) -> &'static str;
    fn is_impl_rule(&self) -> bool {
        false
    }
}

/// The rel node type when implementing a rule.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum RelRuleNode<T: RelNodeTyp> {
    Node {
        typ: T,
        children: Vec<Self>,
        data: Option<Value>,
    },
    Group(GroupId),
}
