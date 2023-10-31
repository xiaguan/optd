use std::sync::Arc;

use crate::rel_node::RelNodeTyp;

pub enum RuleMatcher<T: RelNodeTyp> {
    /// Match a node of type `typ`.
    MatchAndPickNode {
        typ: T,
        children: Vec<Self>,
        pick_to: usize,
    },
    /// Match a node of type `typ`.
    MatchNode { typ: T, children: Vec<Self> },
    /// Match anything,
    PickOne { pick_to: usize },
    /// Match all things in the group
    PickMany { pick_to: usize },
    /// Ignore one
    IgnoreOne,
    /// Ignore many
    IgnoreMany,
}

#[derive(Debug, Clone)]
pub enum OneOrMany<T> {
    One(T),
    Many(Arc<Vec<T>>),
}

impl<T> OneOrMany<T> {
    pub fn as_one(self) -> T {
        match self {
            Self::One(x) => x,
            _ => panic!(),
        }
    }
    pub fn as_many(self) -> Arc<Vec<T>> {
        match self {
            Self::Many(x) => x,
            _ => panic!(),
        }
    }
}
