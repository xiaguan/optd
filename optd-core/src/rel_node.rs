//! The RelNode is the basic data structure of the optimizer. It is dynamically typed and is
//! the internal representation of the plan nodes.

use std::{
    fmt::{Debug, Display},
    hash::Hash,
    sync::Arc,
};

pub type RelNodeRef<T> = Arc<RelNode<T>>;

pub trait RelNodeTyp: PartialEq + Eq + Hash + Clone + Copy + 'static + Display + Debug {}

#[derive(Clone, Debug)]
pub enum Value {
    Int(i64),
    Float(f64),
    String(String),
    Bool(bool),
    Any(Arc<dyn std::any::Any>),
}

impl Value {
    pub fn as_i64(&self) -> i64 {
        match self {
            Value::Int(i) => *i,
            _ => panic!("Value is not an i64"),
        }
    }
}

/// A RelNode is consisted of a plan node type and some children.
#[derive(Clone, Debug)]
pub struct RelNode<T: RelNodeTyp> {
    pub typ: T,
    pub children: Vec<RelNodeRef<T>>,
    pub data: Option<Value>,
}
