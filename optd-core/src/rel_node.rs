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
}

/// A RelNode is consisted of a plan node type and some children.
#[derive(Clone, Debug)]
pub struct RelNode<T: RelNodeTyp> {
    pub typ: T,
    pub children: Vec<RelNodeRef<T>>,
    pub data: Option<Box<Value>>,
}
