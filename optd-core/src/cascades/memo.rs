use std::{collections::HashMap, sync::Arc};

use tracing::trace;

use crate::rel_node::{RelNodeRef, RelNodeTyp, Value};

use super::optimizer::GroupId;

pub type RelMemoNodeRef<T> = Arc<RelMemoNode<T>>;

/// Equivalent to MExpr in Columbia/Cascades.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct RelMemoNode<T: RelNodeTyp> {
    pub typ: T,
    pub children: Vec<GroupId>,
    pub data: Option<Value>,
}

impl<T: RelNodeTyp> std::fmt::Display for RelMemoNode<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "({}", self.typ)?;
        match self.data {
            Some(ref data) => write!(f, " {}", data)?,
            None => {}
        }
        for child in &self.children {
            write!(f, " !{}", child)?;
        }
        write!(f, ")")
    }
}

pub struct Memo<T: RelNodeTyp> {
    groups: HashMap<GroupId, RelMemoNodeRef<T>>,
    rev_groups: HashMap<RelMemoNode<T>, GroupId>,
    next_group_id: GroupId,
}

impl<T: RelNodeTyp> Memo<T> {
    pub fn new() -> Self {
        Self {
            groups: HashMap::new(),
            rev_groups: HashMap::new(),
            next_group_id: 0,
        }
    }

    fn next_group_id(&mut self) -> GroupId {
        let id = self.next_group_id;
        self.next_group_id += 1;
        id
    }

    fn find_existing_group(&self, memo_node: &RelMemoNode<T>) -> Option<GroupId> {
        self.rev_groups.get(memo_node).copied()
    }

    /// Add a group into the memo. SAFETY: should have checked memo_node using `find_existing_group`.
    fn add_new_group(&mut self, memo_node: RelMemoNode<T>) -> GroupId {
        let group_id = self.next_group_id();
        trace!(name: "new_group", group_id = group_id, node = %memo_node);
        self.groups.insert(group_id, Arc::new(memo_node.clone()));
        self.rev_groups.insert(memo_node, group_id);
        group_id
    }

    /// Add a group into the memo. If the group already exists, return the existing group id.
    pub fn get_or_add_group(&mut self, root_rel: RelNodeRef<T>) -> GroupId {
        let children_group_ids = root_rel
            .children
            .iter()
            .map(|child| self.get_or_add_group(child.clone()))
            .collect();
        let group_id = self.next_group_id();
        let memo_node = RelMemoNode {
            typ: root_rel.typ,
            children: children_group_ids,
            data: root_rel.data.clone(),
        };
        if let Some(group_id) = self.find_existing_group(&memo_node) {
            return group_id;
        }
        self.add_new_group(memo_node)
    }
}
