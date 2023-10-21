mod union_find;

use std::{collections::HashMap, sync::Arc};

use tracing::trace;

use crate::rel_node::{RelNode, RelNodeRef, RelNodeTyp, Value};

use super::optimizer::{GroupExprId, GroupId};

pub type RelMemoNodeRef<T> = Arc<RelMemoNode<T>>;

/// Equivalent to MExpr in Columbia/Cascades.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct RelMemoNode<T: RelNodeTyp> {
    pub typ: T,
    pub children: Vec<GroupExprId>,
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

struct Group<T: RelNodeTyp> {
    group_exprs: HashMap<GroupExprId, RelMemoNodeRef<T>>,
}

pub struct Memo<T: RelNodeTyp> {
    group_exprs: HashMap<GroupExprId, RelMemoNodeRef<T>>,
    rev_group_exprs: HashMap<RelMemoNode<T>, GroupExprId>,
    groups: HashMap<GroupId, Group<T>>,
    next_group_id: GroupId,
}

impl<T: RelNodeTyp> Memo<T> {
    pub fn new() -> Self {
        Self {
            group_exprs: HashMap::new(),
            rev_group_exprs: HashMap::new(),
            groups: HashMap::new(),
            next_group_id: 0,
        }
    }

    fn next_group_id(&mut self) -> GroupId {
        let id = self.next_group_id;
        self.next_group_id += 1;
        id
    }

    fn find_existing_group_expr(&self, memo_node: &RelMemoNode<T>) -> Option<GroupExprId> {
        self.rev_group_exprs.get(memo_node).copied()
    }

    /// Add a group into the memo. SAFETY: should have checked memo_node using `find_existing_group_expr`.
    fn add_new_group_expr(&mut self, memo_node: RelMemoNode<T>) -> GroupExprId {
        let group_id = self.next_group_id();
        trace!(name: "new_group", group_id = group_id, node = %memo_node);
        self.group_exprs
            .insert(group_id, Arc::new(memo_node.clone()));
        self.rev_group_exprs.insert(memo_node, group_id);
        group_id
    }

    /// Add a group into the memo. If the group already exists, return the existing group id.
    pub fn get_or_add_group_expr(&mut self, root_rel: RelNodeRef<T>) -> GroupExprId {
        let children_group_ids = root_rel
            .children
            .iter()
            .map(|child| self.get_or_add_group_expr(child.clone()))
            .collect();
        let group_id = self.next_group_id();
        let memo_node = RelMemoNode {
            typ: root_rel.typ,
            children: children_group_ids,
            data: root_rel.data.clone(),
        };
        if let Some(group_id) = self.find_existing_group_expr(&memo_node) {
            return group_id;
        }
        self.add_new_group_expr(memo_node)
    }

    /// Add a group into the memo. If the group already exists, return the existing group id.
    pub fn get_or_add_group_expr_memo(&mut self, memo_node: RelMemoNode<T>) -> GroupExprId {
        if let Some(group_id) = self.find_existing_group_expr(&memo_node) {
            return group_id;
        }
        self.add_new_group_expr(memo_node)
    }

    pub fn get_group_expr_memo(&self, group_expr_id: GroupExprId) -> RelMemoNodeRef<T> {
        self.group_exprs[&group_expr_id].clone()
    }

    pub fn get_group_expr(&self, group_expr_id: GroupExprId) -> Vec<RelNodeRef<T>> {
        let expr = self.group_exprs[&group_expr_id].clone();
        let mut children = vec![];
        let mut cumulative = 1;
        for child in &expr.children {
            let group_exprs = self.get_group_expr(*child);
            cumulative *= group_exprs.len();
            children.push(group_exprs);
        }
        let mut result = vec![];
        for i in 0..cumulative {
            let mut selected_nodes = vec![];
            let mut ii = i;
            for child in children.iter().rev() {
                let idx = ii % child.len();
                ii /= child.len();
                selected_nodes.push(child[idx].clone());
            }
            selected_nodes.reverse();
            result.push(Arc::new(RelNode {
                typ: expr.typ,
                children: selected_nodes,
                data: expr.data.clone(),
            }))
        }
        result
    }
}
