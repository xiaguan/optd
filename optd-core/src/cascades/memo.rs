mod union_find;

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

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

#[derive(Default)]
struct Group {
    group_exprs: HashSet<GroupExprId>,
}

pub struct Memo<T: RelNodeTyp> {
    group_exprs: HashMap<GroupExprId, (GroupId, RelMemoNodeRef<T>)>,
    rev_group_exprs: HashMap<RelMemoNode<T>, (GroupId, GroupExprId)>,
    groups: HashMap<GroupId, Group>,
    next_group_id: GroupId,
    next_group_expr_id: GroupId,
}

impl<T: RelNodeTyp> Memo<T> {
    pub fn new() -> Self {
        Self {
            group_exprs: HashMap::new(),
            rev_group_exprs: HashMap::new(),
            groups: HashMap::new(),
            next_group_id: 0,
            next_group_expr_id: 0,
        }
    }

    fn next_group_id(&mut self) -> GroupId {
        let id = self.next_group_id;
        self.next_group_id += 1;
        id
    }

    fn next_group_expr_id(&mut self) -> GroupId {
        let id = self.next_group_expr_id;
        self.next_group_expr_id += 1;
        id
    }

    fn find_existing_group_expr(
        &self,
        memo_node: &RelMemoNode<T>,
    ) -> Option<(GroupId, GroupExprId)> {
        self.rev_group_exprs.get(memo_node).copied()
    }

    /// Add a group into the memo. SAFETY: should have checked memo_node using `find_existing_group_expr`.
    fn add_new_group_expr(
        &mut self,
        memo_node: RelMemoNode<T>,
        group_id: Option<GroupId>,
    ) -> (GroupId, GroupExprId) {
        let expr_id = self.next_group_expr_id();
        trace!(name: "adding node to group", group_id = group_id, node = %memo_node);
        if let Some(group_id) = group_id {
            self.group_exprs
                .insert(expr_id, (group_id, Arc::new(memo_node.clone())));
            self.groups
                .entry(group_id)
                .or_insert_with(Group::default)
                .group_exprs
                .insert(expr_id);
            self.rev_group_exprs.insert(memo_node, (group_id, expr_id));
            (group_id, expr_id)
        } else {
            let group_id = self.next_group_id();
            self.group_exprs
                .insert(expr_id, (group_id, Arc::new(memo_node.clone())));
            self.groups
                .entry(group_id)
                .or_insert_with(Group::default)
                .group_exprs
                .insert(expr_id);
            self.rev_group_exprs.insert(memo_node, (group_id, expr_id));
            (group_id, expr_id)
        }
    }

    /// Add a group into the memo. If the group already exists, return the existing group id.
    pub fn get_or_add_group_expr(
        &mut self,
        root_rel: RelNodeRef<T>,
        new_group_id: Option<GroupId>,
    ) -> (GroupId, GroupExprId) {
        let children_group_ids = root_rel
            .children
            .iter()
            .map(|child| self.get_or_add_group_expr(child.clone(), None).0)
            .collect::<Vec<_>>();
        let memo_node = RelMemoNode {
            typ: root_rel.typ,
            children: children_group_ids,
            data: root_rel.data.clone(),
        };
        if let Some((group_id, expr_id)) = self.find_existing_group_expr(&memo_node) {
            if let Some(new_group_id) = new_group_id {
                if new_group_id != group_id {
                    panic!("not supported yet :(");
                }
            }
            return (group_id, expr_id);
        }
        self.add_new_group_expr(memo_node, new_group_id)
    }

    pub fn get_group_id(&self, group_expr_id: GroupExprId) -> GroupId {
        self.group_exprs[&group_expr_id].0
    }

    pub fn get_group_expr_memo(&self, group_expr_id: GroupExprId) -> RelMemoNodeRef<T> {
        self.group_exprs[&group_expr_id].1.clone()
    }

    pub fn get_group_expr(&self, group_expr_id: GroupExprId) -> Vec<RelNodeRef<T>> {
        let (_, expr) = self.group_exprs[&group_expr_id].clone();
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
            let node = Arc::new(RelNode {
                typ: expr.typ,
                children: selected_nodes,
                data: expr.data.clone(),
            });
            result.push(node);
        }
        result
    }

    pub fn get_group_exprs(&self, group_id: GroupId) -> Vec<GroupExprId> {
        if let Some(group) = self.groups.get(&group_id) {
            group.group_exprs.iter().copied().collect()
        } else {
            return vec![];
        }
    }
}
