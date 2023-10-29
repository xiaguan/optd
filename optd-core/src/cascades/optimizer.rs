use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
};

use anyhow::Result;

use crate::{
    rel_node::{RelNodeRef, RelNodeTyp},
    rules::Rule,
};

use super::{memo::RelMemoNodeRef, tasks::OptimizeGroupTask, Memo, Task};

pub type RuleId = usize;

pub struct CascadesOptimizer<T: RelNodeTyp> {
    memo: Memo<T>,
    tasks: VecDeque<Box<dyn Task<T>>>,
    explored_group: HashSet<GroupId>,
    fired_rules: HashMap<GroupExprId, HashSet<RuleId>>,
    rules: Arc<[Arc<dyn Rule<T>>]>,
}

pub type GroupId = usize;
pub type GroupExprId = usize;

impl<T: RelNodeTyp> CascadesOptimizer<T> {
    pub fn new_with_rules(rules: Vec<Arc<dyn Rule<T>>>) -> Self {
        let tasks = VecDeque::new();
        let memo = Memo::new();
        Self {
            memo,
            tasks,
            explored_group: HashSet::new(),
            fired_rules: HashMap::new(),
            rules: rules.into(),
        }
    }

    pub fn new() -> Self {
        Self::new_with_rules(vec![])
    }

    pub(super) fn rules(&self) -> Arc<[Arc<dyn Rule<T>>]> {
        self.rules.clone()
    }

    pub fn optimize(&mut self, root_rel: RelNodeRef<T>) -> Result<Vec<RelNodeRef<T>>> {
        let (group_id, _) = self.memo.get_or_add_group_expr(root_rel, None);
        self.tasks
            .push_back(Box::new(OptimizeGroupTask::new(group_id)));
        // get the task from the stack
        while let Some(task) = self.tasks.pop_back() {
            let new_tasks = task.execute(self)?;
            self.tasks.extend(new_tasks);
        }
        let mut result = vec![];
        for expr in self.get_group_exprs(group_id) {
            result.extend(self.get_group_expr(expr));
        }
        Ok(result)
    }

    pub(super) fn get_group_exprs(&self, group_id: GroupId) -> Vec<GroupExprId> {
        self.memo.get_group_exprs(group_id)
    }

    pub(super) fn add_group_expr(
        &mut self,
        expr: RelNodeRef<T>,
        group_id: Option<GroupId>,
    ) -> (GroupId, GroupExprId) {
        self.memo.get_or_add_group_expr(expr, group_id)
    }

    pub(super) fn get_group_id(&self, expr_id: GroupExprId) -> GroupId {
        self.memo.get_group_id(expr_id)
    }

    pub(super) fn get_group_expr_memo(&self, expr_id: GroupExprId) -> RelMemoNodeRef<T> {
        self.memo.get_group_expr_memo(expr_id)
    }

    pub(super) fn get_group_expr(&self, expr_id: GroupExprId) -> Vec<RelNodeRef<T>> {
        self.memo.get_group_expr(expr_id)
    }

    pub(super) fn is_group_explored(&self, group_id: GroupId) -> bool {
        self.explored_group.contains(&group_id)
    }

    pub(super) fn mark_group_explored(&mut self, group_id: GroupId) {
        self.explored_group.insert(group_id);
    }

    pub(super) fn is_rule_fired(&self, group_expr_id: GroupExprId, rule_id: RuleId) -> bool {
        self.fired_rules
            .get(&group_expr_id)
            .map(|rules| rules.contains(&rule_id))
            .unwrap_or(false)
    }

    pub(super) fn mark_rule_fired(&mut self, group_expr_id: GroupExprId, rule_id: RuleId) {
        self.fired_rules
            .entry(group_expr_id)
            .or_insert_with(HashSet::new)
            .insert(rule_id);
    }
}
