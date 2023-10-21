use std::collections::VecDeque;

use anyhow::Result;

use crate::rel_node::{RelNodeRef, RelNodeTyp};

use super::{tasks::OptimizeGroupTask, Memo, Task};

pub struct CascadesOptimizer<T: RelNodeTyp> {
    memo: Memo<T>,
    tasks: VecDeque<Box<dyn Task<T>>>,
}

pub type GroupId = usize;
pub type GroupExprId = usize;

impl<T: RelNodeTyp> CascadesOptimizer<T> {
    pub fn new() -> Self {
        let tasks = VecDeque::new();
        let memo = Memo::new();
        Self { memo, tasks }
    }

    pub fn optimize(&mut self, root_rel: RelNodeRef<T>) -> Result<()> {
        let group_id = self.memo.get_or_add_group(root_rel);
        self.tasks
            .push_back(Box::new(OptimizeGroupTask::new(group_id)));
        while let Some(task) = self.tasks.pop_front() {
            let new_tasks = task.execute(self)?;
            self.tasks.extend(new_tasks);
        }
        Ok(())
    }

    pub(super) fn get_group_exprs(&self, group_id: GroupId) -> Vec<GroupExprId> {
        vec![]
    }
}
