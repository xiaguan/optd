use super::Memo;

pub struct CascadesOptimizer {
    memo: Memo,
}

pub type GroupId = usize;
pub type GroupExprId = usize;

impl CascadesOptimizer {
    pub(super) fn get_group_exprs(&self, group_id: GroupId) -> Vec<GroupExprId> {
        vec![]
    }
}
