use anyhow::Result;

use crate::rel_node::{RelNodeRef, RelNodeTyp};

pub trait Rule<T: RelNodeTyp> {
    fn apply(&self, input: RelNodeRef<T>) -> Result<Vec<RelNodeRef<T>>>;
}
