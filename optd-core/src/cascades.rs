//! The core cascades optimizer implementation.

mod memo;
mod optimizer;
mod tasks;

use memo::Memo;
pub use optimizer::{CascadesOptimizer, GroupId, OptimizerProperties, RelNodeContext};
use tasks::Task;
