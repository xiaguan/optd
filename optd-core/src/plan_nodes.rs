//! Typed interface of plan nodes.

use crate::rel_node::{RelNode, RelNodeRef, RelNodeTyp, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OptRelNodeTyp {
    // Plan nodes
    Join,
    // Expressions
    Constant,
    ColumnRef,
    // Enums
    JoinTypeInner,
}

impl std::fmt::Display for OptRelNodeTyp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl RelNodeTyp for OptRelNodeTyp {}

trait IntoRelNode<T: RelNodeTyp> {
    fn into_rel_node(self) -> RelNodeRef<T>;
}

pub struct PlanNode(RelNodeRef<OptRelNodeTyp>);

impl IntoRelNode<OptRelNodeTyp> for PlanNode {
    fn into_rel_node(self) -> RelNodeRef<OptRelNodeTyp> {
        self.0
    }
}

pub struct Expr(RelNodeRef<OptRelNodeTyp>);

impl IntoRelNode<OptRelNodeTyp> for Expr {
    fn into_rel_node(self) -> RelNodeRef<OptRelNodeTyp> {
        self.0
    }
}

pub struct Property(RelNodeRef<OptRelNodeTyp>);

impl IntoRelNode<OptRelNodeTyp> for Property {
    fn into_rel_node(self) -> RelNodeRef<OptRelNodeTyp> {
        self.0
    }
}

pub struct LogicalJoin(PlanNode);

impl IntoRelNode<OptRelNodeTyp> for LogicalJoin {
    fn into_rel_node(self) -> RelNodeRef<OptRelNodeTyp> {
        self.0.into_rel_node()
    }
}

pub struct ConstantExpr(Expr);

impl IntoRelNode<OptRelNodeTyp> for ConstantExpr {
    fn into_rel_node(self) -> RelNodeRef<OptRelNodeTyp> {
        self.0.into_rel_node()
    }
}

pub struct ColumnRefExpr(Expr);

impl IntoRelNode<OptRelNodeTyp> for ColumnRefExpr {
    fn into_rel_node(self) -> RelNodeRef<OptRelNodeTyp> {
        self.0.into_rel_node()
    }
}

pub struct JoinType(Property);

impl IntoRelNode<OptRelNodeTyp> for JoinType {
    fn into_rel_node(self) -> RelNodeRef<OptRelNodeTyp> {
        self.0.into_rel_node()
    }
}

pub fn constant(value: Value) -> ConstantExpr {
    ConstantExpr(Expr(
        RelNode {
            typ: OptRelNodeTyp::Constant,
            children: vec![],
            data: Some(Box::new(value)),
        }
        .into(),
    ))
}

pub fn column_ref(column_idx: usize) -> ConstantExpr {
    ConstantExpr(Expr(
        RelNode {
            typ: OptRelNodeTyp::ColumnRef,
            children: vec![],
            data: Some(Box::new(Value::Int(column_idx as i64))),
        }
        .into(),
    ))
}

pub fn logical_join(
    left: PlanNode,
    right: PlanNode,
    cond: Expr,
    join_type: JoinType,
) -> LogicalJoin {
    LogicalJoin(PlanNode(
        RelNode {
            typ: OptRelNodeTyp::Join,
            children: vec![left.0, right.0, cond.0, join_type.0 .0],
            data: None,
        }
        .into(),
    ))
}
