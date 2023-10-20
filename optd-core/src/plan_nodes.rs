//! Typed interface of plan nodes.

use crate::rel_node::{RelNode, RelNodeRef, RelNodeTyp, Value};

use num_derive::FromPrimitive;
use num_traits::FromPrimitive;

#[repr(usize)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OptRelNodeTyp {
    // Plan nodes
    // Developers: update `is_plan_node` function after adding new elements
    Projection,
    Filter,
    Join,
    // Expressions
    Constant,
    ColumnRef,
    UnOp,
    BinOp,
    Func,
}

#[derive(FromPrimitive)]
pub enum JoinType {
    Inner = 1,
    FullOuter,
    LeftOuter,
    RightOuter,
}

impl OptRelNodeTyp {
    pub fn is_plan_node(&self) -> bool {
        (OptRelNodeTyp::Projection as usize) <= (*self as usize)
            && (*self as usize) <= (OptRelNodeTyp::Join as usize)
    }

    pub fn is_expression(&self) -> bool {
        (OptRelNodeTyp::Constant as usize) < (*self as usize)
            && (*self as usize) <= (OptRelNodeTyp::Func as usize)
    }
}

impl std::fmt::Display for OptRelNodeTyp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl RelNodeTyp for OptRelNodeTyp {}

pub type OptRelNodeRef = RelNodeRef<OptRelNodeTyp>;

trait OptRelNode {
    fn into_rel_node(self) -> OptRelNodeRef;
    fn from_rel_node(rel_node: OptRelNodeRef) -> Option<Self>
    where
        Self: Sized;
    fn explain(&self) {}
}

#[derive(Clone, Debug)]
pub struct PlanNode(OptRelNodeRef);

impl OptRelNode for PlanNode {
    fn into_rel_node(self) -> OptRelNodeRef {
        self.0
    }
    fn from_rel_node(rel_node: OptRelNodeRef) -> Option<Self> {
        if !rel_node.typ.is_plan_node() {
            return None;
        }
        Some(Self(rel_node))
    }
}

#[derive(Clone, Debug)]
pub struct Expr(OptRelNodeRef);

impl OptRelNode for Expr {
    fn into_rel_node(self) -> OptRelNodeRef {
        self.0
    }
    fn from_rel_node(rel_node: OptRelNodeRef) -> Option<Self> {
        if !rel_node.typ.is_expression() {
            return None;
        }
        Some(Self(rel_node))
    }
}

#[derive(Clone, Debug)]
pub struct LogicalJoin(PlanNode);

impl OptRelNode for LogicalJoin {
    fn into_rel_node(self) -> OptRelNodeRef {
        self.0.into_rel_node()
    }
    fn from_rel_node(rel_node: OptRelNodeRef) -> Option<Self> {
        if rel_node.typ != OptRelNodeTyp::Join {
            return None;
        }
        PlanNode::from_rel_node(rel_node).map(Self)
    }
}

impl LogicalJoin {
    pub fn new(left: PlanNode, right: PlanNode, cond: Expr, join_type: JoinType) -> LogicalJoin {
        LogicalJoin(PlanNode(
            RelNode {
                typ: OptRelNodeTyp::Join,
                children: vec![
                    left.into_rel_node(),
                    right.into_rel_node(),
                    cond.into_rel_node(),
                ],
                data: Some(Value::Int(join_type as i64)),
            }
            .into(),
        ))
    }

    pub fn left_child(&self) -> PlanNode {
        PlanNode::from_rel_node(self.clone().into_rel_node().children[0].clone()).unwrap()
    }

    pub fn right_child(&self) -> PlanNode {
        PlanNode::from_rel_node(self.clone().into_rel_node().children[1].clone()).unwrap()
    }

    pub fn cond(&self) -> Expr {
        Expr::from_rel_node(self.clone().into_rel_node().children[3].clone()).unwrap()
    }

    pub fn join_type(&self) -> JoinType {
        JoinType::from_i64(self.0 .0.data.as_ref().unwrap().as_i64()).unwrap()
    }
}

#[derive(Clone, Debug)]
pub struct ConstantExpr(Expr);

impl OptRelNode for ConstantExpr {
    fn into_rel_node(self) -> OptRelNodeRef {
        self.0.into_rel_node()
    }
    fn from_rel_node(rel_node: OptRelNodeRef) -> Option<Self> {
        if rel_node.typ != OptRelNodeTyp::Constant {
            return None;
        }
        Expr::from_rel_node(rel_node).map(Self)
    }
}

#[derive(Clone, Debug)]
pub struct ColumnRefExpr(Expr);

impl OptRelNode for ColumnRefExpr {
    fn into_rel_node(self) -> OptRelNodeRef {
        self.0.into_rel_node()
    }
    fn from_rel_node(rel_node: OptRelNodeRef) -> Option<Self> {
        if rel_node.typ != OptRelNodeTyp::ColumnRef {
            return None;
        }
        Expr::from_rel_node(rel_node).map(Self)
    }
}

pub fn constant(value: Value) -> ConstantExpr {
    ConstantExpr(Expr(
        RelNode {
            typ: OptRelNodeTyp::Constant,
            children: vec![],
            data: Some(value),
        }
        .into(),
    ))
}

pub fn column_ref(column_idx: usize) -> ConstantExpr {
    ConstantExpr(Expr(
        RelNode {
            typ: OptRelNodeTyp::ColumnRef,
            children: vec![],
            data: Some(Value::Int(column_idx as i64)),
        }
        .into(),
    ))
}

pub fn explain(rel_node: OptRelNodeRef) {
    match rel_node.typ {
        OptRelNodeTyp::ColumnRef => ColumnRefExpr::from_rel_node(rel_node).unwrap().explain(),
        OptRelNodeTyp::Constant => ConstantExpr::from_rel_node(rel_node).unwrap().explain(),
        OptRelNodeTyp::Join => LogicalJoin::from_rel_node(rel_node).unwrap().explain(),
        _ => unimplemented!(),
    }
}
