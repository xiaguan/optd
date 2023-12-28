# Integration with Datafusion

optd is currently used as a physical optimizer for Apache Arrow Datafusion. To interact with Datafusion, you may use the following command to start the Datafusion cli.

```bash
cargo run --bin datafusion-optd-cli
cargo run --bin datafusion-optd-cli -- -f tpch/test.sql # run TPC-H queries
```

optd is designed as a flexible optimizer framework that can be used in any database systems. The core of optd is in `optd-core`, which contains the Cascades optimizer implementation and the definition of key structures in the optimization process. Users can implement the interfaces and use optd in their own database systems by using the `optd-core` crate.

The optd Datafusion representation contains Datafusion plan nodes, SQL expressions, optimizer rules, properties, and cost models, as in the `optd-datafusion-repr` crate.

The `optd-datafusion-bridge` crate contains necessary code to convert Datafusion logical plans into optd Datafusion representation and convert optd Datafusion representation back into Datafusion physical plans. It implements the `QueryPlanner` trait so that it can be easily integrated into Datafusion.

![integration with Datafusion](./optd-cascades/optd-datafusion-overview.svg)

## Plan Nodes

This is an incomplete list of all Datafusion plan nodes and their representations that we have implemented in the system.

```
Join(type) left:PlanNode right:PlanNode cond:Expr
Projection expr_list:ExprList
Agg child:PlanNode expr_list:ExprList groups:ExprList
Scan table:String
ExprList ...children:Expr
Sort child:PlanNode sort_exprs:ExprList <- requiring SortExprs
... and others
```

Note that only `ExprList` or `List` can have variable number of children. All plan nodes only have a fixed number of children. For projections and aggregations where users will need to provide a list of expressions, they will have `List` node as their direct child.

Developers can use the `define_plan_node` macro to add new plan nodes into the optd-datafusion-repr.

```rust
#[derive(Clone, Debug)]
pub struct LogicalJoin(pub PlanNode);

define_plan_node!(
    LogicalJoin : PlanNode,
    Join, [
        { 0, left: PlanNode },
        { 1, right: PlanNode }
    ], [
        { 2, cond: Expr }
    ], { join_type: JoinType }
);
```

Developers will also need to add the plan node type into the `OptRelNodeTyp` enum, implement `is_plan_node` and `is_expression` for them, and implement the explain format in `explain`.

## Expressions

SQL Expressions are also a kind of `RelNode`. We have binary expressions, function calls, etc. in the representation.

Notably, we convert all column references into column indexes in the Datafusion bridge. For example, if Datafusion yields a logical plan of:

```
LogicalJoin { a = b }
  Scan t1 [a, v1, v2]
  Scan t2 [b, v3, v4]
```

It will be converted to:

```
LogicalJoin { #0 = #3 }
  Scan t1 
  Scan t2
```

in the optd representation.

## Explain

We use risinglightdb's pretty-xmlish crate and implement a custom explain format for Datafusion plan nodes.

```rust
PhysicalProjection { exprs: [ #0 ] }                                             
└── PhysicalHashJoin { join_type: Inner, left_keys: [ #0 ], right_keys: [ #0 ] } 
    ├── PhysicalProjection { exprs: [ #0 ] }                                     
    │   └── PhysicalScan { table: t1 }                                           
    └── PhysicalProjection { exprs: [ #0 ] }                                     
        └── PhysicalScan { table: t2 }
```

This is different from the default Lisp-representation of the `RelNode`.

## Rules

Currently, we have a few rules that pulls filters and projections up and down through joins. Also, we have join assoc and join commute rules to reorder the joins.

## Properties

We have the `Schema` property that will be used in the optimizer rules to determine number of columns of each plan nodes so that we can rewrite column reference expressions correctly.

## Cost Model

We have a simple cost model that computes I/O cost and compute cost based on number of rows of the children plan nodes.
