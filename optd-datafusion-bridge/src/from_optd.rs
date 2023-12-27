use std::{collections::HashMap, sync::Arc};

use anyhow::{bail, Context, Result};
use async_recursion::async_recursion;
use datafusion::{
    arrow::{
        compute::kernels::filter,
        datatypes::{Schema, SchemaRef},
    },
    datasource::source_as_provider,
    logical_expr::Operator,
    physical_expr,
    physical_plan::{
        self,
        aggregates::AggregateMode,
        explain::ExplainExec,
        expressions::create_aggregate_expr,
        joins::{
            utils::{ColumnIndex, JoinFilter},
            PartitionMode,
        },
        projection::ProjectionExec,
        AggregateExpr, ExecutionPlan, PhysicalExpr,
    },
    scalar::ScalarValue,
};
use optd_datafusion_repr::{
    plan_nodes::{
        BinOpExpr, BinOpType, ColumnRefExpr, ConstantExpr, ConstantType, Expr, FuncExpr, FuncType,
        JoinType, LogOpExpr, LogOpType, OptRelNode, OptRelNodeRef, OptRelNodeTyp, PhysicalAgg,
        PhysicalFilter, PhysicalHashJoin, PhysicalNestedLoopJoin, PhysicalProjection, PhysicalScan,
        PhysicalSort, PlanNode, SortOrderExpr, SortOrderType,
    },
    PhysicalCollector, Value,
};

use crate::{physical_collector::CollectorExec, OptdPlanContext};

impl OptdPlanContext<'_> {
    #[async_recursion]
    async fn from_optd_table_scan(
        &mut self,
        node: PhysicalScan,
    ) -> Result<Arc<dyn ExecutionPlan + 'static>> {
        let source = self.tables.get(node.table().as_ref()).unwrap();
        let provider = source_as_provider(source)?;
        let plan = provider.scan(self.session_state, None, &[], None).await?;
        Ok(plan)
    }

    fn from_optd_sort_order_expr(
        &mut self,
        sort_expr: SortOrderExpr,
        context: &SchemaRef,
    ) -> Result<physical_expr::PhysicalSortExpr> {
        let expr = self.from_optd_expr(sort_expr.child(), context)?;
        Ok(physical_expr::PhysicalSortExpr {
            expr,
            options: match sort_expr.order() {
                SortOrderType::Asc => datafusion::arrow::compute::SortOptions {
                    descending: false,
                    nulls_first: true,
                },
                SortOrderType::Desc => datafusion::arrow::compute::SortOptions {
                    descending: true,
                    nulls_first: true,
                },
            },
        })
    }

    fn from_optd_agg_expr(
        &mut self,
        expr: Expr,
        context: &SchemaRef,
    ) -> Result<Arc<dyn AggregateExpr>> {
        let expr = FuncExpr::from_rel_node(expr.into_rel_node()).unwrap();
        let typ = expr.func();
        let FuncType::Agg(func) = typ else {
            unreachable!()
        };
        let args = expr
            .children()
            .to_vec()
            .into_iter()
            .map(|expr| self.from_optd_expr(expr, context))
            .collect::<Result<Vec<_>>>()?;
        Ok(create_aggregate_expr(
            &func,
            false,
            &args,
            &[],
            &context,
            "<agg_func>",
        )?)
    }

    fn from_optd_expr(&mut self, expr: Expr, context: &SchemaRef) -> Result<Arc<dyn PhysicalExpr>> {
        match expr.typ() {
            OptRelNodeTyp::ColumnRef => {
                let expr = ColumnRefExpr::from_rel_node(expr.into_rel_node()).unwrap();
                let idx = expr.index();
                Ok(Arc::new(
                    datafusion::physical_plan::expressions::Column::new("<expr>", idx),
                ))
            }
            OptRelNodeTyp::Constant(typ) => {
                let expr = ConstantExpr::from_rel_node(expr.into_rel_node()).unwrap();
                let value = expr.value();
                let value = match typ {
                    ConstantType::Bool => ScalarValue::Boolean(Some(value.as_bool())),
                    ConstantType::Int => ScalarValue::Int64(Some(value.as_i64())),
                    ConstantType::Decimal => {
                        ScalarValue::Decimal128(Some(value.as_f64() as i128), 20, 0)
                        // TODO(chi): no hard code decimal
                    }
                    ConstantType::Date => ScalarValue::Date32(Some(value.as_i64() as i32)),
                    ConstantType::Utf8String => ScalarValue::Utf8(Some(value.as_str().to_string())),
                    ConstantType::Any => unimplemented!(),
                };
                Ok(Arc::new(
                    datafusion::physical_plan::expressions::Literal::new(value),
                ))
            }
            OptRelNodeTyp::Func(_) => {
                let expr = FuncExpr::from_rel_node(expr.into_rel_node()).unwrap();
                let func = expr.func();
                let args = expr
                    .children()
                    .to_vec()
                    .into_iter()
                    .map(|expr| self.from_optd_expr(expr, context))
                    .collect::<Result<Vec<_>>>()?;
                match func {
                    FuncType::Scalar(func) => {
                        Ok(datafusion::physical_expr::functions::create_physical_expr(
                            &func,
                            &args,
                            context,
                            &physical_expr::execution_props::ExecutionProps::new(),
                        )?)
                    }
                    FuncType::Case => {
                        let when_expr = args[0].clone();
                        let then_expr = args[1].clone();
                        let else_expr = args[2].clone();
                        Ok(physical_expr::expressions::case(
                            None,
                            vec![(when_expr, then_expr)],
                            Some(else_expr),
                        )?)
                    }
                    _ => unreachable!(),
                }
            }
            OptRelNodeTyp::Sort => unreachable!(),
            OptRelNodeTyp::LogOp(typ) => {
                let expr = LogOpExpr::from_rel_node(expr.into_rel_node()).unwrap();
                let mut children = expr.children().to_vec().into_iter();
                let first_expr = self.from_optd_expr(children.next().unwrap(), context)?;
                let op = match typ {
                    LogOpType::And => datafusion::logical_expr::Operator::And,
                    LogOpType::Or => datafusion::logical_expr::Operator::Or,
                };
                children.try_fold(first_expr, |acc, expr| {
                    let expr = self.from_optd_expr(expr, context)?;
                    Ok(
                        Arc::new(datafusion::physical_plan::expressions::BinaryExpr::new(
                            acc, op, expr,
                        )) as Arc<dyn PhysicalExpr>,
                    )
                })
            }
            OptRelNodeTyp::BinOp(op) => {
                let expr = BinOpExpr::from_rel_node(expr.into_rel_node()).unwrap();
                let left = self.from_optd_expr(expr.left_child(), context)?;
                let right = self.from_optd_expr(expr.right_child(), context)?;
                let op = match op {
                    BinOpType::Eq => Operator::Eq,
                    BinOpType::Neq => Operator::NotEq,
                    BinOpType::Leq => Operator::LtEq,
                    BinOpType::Geq => Operator::GtEq,
                    BinOpType::And => Operator::And,
                    BinOpType::Add => Operator::Plus,
                    BinOpType::Sub => Operator::Minus,
                    BinOpType::Mul => Operator::Multiply,
                    BinOpType::Div => Operator::Divide,
                    op => unimplemented!("{}", op),
                };
                Ok(
                    Arc::new(datafusion::physical_plan::expressions::BinaryExpr::new(
                        left, op, right,
                    )) as Arc<dyn PhysicalExpr>,
                )
            }
            _ => unimplemented!("{}", expr.into_rel_node()),
        }
    }

    #[async_recursion]
    async fn from_optd_projection(
        &mut self,
        node: PhysicalProjection,
    ) -> Result<Arc<dyn ExecutionPlan + 'static>> {
        let input_exec = self.from_optd_plan_node(node.child()).await?;
        let physical_exprs = node
            .exprs()
            .to_vec()
            .into_iter()
            .enumerate()
            .map(|(idx, expr)| {
                Ok((
                    self.from_optd_expr(expr, &input_exec.schema())?,
                    format!("col{}", idx),
                ))
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(
            Arc::new(ProjectionExec::try_new(physical_exprs, input_exec)?)
                as Arc<dyn ExecutionPlan + 'static>,
        )
    }

    #[async_recursion]
    async fn from_optd_filter(
        &mut self,
        node: PhysicalFilter,
    ) -> Result<Arc<dyn ExecutionPlan + 'static>> {
        let input_exec = self.from_optd_plan_node(node.child()).await?;
        let physical_expr = self.from_optd_expr(node.cond(), &input_exec.schema())?;
        Ok(
            Arc::new(datafusion::physical_plan::filter::FilterExec::try_new(
                physical_expr,
                input_exec,
            )?) as Arc<dyn ExecutionPlan + 'static>,
        )
    }

    #[async_recursion]
    async fn from_optd_sort(
        &mut self,
        node: PhysicalSort,
    ) -> Result<Arc<dyn ExecutionPlan + 'static>> {
        let input_exec = self.from_optd_plan_node(node.child()).await?;
        let physical_exprs = node
            .exprs()
            .to_vec()
            .into_iter()
            .map(|expr| {
                self.from_optd_sort_order_expr(
                    SortOrderExpr::from_rel_node(expr.into_rel_node()).unwrap(),
                    &input_exec.schema(),
                )
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(
            Arc::new(datafusion::physical_plan::sorts::sort::SortExec::new(
                physical_exprs,
                input_exec,
            )) as Arc<dyn ExecutionPlan + 'static>,
        )
    }

    #[async_recursion]
    async fn from_optd_agg(
        &mut self,
        node: PhysicalAgg,
    ) -> Result<Arc<dyn ExecutionPlan + 'static>> {
        let input_exec = self.from_optd_plan_node(node.child()).await?;
        let agg_exprs = node
            .aggrs()
            .to_vec()
            .into_iter()
            .map(|expr| self.from_optd_agg_expr(expr, &input_exec.schema()))
            .collect::<Result<Vec<_>>>()?;
        let group_exprs = node
            .groups()
            .to_vec()
            .into_iter()
            .map(|expr| {
                Ok((
                    self.from_optd_expr(expr, &input_exec.schema())?,
                    "<agg_expr>".to_string(),
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let group_exprs = physical_plan::aggregates::PhysicalGroupBy::new_single(group_exprs);
        let agg_num = agg_exprs.len();
        let schema = input_exec.schema().clone();
        Ok(Arc::new(
            datafusion::physical_plan::aggregates::AggregateExec::try_new(
                AggregateMode::Single,
                group_exprs,
                agg_exprs,
                vec![None; agg_num],
                vec![None; agg_num],
                input_exec,
                schema,
            )?,
        ) as Arc<dyn ExecutionPlan + 'static>)
    }

    #[async_recursion]
    async fn from_optd_nested_loop_join(
        &mut self,
        node: PhysicalNestedLoopJoin,
    ) -> Result<Arc<dyn ExecutionPlan + 'static>> {
        let left_exec = self.from_optd_plan_node(node.left()).await?;
        let right_exec = self.from_optd_plan_node(node.right()).await?;
        let filter_schema = {
            let fields = left_exec
                .schema()
                .fields()
                .into_iter()
                .chain(right_exec.schema().fields().into_iter())
                .cloned()
                .collect::<Vec<_>>();
            Schema::new_with_metadata(fields, HashMap::new())
        };

        let physical_expr = self.from_optd_expr(node.cond(), &Arc::new(filter_schema.clone()))?;
        let join_type = match node.join_type() {
            JoinType::Inner => datafusion::logical_expr::JoinType::Inner,
            JoinType::LeftOuter => datafusion::logical_expr::JoinType::Left,
            _ => unimplemented!(),
        };

        let mut column_idxs = vec![];
        for i in 0..left_exec.schema().fields().len() {
            column_idxs.push(ColumnIndex {
                index: i,
                side: physical_plan::joins::utils::JoinSide::Left,
            });
        }
        for i in 0..right_exec.schema().fields().len() {
            column_idxs.push(ColumnIndex {
                index: i,
                side: physical_plan::joins::utils::JoinSide::Right,
            });
        }

        Ok(Arc::new(
            datafusion::physical_plan::joins::NestedLoopJoinExec::try_new(
                left_exec,
                right_exec,
                Some(JoinFilter::new(physical_expr, column_idxs, filter_schema)),
                &join_type,
            )?,
        ) as Arc<dyn ExecutionPlan + 'static>)
    }

    #[async_recursion]
    async fn from_optd_hash_join(
        &mut self,
        node: PhysicalHashJoin,
    ) -> Result<Arc<dyn ExecutionPlan + 'static>> {
        let left_exec = self.from_optd_plan_node(node.left()).await?;
        let right_exec = self.from_optd_plan_node(node.right()).await?;
        let join_type = match node.join_type() {
            JoinType::Inner => datafusion::logical_expr::JoinType::Inner,
            _ => unimplemented!(),
        };
        let left_exprs = node.left_keys().to_vec();
        let right_exprs = node.right_keys().to_vec();
        assert_eq!(left_exprs.len(), right_exprs.len());
        let mut on = Vec::with_capacity(left_exprs.len());
        for (left_expr, right_expr) in left_exprs.into_iter().zip(right_exprs.into_iter()) {
            let Some(left_expr) = ColumnRefExpr::from_rel_node(left_expr.into_rel_node()) else {
                bail!("left expr is not column ref")
            };
            let Some(right_expr) = ColumnRefExpr::from_rel_node(right_expr.into_rel_node()) else {
                bail!("right expr is not column ref")
            };
            on.push((
                physical_expr::expressions::Column::new(
                    left_exec.schema().field(left_expr.index()).name(),
                    left_expr.index(),
                ),
                physical_expr::expressions::Column::new(
                    right_exec.schema().field(right_expr.index()).name(),
                    right_expr.index(),
                ),
            ));
        }
        Ok(
            Arc::new(datafusion::physical_plan::joins::HashJoinExec::try_new(
                left_exec,
                right_exec,
                on,
                None,
                &join_type,
                PartitionMode::CollectLeft,
                false,
            )?) as Arc<dyn ExecutionPlan + 'static>,
        )
    }

    #[async_recursion]
    async fn from_optd_plan_node(&mut self, node: PlanNode) -> Result<Arc<dyn ExecutionPlan>> {
        let rel_node = node.into_rel_node();
        let rel_node_dbg = rel_node.clone();
        let result = match &rel_node.typ {
            OptRelNodeTyp::PhysicalScan => {
                self.from_optd_table_scan(PhysicalScan::from_rel_node(rel_node).unwrap())
                    .await
            }
            OptRelNodeTyp::PhysicalProjection => {
                self.from_optd_projection(PhysicalProjection::from_rel_node(rel_node).unwrap())
                    .await
            }
            OptRelNodeTyp::PhysicalFilter => {
                self.from_optd_filter(PhysicalFilter::from_rel_node(rel_node).unwrap())
                    .await
            }
            OptRelNodeTyp::PhysicalSort => {
                self.from_optd_sort(PhysicalSort::from_rel_node(rel_node).unwrap())
                    .await
            }
            OptRelNodeTyp::PhysicalAgg => {
                self.from_optd_agg(PhysicalAgg::from_rel_node(rel_node).unwrap())
                    .await
            }
            OptRelNodeTyp::PhysicalNestedLoopJoin(_) => {
                self.from_optd_nested_loop_join(
                    PhysicalNestedLoopJoin::from_rel_node(rel_node).unwrap(),
                )
                .await
            }
            OptRelNodeTyp::PhysicalHashJoin(_) => {
                self.from_optd_hash_join(PhysicalHashJoin::from_rel_node(rel_node).unwrap())
                    .await
            }
            OptRelNodeTyp::PhysicalCollector(_) => {
                let node = PhysicalCollector::from_rel_node(rel_node).unwrap();
                let child = self.from_optd_plan_node(node.child()).await?;
                Ok(Arc::new(CollectorExec::new(
                    child,
                    node.group_id(),
                    self.optimizer.as_ref().unwrap().runtime_statistics.clone(),
                )) as Arc<dyn ExecutionPlan>)
            }
            typ => unimplemented!("{}", typ),
        };
        result.with_context(|| format!("when processing {}", rel_node_dbg))
    }

    pub async fn from_optd(&mut self, root_rel: OptRelNodeRef) -> Result<Arc<dyn ExecutionPlan>> {
        self.from_optd_plan_node(PlanNode::from_rel_node(root_rel).unwrap())
            .await
    }
}
