use std::fmt::{self, Write};

use crate::{
    AggregateFunction, BoundValue, GraphPhysicalPlan, PhysicalBinaryOperator, PhysicalExpression,
    PhysicalProjection, PhysicalSort, PhysicalUnaryOperator, ScalarValue, Symbol, ValueOrigin,
};

pub fn explain_physical_plan(plan: &GraphPhysicalPlan) -> String {
    let mut output = String::new();
    render_plan(plan, 0, &mut output);
    output
}

thread_local! {
    /// Set only while [`explain_physical_plan_shape`] renders. Rendering is
    /// synchronous, so a thread-local scoped to one call cannot leak into
    /// another plan's output.
    static REDACT_LITERALS: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

fn redacting_literals() -> bool {
    REDACT_LITERALS.with(std::cell::Cell::get)
}

/// A physical field the planner *derived* from something the shape must not
/// split on: a literal, as `OrderedVertexPropertyScan.required` is derived
/// from `SKIP + LIMIT`, or the statistics behind `SortExec`'s estimate.
///
/// These are plain integers by the time they reach a plan, so [`write_value`]
/// never sees them and redacting the `BoundValue` they came from does not
/// redact the copy. Rendered as `?` in a shape, they stop `LIMIT 5` and
/// `LIMIT 10` — or one query before and after a statistics refresh — from
/// hashing to two plan identities for one operator tree.
fn render_derived(value: impl fmt::Display) -> String {
    if redacting_literals() {
        "?".to_string()
    } else {
        value.to_string()
    }
}

/// [`explain_physical_plan`] with every literal value rendered as `?` and
/// every value list elided.
///
/// Parameters already render by name, so this is the plan with no tenant
/// value in it: safe to attach to a trace, and stable across requests that
/// differ only in literals, which is what makes it hashable into a plan
/// identity.
pub fn explain_physical_plan_shape(plan: &GraphPhysicalPlan) -> String {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            REDACT_LITERALS.with(|redact| redact.set(false));
        }
    }
    REDACT_LITERALS.with(|redact| redact.set(true));
    let _reset = Reset;
    explain_physical_plan(plan)
}

fn render_plan(plan: &GraphPhysicalPlan, depth: usize, output: &mut String) {
    let indent = "  ".repeat(depth);
    match plan {
        GraphPhysicalPlan::Union { arms, all } => {
            let _ = writeln!(output, "{indent}UnionExec(all={all})");
            for arm in arms {
                render_plan(arm, depth + 1, output);
            }
        }
        GraphPhysicalPlan::VertexIdSeek {
            binding,
            labels,
            value,
        } => {
            let _ = writeln!(
                output,
                "{indent}VertexIdSeek(binding={binding}, labels={}, value={})",
                render_symbols(labels),
                render_value(value)
            );
        }
        GraphPhysicalPlan::VertexPropertySeek {
            binding,
            labels,
            property,
            value,
        } => {
            let _ = writeln!(
                output,
                "{indent}VertexPropertySeek(binding={binding}, labels={}, property={property}, value={})",
                render_symbols(labels),
                render_value(value)
            );
        }
        GraphPhysicalPlan::VertexPropertyMultiSeek {
            binding,
            labels,
            property,
            values,
        } => {
            // A plan shape must not vary with how many values a list bound.
            let values = if redacting_literals() {
                "...".to_string()
            } else {
                values
                    .iter()
                    .map(render_value)
                    .collect::<Vec<_>>()
                    .join(", ")
            };
            let _ = writeln!(
                output,
                "{indent}VertexPropertyMultiSeek(binding={binding}, labels={}, property={property}, values=[{values}])",
                render_symbols(labels)
            );
        }
        GraphPhysicalPlan::VertexPropertyScan {
            binding,
            labels,
            property,
        } => {
            let _ = writeln!(
                output,
                "{indent}VertexPropertyScan(binding={binding}, labels={}, property={property})",
                render_symbols(labels)
            );
        }
        GraphPhysicalPlan::OrderedVertexPropertyScan {
            binding,
            labels,
            property,
            direction,
            prefix,
            lower,
            upper,
            required,
            items,
            residual,
        } => {
            let prefix = prefix
                .as_ref()
                .map(render_value)
                .unwrap_or_else(|| "none".to_string());
            let lower = render_bound(lower.as_ref());
            let upper = render_bound(upper.as_ref());
            // Printed because the scan absorbed a Filter: without it the plan
            // would read as though the index proved the whole predicate.
            let residual = residual
                .as_ref()
                .map(render_expression)
                .unwrap_or_else(|| "none".to_string());
            let required = render_derived(required);
            let _ = writeln!(
                output,
                "{indent}OrderedVertexPropertyScan(binding={binding}, labels={}, property={property}, direction={direction}, prefix={prefix}, lower={lower}, upper={upper}, required={required}, residual={residual}, items=[{}])",
                render_symbols(labels),
                SortList(items)
            );
        }
        GraphPhysicalPlan::VertexLabelScan {
            binding,
            label,
            labels,
        } => {
            let _ = writeln!(
                output,
                "{indent}VertexLabelScan(binding={binding}, label={label}, labels={})",
                render_symbols(labels)
            );
        }
        GraphPhysicalPlan::AllVertexScan { binding } => {
            let _ = writeln!(output, "{indent}AllVertexScan(binding={binding})");
        }
        GraphPhysicalPlan::RelationshipPropertySeek {
            from,
            source_labels,
            relationship,
            relationship_type,
            property,
            value,
            direction,
            to,
            target_labels,
        } => {
            let _ = writeln!(
                output,
                "{indent}RelationshipPropertySeek(from={from}, source_labels={}, relationship={relationship}, type={relationship_type}, property={property}, value={}, direction={direction}, to={to}, target_labels={})",
                render_symbols(source_labels),
                render_value(value),
                render_symbols(target_labels)
            );
        }
        GraphPhysicalPlan::Expand {
            input,
            from,
            relationship,
            relationship_types,
            direction,
            to,
            target_labels,
        } => {
            let relationship = relationship
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_else(|| "_".to_string());
            let _ = writeln!(
                output,
                "{indent}ExpandExec(from={from}, relationship={relationship}, types={}, direction={direction}, to={to}, target_labels={})",
                render_symbols(relationship_types),
                render_symbols(target_labels)
            );
            render_plan(input, depth + 1, output);
        }
        GraphPhysicalPlan::VariableExpand {
            input,
            from,
            relationship_types,
            direction,
            to,
            target_labels,
            min_hops,
            max_hops,
        } => {
            // `*1..2` and `*1..5` are the same operator tree over the same
            // schema names, so they are one shape however differently they
            // read: the bounds come from pattern literals exactly as `LIMIT`
            // does, and `LimitExec(count=?)` has always been redacted.
            let hops = format!("{}..{}", render_derived(min_hops), render_derived(max_hops));
            let _ = writeln!(
                output,
                "{indent}VariableExpandExec(from={from}, types={}, direction={direction}, to={to}, target_labels={}, hops={hops})",
                render_symbols(relationship_types),
                render_symbols(target_labels)
            );
            render_plan(input, depth + 1, output);
        }
        GraphPhysicalPlan::NaturalJoin {
            left,
            right,
            optional,
        } => {
            let name = if *optional {
                "LeftOuterNaturalJoinExec"
            } else {
                "NaturalJoinExec"
            };
            let _ = writeln!(output, "{indent}{name}");
            render_plan(left, depth + 1, output);
            render_plan(right, depth + 1, output);
        }
        GraphPhysicalPlan::Filter { input, predicate } => {
            let _ = writeln!(
                output,
                "{indent}FilterExec(predicate={})",
                render_expression(predicate)
            );
            render_plan(input, depth + 1, output);
        }
        GraphPhysicalPlan::Sort {
            input,
            items,
            estimated_input_rows,
        } => {
            let items = items.iter().map(render_sort).collect::<Vec<_>>().join(", ");
            // One `?` for both arms while redacting: whether statistics were
            // available is a property of the moment, not of the plan, and a
            // shape that told them apart would give one query two identities
            // either side of a statistics refresh.
            let estimate = if redacting_literals() {
                "?".to_string()
            } else {
                estimated_input_rows
                    .map(|rows| rows.to_string())
                    .unwrap_or_else(|| "unknown".to_string())
            };
            let _ = writeln!(
                output,
                "{indent}SortExec(items=[{items}], estimated_input_rows={estimate})"
            );
            render_plan(input, depth + 1, output);
        }
        GraphPhysicalPlan::Skip { input, count } => {
            let _ = writeln!(output, "{indent}SkipExec(count={})", render_value(count));
            render_plan(input, depth + 1, output);
        }
        GraphPhysicalPlan::Limit { input, count } => {
            let _ = writeln!(output, "{indent}LimitExec(count={})", render_value(count));
            render_plan(input, depth + 1, output);
        }
        GraphPhysicalPlan::Project {
            input,
            items,
            distinct,
            ..
        } => {
            let items = items
                .iter()
                .map(render_projection)
                .collect::<Vec<_>>()
                .join(", ");
            let _ = writeln!(
                output,
                "{indent}ProjectExec(distinct={distinct}, items=[{items}])"
            );
            render_plan(input, depth + 1, output);
        }
    }
}

fn render_bound(bound: Option<&(BoundValue, bool)>) -> String {
    match bound {
        Some((value, true)) => format!("inclusive {}", render_value(value)),
        Some((value, false)) => format!("exclusive {}", render_value(value)),
        None => "none".to_string(),
    }
}

fn render_sort(sort: &PhysicalSort) -> String {
    let mut output = String::new();
    let _ = write_sort(&mut output, sort);
    output
}

struct SortList<'a>(&'a [PhysicalSort]);

impl fmt::Display for SortList<'_> {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, sort) in self.0.iter().enumerate() {
            if index > 0 {
                output.write_str(", ")?;
            }
            write_sort(output, sort)?;
        }
        Ok(())
    }
}

fn write_sort(output: &mut impl Write, sort: &PhysicalSort) -> fmt::Result {
    write_expression(output, &sort.expression)?;
    write!(output, " {}", sort.direction)
}

fn render_projection(projection: &PhysicalProjection) -> String {
    let expression = render_expression(&projection.expression);
    match &projection.alias {
        Some(alias) => format!("{expression} AS {alias}"),
        None => expression,
    }
}

fn render_expression(expression: &PhysicalExpression) -> String {
    let mut output = String::new();
    let _ = write_expression(&mut output, expression);
    output
}

fn write_expression(output: &mut impl Write, expression: &PhysicalExpression) -> fmt::Result {
    match expression {
        PhysicalExpression::Binding(binding) => write!(output, "{binding}"),
        PhysicalExpression::Identity(binding) => write!(output, "{binding}.id"),
        PhysicalExpression::Property { binding, property } => {
            write!(output, "{binding}.{property}")
        }
        PhysicalExpression::Value(value) => write_value(output, value),
        // Candidate values are rendered as a count rather than inline: an IN
        // list can hold hundreds of entries, and EXPLAIN output is read by
        // people.
        PhysicalExpression::InList { expression, values } => {
            write_expression(output, expression)?;
            if redacting_literals() {
                output.write_str(" IN [...]")
            } else {
                write!(output, " IN [{} values]", values.len())
            }
        }
        PhysicalExpression::Aggregate {
            function,
            expression,
        } => {
            let function = match function {
                AggregateFunction::Count => "count",
                AggregateFunction::Sum => "sum",
                AggregateFunction::Average => "avg",
                AggregateFunction::Collect => "collect",
            };
            write!(output, "{function}(")?;
            match expression.as_deref() {
                Some(expression) => write_expression(output, expression)?,
                None => output.write_char('*')?,
            }
            output.write_char(')')
        }
        PhysicalExpression::Unary {
            operator,
            expression,
        } => {
            let (prefix, suffix) = match operator {
                PhysicalUnaryOperator::Not => ("NOT ", ""),
                PhysicalUnaryOperator::Plus => ("+", ""),
                PhysicalUnaryOperator::Minus => ("-", ""),
                PhysicalUnaryOperator::IsNull => ("", " IS NULL"),
                PhysicalUnaryOperator::IsNotNull => ("", " IS NOT NULL"),
            };
            output.write_str(prefix)?;
            write_expression(output, expression)?;
            output.write_str(suffix)
        }
        PhysicalExpression::Binary {
            left,
            operator,
            right,
        } => {
            let operator = match operator {
                PhysicalBinaryOperator::Equal => "=",
                PhysicalBinaryOperator::NotEqual => "<>",
                PhysicalBinaryOperator::LessThan => "<",
                PhysicalBinaryOperator::LessThanOrEqual => "<=",
                PhysicalBinaryOperator::GreaterThan => ">",
                PhysicalBinaryOperator::GreaterThanOrEqual => ">=",
                PhysicalBinaryOperator::And => "AND",
                PhysicalBinaryOperator::Or => "OR",
                PhysicalBinaryOperator::Add => "+",
                PhysicalBinaryOperator::Subtract => "-",
                PhysicalBinaryOperator::Multiply => "*",
                PhysicalBinaryOperator::Divide => "/",
                PhysicalBinaryOperator::Modulo => "%",
                PhysicalBinaryOperator::StartsWith => "STARTS WITH",
            };
            output.write_char('(')?;
            write_expression(output, left)?;
            write!(output, " {operator} ")?;
            write_expression(output, right)?;
            output.write_char(')')
        }
    }
}

fn render_value(value: &BoundValue) -> String {
    let mut output = String::new();
    let _ = write_value(&mut output, value);
    output
}

fn write_value(output: &mut impl Write, value: &BoundValue) -> fmt::Result {
    match &value.origin {
        ValueOrigin::Parameter(name) => write!(output, "${name}"),
        ValueOrigin::Literal if redacting_literals() => output.write_str("?"),
        ValueOrigin::Literal => match &value.value {
            ScalarValue::Null => output.write_str("NULL"),
            ScalarValue::Boolean(value) => write!(output, "{value}"),
            ScalarValue::Integer(value) => write!(output, "{value}"),
            ScalarValue::Float(value) => write!(output, "{value}"),
            ScalarValue::String(value) => write!(output, "'{value}'"),
        },
    }
}

fn render_symbols(symbols: &[Symbol]) -> String {
    format!(
        "[{}]",
        symbols
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ")
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{PatternDirection, SortDirection};

    fn binding(name: &str) -> PhysicalExpression {
        PhysicalExpression::Binding(name.into())
    }

    fn literal(value: ScalarValue) -> PhysicalExpression {
        PhysicalExpression::Value(BoundValue {
            value,
            origin: ValueOrigin::Literal,
        })
    }

    #[test]
    fn renders_every_expression_and_value_shape_exactly() {
        assert_eq!(render_expression(&binding("n")), "n");
        assert_eq!(
            render_expression(&PhysicalExpression::Identity("n".into())),
            "n.id"
        );
        let property = PhysicalExpression::Property {
            binding: "n".into(),
            property: "score".into(),
        };
        assert_eq!(render_expression(&property), "n.score");

        for (value, expected) in [
            (ScalarValue::Null, "NULL"),
            (ScalarValue::Boolean(true), "true"),
            (ScalarValue::Integer(42), "42"),
            (ScalarValue::Float("1.5".into()), "1.5"),
            (ScalarValue::String("hello".into()), "'hello'"),
        ] {
            assert_eq!(render_expression(&literal(value)), expected);
        }
        assert_eq!(
            render_expression(&PhysicalExpression::Value(BoundValue {
                value: ScalarValue::Integer(42),
                origin: ValueOrigin::Parameter("value".into()),
            })),
            "$value"
        );
        assert_eq!(
            render_expression(&PhysicalExpression::InList {
                expression: Box::new(property.clone()),
                values: vec![
                    BoundValue {
                        value: ScalarValue::Integer(1),
                        origin: ValueOrigin::Literal,
                    },
                    BoundValue {
                        value: ScalarValue::Integer(2),
                        origin: ValueOrigin::Literal,
                    },
                ],
            }),
            "n.score IN [2 values]"
        );

        for (function, name) in [
            (AggregateFunction::Count, "count"),
            (AggregateFunction::Sum, "sum"),
            (AggregateFunction::Average, "avg"),
            (AggregateFunction::Collect, "collect"),
        ] {
            assert_eq!(
                render_expression(&PhysicalExpression::Aggregate {
                    function,
                    expression: Some(Box::new(binding("n"))),
                }),
                format!("{name}(n)")
            );
        }
        assert_eq!(
            render_expression(&PhysicalExpression::Aggregate {
                function: AggregateFunction::Count,
                expression: None,
            }),
            "count(*)"
        );

        for (operator, expected) in [
            (PhysicalUnaryOperator::Not, "NOT n"),
            (PhysicalUnaryOperator::Plus, "+n"),
            (PhysicalUnaryOperator::Minus, "-n"),
            (PhysicalUnaryOperator::IsNull, "n IS NULL"),
            (PhysicalUnaryOperator::IsNotNull, "n IS NOT NULL"),
        ] {
            assert_eq!(
                render_expression(&PhysicalExpression::Unary {
                    operator,
                    expression: Box::new(binding("n")),
                }),
                expected
            );
        }

        for (operator, symbol) in [
            (PhysicalBinaryOperator::Equal, "="),
            (PhysicalBinaryOperator::NotEqual, "<>"),
            (PhysicalBinaryOperator::LessThan, "<"),
            (PhysicalBinaryOperator::LessThanOrEqual, "<="),
            (PhysicalBinaryOperator::GreaterThan, ">"),
            (PhysicalBinaryOperator::GreaterThanOrEqual, ">="),
            (PhysicalBinaryOperator::And, "AND"),
            (PhysicalBinaryOperator::Or, "OR"),
            (PhysicalBinaryOperator::Add, "+"),
            (PhysicalBinaryOperator::Subtract, "-"),
            (PhysicalBinaryOperator::Multiply, "*"),
            (PhysicalBinaryOperator::Divide, "/"),
            (PhysicalBinaryOperator::Modulo, "%"),
            (PhysicalBinaryOperator::StartsWith, "STARTS WITH"),
        ] {
            assert_eq!(
                render_expression(&PhysicalExpression::Binary {
                    left: Box::new(binding("left")),
                    operator,
                    right: Box::new(binding("right")),
                }),
                format!("(left {symbol} right)")
            );
        }

        let sorts = [
            PhysicalSort {
                expression: binding("n"),
                direction: SortDirection::Ascending,
            },
            PhysicalSort {
                expression: PhysicalExpression::Identity("n".into()),
                direction: SortDirection::Descending,
            },
        ];
        assert_eq!(SortList(&sorts).to_string(), "n ASC, n.id DESC");
    }

    /// The two plan fields the planner derives rather than binds: an ordered
    /// scan's `required` (`SKIP + LIMIT`) and a sort's estimate. `EXPLAIN`
    /// prints both, and a shape must not, or two windows of one query hash to
    /// two plan identities.
    #[test]
    fn a_shape_elides_the_fields_derived_from_a_literal_or_from_statistics() {
        fn window(required: usize, estimated_input_rows: Option<u64>) -> GraphPhysicalPlan {
            let items = vec![PhysicalSort {
                expression: PhysicalExpression::Property {
                    binding: "n".into(),
                    property: "created_at".into(),
                },
                direction: SortDirection::Ascending,
            }];
            GraphPhysicalPlan::Sort {
                input: Box::new(GraphPhysicalPlan::OrderedVertexPropertyScan {
                    binding: "n".into(),
                    labels: vec!["Entity".into()],
                    property: "created_at".into(),
                    direction: SortDirection::Ascending,
                    prefix: None,
                    lower: None,
                    upper: None,
                    required,
                    items: items.clone(),
                    residual: None,
                }),
                items,
                estimated_input_rows,
            }
        }

        let five = explain_physical_plan_shape(&window(5, Some(1_000)));
        assert!(five.contains("required=?"), "{five}");
        assert!(five.contains("estimated_input_rows=?"), "{five}");
        // Same operator tree, a wider window and fresher statistics: one shape.
        assert_eq!(five, explain_physical_plan_shape(&window(10, Some(20_000))));
        // Statistics that have not landed yet are the same shape as statistics
        // that have; whether they exist is a property of the moment.
        assert_eq!(five, explain_physical_plan_shape(&window(10, None)));

        // A variable-length bound is the same kind of value, and reaches the
        // plan as a plain `usize` the same way.
        fn walk(min_hops: u8, max_hops: u8) -> GraphPhysicalPlan {
            GraphPhysicalPlan::VariableExpand {
                input: Box::new(GraphPhysicalPlan::AllVertexScan {
                    binding: "n".into(),
                }),
                from: "n".into(),
                relationship_types: vec!["KNOWS".into()],
                direction: PatternDirection::Outgoing,
                to: "m".into(),
                target_labels: vec![],
                min_hops,
                max_hops,
            }
        }
        let near = explain_physical_plan_shape(&walk(1, 2));
        assert!(near.contains("hops=?..?"), "{near}");
        assert_eq!(near, explain_physical_plan_shape(&walk(1, 5)));
        assert!(explain_physical_plan(&walk(1, 5)).contains("hops=1..5"));

        // `EXPLAIN` itself still prints both, which is the point of printing
        // them: the estimate next to the actual row count is how a bad
        // estimate is found.
        let explained = explain_physical_plan(&window(5, Some(1_000)));
        assert!(explained.contains("required=5"), "{explained}");
        assert!(
            explained.contains("estimated_input_rows=1000"),
            "{explained}"
        );
    }
}
