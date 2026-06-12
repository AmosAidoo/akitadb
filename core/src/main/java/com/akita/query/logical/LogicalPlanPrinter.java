package com.akita.query.logical;

import com.akita.datatype.ColumnMetadata;
import com.akita.datatype.AkitaTypes;
import com.akita.query.bind.BoundBinaryExpression;
import com.akita.query.bind.BoundColumnReference;
import com.akita.query.bind.BoundExpression;
import com.akita.query.bind.BoundLiteral;
import com.akita.query.bind.BoundSelectItem;
import com.akita.query.bind.BoundUnaryExpression;

import java.util.stream.Collectors;

public final class LogicalPlanPrinter {
    private static final String INDENT = "  ";

    private LogicalPlanPrinter() {
    }

    public static String print(LogicalPlan plan) {
        StringBuilder builder = new StringBuilder();
        appendPlan(builder, plan, 0);
        return builder.toString();
    }

    private static void appendPlan(StringBuilder builder, LogicalPlan plan, int depth) {
        indent(builder, depth);
        switch (plan) {
            case LogicalProjection projection -> {
                builder.append("Projection[")
                        .append(projection.selectItems().stream()
                                .map(LogicalPlanPrinter::formatSelectItem)
                                .collect(Collectors.joining(", ")))
                        .append("] output=")
                        .append(formatSchema(projection))
                        .append(System.lineSeparator());
                appendPlan(builder, projection.input(), depth + 1);
            }
            case LogicalFilter filter -> {
                builder.append("Filter[predicate=")
                        .append(formatExpression(filter.predicate()))
                        .append("] output=")
                        .append(formatSchema(filter))
                        .append(System.lineSeparator());
                appendPlan(builder, filter.input(), depth + 1);
            }
            case LogicalScan scan -> builder.append("Scan[table=")
                    .append(scan.table().metadata().tableName())
                    .append("] output=")
                    .append(formatSchema(scan))
                    .append(System.lineSeparator());
            case LogicalInsert insert -> builder.append("Insert[table=")
                    .append(insert.statement().table().metadata().tableName())
                    .append("] output=")
                    .append(formatSchema(insert))
                    .append(System.lineSeparator());
        }
    }

    private static String formatSelectItem(BoundSelectItem item) {
        String formatted = formatExpression(item.expression());
        return item.optionalAlias()
                .map(alias -> formatted + " AS " + alias)
                .orElse(formatted);
    }

    private static String formatExpression(BoundExpression expression) {
        return switch (expression) {
            case BoundColumnReference column -> column.columnName();
            case BoundLiteral literal -> String.valueOf(literal.value());
            case BoundUnaryExpression unary -> unary.operator() + "(" + formatExpression(unary.operand()) + ")";
            case BoundBinaryExpression binary -> "("
                    + formatExpression(binary.left())
                    + " "
                    + binary.operator()
                    + " "
                    + formatExpression(binary.right())
                    + ")";
        };
    }

    private static String formatSchema(LogicalPlan plan) {
        return plan.outputSchema().columns().stream()
                .map(LogicalPlanPrinter::formatColumn)
                .collect(Collectors.joining(", ", "[", "]"));
    }

    private static String formatColumn(ColumnMetadata column) {
        return column.name() + ":" + AkitaTypes.format(column.type());
    }

    private static void indent(StringBuilder builder, int depth) {
        builder.append(INDENT.repeat(depth));
    }
}
