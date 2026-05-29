package com.akita.query.logical;

import com.akita.datatype.ColumnMetadata;
import com.akita.datatype.Schema;
import com.akita.query.bind.BoundColumnReference;
import com.akita.query.bind.BoundExpression;
import com.akita.query.bind.BoundInsertStatement;
import com.akita.query.bind.BoundLiteral;
import com.akita.query.bind.BoundSelectItem;
import com.akita.query.bind.BoundSelectStatement;
import com.akita.query.bind.BoundStatement;

import java.util.ArrayList;
import java.util.List;

public class LogicalPlanner {

    public LogicalPlan plan(BoundStatement statement) {
        return switch (statement) {
            case BoundInsertStatement insert -> new LogicalInsert(insert);
            case BoundSelectStatement select -> planSelect(select);
        };
    }

    private LogicalPlan planSelect(BoundSelectStatement statement) {
        LogicalPlan plan = new LogicalScan(statement.table());
        if (statement.where() != null) {
            plan = new LogicalFilter(plan, statement.where());
        }
        return new LogicalProjection(plan, statement.selectItems(), projectionSchema(statement.selectItems()));
    }

    private static Schema projectionSchema(List<BoundSelectItem> selectItems) {
        List<ColumnMetadata> columns = new ArrayList<>();
        for (int i = 0; i < selectItems.size(); i++) {
            BoundSelectItem item = selectItems.get(i);
            columns.add(new ColumnMetadata(outputName(item, i), item.expression().type(), i, true));
        }
        return new Schema(columns);
    }

    private static String outputName(BoundSelectItem item, int ordinalPosition) {
        return item.optionalAlias().orElseGet(() -> expressionName(item.expression(), ordinalPosition));
    }

    private static String expressionName(BoundExpression expression, int ordinalPosition) {
        return switch (expression) {
            case BoundColumnReference column -> column.columnName();
            case BoundLiteral ignored -> "literal" + ordinalPosition;
            default -> "expr" + ordinalPosition;
        };
    }
}
