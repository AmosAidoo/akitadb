package com.akita.query.bind;

import com.akita.catalog.Catalog;
import com.akita.catalog.TableMetadata;
import com.akita.datatype.AkitaType;
import com.akita.sql.ast.BinaryExpression;
import com.akita.sql.ast.BinaryOperator;
import com.akita.sql.ast.CreateTableStatement;
import com.akita.sql.ast.Expression;
import com.akita.sql.ast.IdentifierExpression;
import com.akita.sql.ast.LiteralExpression;
import com.akita.sql.ast.QualifiedName;
import com.akita.sql.ast.SelectItem;
import com.akita.sql.ast.SelectStatement;
import com.akita.sql.ast.Statement;
import com.akita.sql.ast.TableRef;
import com.akita.sql.ast.UnaryExpression;

import java.util.ArrayList;
import java.util.List;

public class Binder {
    private final Catalog catalog;

    public Binder(Catalog catalog) {
        this.catalog = catalog;
    }

    public BoundStatement bind(Statement statement) {
        return switch (statement) {
            case SelectStatement select -> bindSelect(select);
            case CreateTableStatement ignored -> throw new BindException("CREATE TABLE is executed directly");
        };
    }

    private BoundSelectStatement bindSelect(SelectStatement statement) {
        BoundTable table = bindTable(statement.from());
        BindingScope scope = new BindingScope(table);

        List<BoundSelectItem> selectItems = new ArrayList<>();
        for (SelectItem item : statement.selectItems()) {
            selectItems.add(new BoundSelectItem(bindExpression(item.expression(), scope), item.alias()));
        }

        BoundExpression where = null;
        if (statement.where() != null) {
            where = bindExpression(statement.where(), scope);
            if (!(where.type() instanceof AkitaType.Boolean)) {
                throw new BindException("WHERE expression must be boolean");
            }
        }

        return new BoundSelectStatement(selectItems, table, where);
    }

    private BoundTable bindTable(TableRef tableRef) {
        String tableName = tableName(tableRef.name());
        TableMetadata table = catalog.getTable(tableName);
        if (table == null) {
            throw new BindException("Unknown table: " + tableName);
        }
        return new BoundTable(table, tableRef.alias());
    }

    private BoundExpression bindExpression(Expression expression, BindingScope scope) {
        return switch (expression) {
            case IdentifierExpression identifier -> bindIdentifier(identifier.name(), scope);
            case LiteralExpression literal -> bindLiteral(literal);
            case BinaryExpression binary -> bindBinary(binary, scope);
            case UnaryExpression unary -> {
                BoundExpression operand = bindExpression(unary.operand(), scope);
                yield new BoundUnaryExpression(unary.operator(), operand, operand.type());
            }
        };
    }

    private BoundExpression bindIdentifier(QualifiedName name, BindingScope scope) {
        List<String> parts = name.parts();
        if (parts.size() == 1) {
            return scope.resolveColumn(null, parts.getFirst());
        }
        if (parts.size() == 2) {
            return scope.resolveColumn(parts.getFirst(), parts.get(1));
        }
        throw new BindException("Column references must be unqualified or table-qualified: " + String.join(".", parts));
    }

    private BoundLiteral bindLiteral(LiteralExpression literal) {
        Object value = literal.value();
        return new BoundLiteral(value, literalType(value));
    }

    private BoundBinaryExpression bindBinary(BinaryExpression binary, BindingScope scope) {
        BoundExpression left = bindExpression(binary.left(), scope);
        BoundExpression right = bindExpression(binary.right(), scope);
        return new BoundBinaryExpression(left, binary.operator(), right, binaryType(binary.operator(), left));
    }

    private static String tableName(QualifiedName name) {
        List<String> parts = name.parts();
        return parts.getLast();
    }

    private static AkitaType literalType(Object value) {
        if (value instanceof Integer) {
            return new AkitaType.Integer();
        }
        if (value instanceof Long) {
            return new AkitaType.BigInt();
        }
        if (value instanceof Double || value instanceof Float) {
            return new AkitaType.Double();
        }
        if (value instanceof String string) {
            return new AkitaType.Varchar(string.length());
        }
        if (value instanceof Boolean) {
            return new AkitaType.Boolean();
        }
        return null;
    }

    private static AkitaType binaryType(BinaryOperator operator, BoundExpression left) {
        return switch (operator) {
            case OR, AND, EQUAL, NOT_EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL ->
                    new AkitaType.Boolean();
            case ADD, SUBTRACT, MULTIPLY, DIVIDE -> left.type();
        };
    }
}
