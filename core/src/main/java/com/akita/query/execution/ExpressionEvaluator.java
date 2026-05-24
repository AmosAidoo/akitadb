package com.akita.query.execution;

import com.akita.datatype.AkitaType;
import com.akita.datatype.AkitaValue;
import com.akita.datatype.AkitaValueComparator;
import com.akita.query.bind.BoundBinaryExpression;
import com.akita.query.bind.BoundColumnReference;
import com.akita.query.bind.BoundExpression;
import com.akita.query.bind.BoundLiteral;
import com.akita.query.bind.BoundSelectItem;
import com.akita.query.bind.BoundUnaryExpression;
import com.akita.sql.ast.BinaryOperator;
import com.akita.sql.ast.UnaryOperator;

import java.util.List;

public class ExpressionEvaluator {

    public AkitaValue evaluate(BoundExpression expression, Row row) {
        return switch (expression) {
            case BoundColumnReference column -> row.get(column.ordinalPosition());
            case BoundLiteral literal -> literalValue(literal);
            case BoundBinaryExpression binary -> evaluateBinary(binary, row);
            case BoundUnaryExpression unary -> evaluateUnary(unary, row);
        };
    }

    public Row project(List<BoundSelectItem> selectItems, Row row) {
        AkitaValue[] values = new AkitaValue[selectItems.size()];
        for (int i = 0; i < selectItems.size(); i++) {
            values[i] = evaluate(selectItems.get(i).expression(), row);
        }
        return new Row(values);
    }

    private AkitaValue evaluateBinary(BoundBinaryExpression expression, Row row) {
        if (expression.operator() == BinaryOperator.AND) {
            return new AkitaValue.BoolVal(asBoolean(evaluate(expression.left(), row))
                    && asBoolean(evaluate(expression.right(), row)));
        }
        if (expression.operator() == BinaryOperator.OR) {
            return new AkitaValue.BoolVal(asBoolean(evaluate(expression.left(), row))
                    || asBoolean(evaluate(expression.right(), row)));
        }

        AkitaValue left = requireNonNull(evaluate(expression.left(), row));
        AkitaValue right = requireNonNull(evaluate(expression.right(), row));

        return switch (expression.operator()) {
            case EQUAL -> new AkitaValue.BoolVal(AkitaValueComparator.INSTANCE.compare(left, right) == 0);
            case NOT_EQUAL -> new AkitaValue.BoolVal(AkitaValueComparator.INSTANCE.compare(left, right) != 0);
            case LESS_THAN -> new AkitaValue.BoolVal(AkitaValueComparator.INSTANCE.compare(left, right) < 0);
            case LESS_THAN_OR_EQUAL -> new AkitaValue.BoolVal(AkitaValueComparator.INSTANCE.compare(left, right) <= 0);
            case GREATER_THAN -> new AkitaValue.BoolVal(AkitaValueComparator.INSTANCE.compare(left, right) > 0);
            case GREATER_THAN_OR_EQUAL -> new AkitaValue.BoolVal(AkitaValueComparator.INSTANCE.compare(left, right) >= 0);
            case ADD, SUBTRACT, MULTIPLY, DIVIDE ->
                    throw new ExpressionEvaluationException("Arithmetic expressions are not executable yet");
            case AND, OR -> throw new IllegalStateException("Boolean operator should have been handled earlier");
        };
    }

    private AkitaValue evaluateUnary(BoundUnaryExpression expression, Row row) {
        AkitaValue operand = requireNonNull(evaluate(expression.operand(), row));
        if (expression.operator() == UnaryOperator.PLUS) {
            return operand;
        }
        if (expression.operator() == UnaryOperator.MINUS) {
            return switch (operand) {
                case AkitaValue.IntVal(var value) -> new AkitaValue.IntVal(-value);
                case AkitaValue.BigIntVal(var value) -> new AkitaValue.BigIntVal(-value);
                case AkitaValue.DoubleVal(var value) -> new AkitaValue.DoubleVal(-value);
                default -> throw new ExpressionEvaluationException(
                        "Unary minus requires a numeric value, got " + operand.getClass().getSimpleName());
            };
        }
        throw new ExpressionEvaluationException("Unsupported unary operator: " + expression.operator());
    }

    private static AkitaValue literalValue(BoundLiteral literal) {
        Object value = literal.value();
        if (value == null) {
            return new AkitaValue.Null();
        }
        if (literal.type() instanceof AkitaType.Integer && value instanceof Integer integer) {
            return new AkitaValue.IntVal(integer);
        }
        if (literal.type() instanceof AkitaType.BigInt && value instanceof Long bigint) {
            return new AkitaValue.BigIntVal(bigint);
        }
        if (literal.type() instanceof AkitaType.Double && value instanceof Number number) {
            return new AkitaValue.DoubleVal(number.doubleValue());
        }
        if (literal.type() instanceof AkitaType.Varchar && value instanceof String string) {
            return new AkitaValue.VarcharVal(string);
        }
        if (literal.type() instanceof AkitaType.Boolean && value instanceof Boolean bool) {
            return new AkitaValue.BoolVal(bool);
        }
        throw new ExpressionEvaluationException("Literal value does not match bound type");
    }

    private static boolean asBoolean(AkitaValue value) {
        return switch (requireNonNull(value)) {
            case AkitaValue.BoolVal(var bool) -> bool;
            default -> throw new ExpressionEvaluationException(
                    "Boolean expression requires BoolVal, got " + value.getClass().getSimpleName());
        };
    }

    private static AkitaValue requireNonNull(AkitaValue value) {
        if (value instanceof AkitaValue.Null) {
            throw new ExpressionEvaluationException("Null expression evaluation is not supported yet");
        }
        return value;
    }
}
