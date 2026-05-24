package com.akita.sql.ast;

public record UnaryExpression(
        UnaryOperator operator,
        Expression operand
) implements Expression {
}
