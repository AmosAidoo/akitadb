package com.akita.sql.ast;

public record LiteralExpression(Object value) implements Expression {

    public static LiteralExpression integer(int value) {
        return new LiteralExpression(value);
    }

    public static LiteralExpression string(String value) {
        return new LiteralExpression(value);
    }

    public static LiteralExpression bool(boolean value) {
        return new LiteralExpression(value);
    }

    public static LiteralExpression nullLiteral() {
        return new LiteralExpression(null);
    }
}
