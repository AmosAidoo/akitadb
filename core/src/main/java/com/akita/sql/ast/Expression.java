package com.akita.sql.ast;

public sealed interface Expression permits BinaryExpression, IdentifierExpression, LiteralExpression, UnaryExpression {
}
