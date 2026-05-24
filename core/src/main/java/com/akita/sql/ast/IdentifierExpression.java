package com.akita.sql.ast;

public record IdentifierExpression(QualifiedName name) implements Expression {
}
