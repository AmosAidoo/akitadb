package com.akita.sql.ast;

import java.util.Optional;

public record SelectItem(
        Expression expression,
        String alias
) {

    public SelectItem(Expression expression) {
        this(expression, null);
    }

    public Optional<String> optionalAlias() {
        return Optional.ofNullable(alias);
    }
}
