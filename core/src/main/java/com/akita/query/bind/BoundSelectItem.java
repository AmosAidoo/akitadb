package com.akita.query.bind;

import java.util.Optional;

public record BoundSelectItem(
        BoundExpression expression,
        String alias
) {
    public Optional<String> optionalAlias() {
        return Optional.ofNullable(alias);
    }
}
