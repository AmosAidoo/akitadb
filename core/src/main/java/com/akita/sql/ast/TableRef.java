package com.akita.sql.ast;

import java.util.Optional;

public record TableRef(
        QualifiedName name,
        String alias
) {

    public TableRef(QualifiedName name) {
        this(name, null);
    }

    public Optional<String> optionalAlias() {
        return Optional.ofNullable(alias);
    }
}
