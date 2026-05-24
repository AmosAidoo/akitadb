package com.akita.sql.ast;

import java.util.List;
import java.util.Optional;

public record SelectStatement(
        List<SelectItem> selectItems,
        TableRef from,
        Expression where
) implements Statement {

    public SelectStatement {
        selectItems = List.copyOf(selectItems);
    }

    public SelectStatement(List<SelectItem> selectItems, TableRef from) {
        this(selectItems, from, null);
    }

    public Optional<Expression> optionalWhere() {
        return Optional.ofNullable(where);
    }
}
