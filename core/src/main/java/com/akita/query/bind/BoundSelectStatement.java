package com.akita.query.bind;

import java.util.List;
import java.util.Optional;

public record BoundSelectStatement(
        List<BoundSelectItem> selectItems,
        BoundTable table,
        BoundExpression where
) implements BoundStatement {

    public BoundSelectStatement {
        selectItems = List.copyOf(selectItems);
    }

    public Optional<BoundExpression> optionalWhere() {
        return Optional.ofNullable(where);
    }
}
