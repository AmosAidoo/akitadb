package com.akita.query.logical;

import com.akita.datatype.Schema;
import com.akita.query.bind.BoundExpression;

public record LogicalFilter(
        LogicalPlan input,
        BoundExpression predicate
) implements LogicalPlan {

    @Override
    public Schema outputSchema() {
        return input.outputSchema();
    }
}
