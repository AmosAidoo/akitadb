package com.akita.query.physical;

import com.akita.datatype.Schema;
import com.akita.query.bind.BoundExpression;

public record FilterPlan(
        PhysicalPlan input,
        BoundExpression predicate
) implements PhysicalPlan {

    @Override
    public Schema outputSchema() {
        return input.outputSchema();
    }
}
