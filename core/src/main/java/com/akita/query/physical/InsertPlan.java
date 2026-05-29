package com.akita.query.physical;

import com.akita.datatype.Schema;
import com.akita.query.bind.BoundInsertStatement;

import java.util.List;

public record InsertPlan(
        BoundInsertStatement statement
) implements PhysicalPlan {
    @Override
    public Schema outputSchema() {
        return new Schema(List.of());
    }
}
