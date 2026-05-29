package com.akita.query.logical;

import com.akita.datatype.Schema;
import com.akita.query.bind.BoundInsertStatement;

import java.util.List;

public record LogicalInsert(
        BoundInsertStatement statement
) implements LogicalPlan {
    @Override
    public Schema outputSchema() {
        return new Schema(List.of());
    }
}
