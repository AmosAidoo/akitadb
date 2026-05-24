package com.akita.query.logical;

import com.akita.datatype.Schema;
import com.akita.query.bind.BoundTable;

public record LogicalScan(
        BoundTable table
) implements LogicalPlan {

    @Override
    public Schema outputSchema() {
        return table.metadata().schema();
    }
}
