package com.akita.query.physical;

import com.akita.datatype.Schema;
import com.akita.query.bind.BoundTable;

public record SeqScanPlan(
        BoundTable table
) implements PhysicalPlan {

    @Override
    public Schema outputSchema() {
        return table.metadata().schema();
    }
}
