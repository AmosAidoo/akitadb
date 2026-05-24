package com.akita.query.physical;

import com.akita.datatype.Schema;
import com.akita.query.bind.BoundSelectItem;

import java.util.List;

public record ProjectionPlan(
        PhysicalPlan input,
        List<BoundSelectItem> selectItems,
        Schema outputSchema
) implements PhysicalPlan {

    public ProjectionPlan {
        selectItems = List.copyOf(selectItems);
    }
}
