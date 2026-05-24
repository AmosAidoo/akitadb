package com.akita.query.logical;

import com.akita.datatype.Schema;
import com.akita.query.bind.BoundSelectItem;

import java.util.List;

public record LogicalProjection(
        LogicalPlan input,
        List<BoundSelectItem> selectItems,
        Schema outputSchema
) implements LogicalPlan {

    public LogicalProjection {
        selectItems = List.copyOf(selectItems);
    }
}
