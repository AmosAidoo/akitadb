package com.akita.query.bind;

import com.akita.catalog.TableMetadata;
import com.akita.datatype.AkitaType;

public record BoundColumnReference(
        TableMetadata table,
        String columnName,
        int ordinalPosition,
        AkitaType type
) implements BoundExpression {
}
