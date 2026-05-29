package com.akita.query.bind;

import com.akita.datatype.ColumnMetadata;

import java.util.List;

public record BoundInsertStatement(
        BoundTable table,
        List<ColumnMetadata> targetColumns,
        List<List<BoundExpression>> values
) implements BoundStatement {
    public BoundInsertStatement {
        targetColumns = List.copyOf(targetColumns);
        values = values.stream().map(List::copyOf).toList();
    }
}
