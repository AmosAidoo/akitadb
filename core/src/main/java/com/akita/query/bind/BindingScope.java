package com.akita.query.bind;

import com.akita.catalog.TableMetadata;
import com.akita.datatype.ColumnMetadata;

final class BindingScope {
    private final BoundTable table;

    BindingScope(BoundTable table) {
        this.table = table;
    }

    BoundColumnReference resolveColumn(String qualifier, String columnName) {
        if (qualifier != null && !matchesRangeName(qualifier)) {
            throw new BindException("Unknown table qualifier: " + qualifier);
        }

        TableMetadata metadata = table.metadata();
        for (ColumnMetadata column : metadata.schema().columns()) {
            if (column.name().equalsIgnoreCase(columnName)) {
                return new BoundColumnReference(
                        metadata,
                        column.name(),
                        column.ordinalPosition(),
                        column.type()
                );
            }
        }

        throw new BindException("Unknown column: " + columnName);
    }

    private boolean matchesRangeName(String qualifier) {
        return table.rangeName().equalsIgnoreCase(qualifier)
                || table.metadata().tableName().equalsIgnoreCase(qualifier);
    }
}
