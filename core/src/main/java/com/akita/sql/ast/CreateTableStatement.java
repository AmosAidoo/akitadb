package com.akita.sql.ast;

import java.util.List;

public record CreateTableStatement(
        QualifiedName tableName,
        List<ColumnDefinition> columns
) implements Statement {

    public CreateTableStatement {
        columns = List.copyOf(columns);
    }
}
