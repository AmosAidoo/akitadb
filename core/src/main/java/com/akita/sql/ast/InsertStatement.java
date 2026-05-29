package com.akita.sql.ast;

import java.util.List;

public record InsertStatement(
        QualifiedName tableName,
        List<String> columns,
        List<List<Expression>> values
) implements Statement {
    public InsertStatement {
        columns = List.copyOf(columns);
        values = values.stream().map(List::copyOf).toList();
    }
}
