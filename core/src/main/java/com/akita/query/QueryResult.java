package com.akita.query;

import com.akita.datatype.Schema;
import com.akita.query.execution.Row;

import java.util.List;

public record QueryResult(
        Schema schema,
        List<Row> rows
) {
    public QueryResult {
        rows = List.copyOf(rows);
    }
}
