package com.akita.query;

import com.akita.datatype.Schema;
import com.akita.query.execution.Row;

import java.util.Optional;

public interface QueryCursor extends AutoCloseable {
    Schema schema();

    Optional<Row> next();

    @Override
    void close();
}
