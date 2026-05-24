package com.akita.query;

import com.akita.datatype.Schema;
import com.akita.query.execution.Executor;
import com.akita.query.execution.Row;

import java.util.Optional;

final class ExecutorQueryCursor implements QueryCursor {
    private final Schema schema;
    private final Executor executor;
    private boolean closed;

    ExecutorQueryCursor(Schema schema, Executor executor) {
        this.schema = schema;
        this.executor = executor;
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public Optional<Row> next() {
        if (closed) {
            return Optional.empty();
        }
        try {
            return executor.next();
        } catch (Exception exception) {
            throw new QueryException(QueryException.Kind.EXECUTION, exception.getMessage(), exception);
        }
    }

    @Override
    public void close() {
        closed = true;
    }
}
