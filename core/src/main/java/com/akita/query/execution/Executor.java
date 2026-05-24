package com.akita.query.execution;

import java.util.Optional;

public interface Executor {
    Optional<Row> next() throws Exception;
}
