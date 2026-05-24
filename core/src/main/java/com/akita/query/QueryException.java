package com.akita.query;

public class QueryException extends RuntimeException {
    private final Kind kind;

    public QueryException(Kind kind, String message, Throwable cause) {
        super(message, cause);
        this.kind = kind;
    }

    public Kind kind() {
        return kind;
    }

    public enum Kind {
        PARSE,
        BIND,
        EXECUTION
    }
}
