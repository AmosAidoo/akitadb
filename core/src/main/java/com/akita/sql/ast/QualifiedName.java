package com.akita.sql.ast;

import java.util.List;

public record QualifiedName(List<String> parts) {

    public QualifiedName {
        if (parts.isEmpty()) {
            throw new IllegalArgumentException("qualified name must contain at least one part");
        }
        parts = List.copyOf(parts);
    }

    public static QualifiedName of(String first, String... rest) {
        List<String> parts = new java.util.ArrayList<>(1 + rest.length);
        parts.add(first);
        parts.addAll(List.of(rest));
        return new QualifiedName(parts);
    }
}
