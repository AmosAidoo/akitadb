package com.akita.sql.ast;

public record ColumnDefinition(
        String name,
        SqlTypeName type,
        boolean nullable
) {}
