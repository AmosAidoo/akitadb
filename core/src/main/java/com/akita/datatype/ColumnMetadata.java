package com.akita.datatype;

public record ColumnMetadata(
        String name,
        AkitaType type,
        int ordinalPosition,
        boolean nullable
) {}
