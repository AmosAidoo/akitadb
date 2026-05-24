package com.akita.query.bind;

import com.akita.catalog.TableMetadata;

import java.util.Optional;

public record BoundTable(
        TableMetadata metadata,
        String alias
) {
    public Optional<String> optionalAlias() {
        return Optional.ofNullable(alias);
    }

    public String rangeName() {
        return alias == null ? metadata.tableName() : alias;
    }
}
