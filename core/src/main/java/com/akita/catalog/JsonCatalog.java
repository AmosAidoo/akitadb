package com.akita.catalog;

import com.akita.datatype.AkitaType;
import com.akita.datatype.ColumnMetadata;
import com.akita.datatype.Schema;
import com.akita.storage.ContainerId;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

public class JsonCatalog implements Catalog {
    private final Path catalogPath;
    private final Map<String, TableMetadata> tables = new LinkedHashMap<>();

    public JsonCatalog() {
        this.catalogPath = null;
    }

    public JsonCatalog(Path catalogPath) {
        this.catalogPath = catalogPath;
        load();
    }

    @Override
    public synchronized void createTable(TableMetadata table) {
        String key = normalize(table.tableName());
        if (tables.containsKey(key)) {
            throw new IllegalArgumentException("table already exists: " + table.tableName());
        }

        tables.put(key, table);
        persist();
    }

    @Override
    public synchronized void dropTable(TableMetadata table) {
        String key = normalize(table.tableName());
        if (tables.remove(key) == null) {
            throw new IllegalArgumentException("unknown table: " + table.tableName());
        }

        persist();
    }

    @Override
    public synchronized TableMetadata getTable(String tableName) {
        return tables.get(normalize(tableName));
    }

    private static String normalize(String tableName) {
        return tableName.toLowerCase(Locale.ROOT);
    }

    private void load() {
        if (catalogPath == null || !Files.exists(catalogPath)) {
            return;
        }

        try {
            Map<String, Object> root = requireObject(CatalogJsonParser.parse(Files.readString(catalogPath)), "catalog root");
            List<Object> tableObjects = requireArray(root.get("tables"), "tables");
            for (Object tableObject : tableObjects) {
                TableMetadata table = decodeTable(requireObject(tableObject, "table"));
                String key = normalize(table.tableName());
                if (tables.put(key, table) != null) {
                    throw new IllegalArgumentException("duplicate table: " + table.tableName());
                }
            }
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException("invalid catalog file " + catalogPath + ": " + e.getMessage(), e);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to load catalog " + catalogPath, e);
        }
    }

    private static TableMetadata decodeTable(Map<String, Object> object) {
        String tableName = requireString(object.get("tableName"), "tableName");
        ContainerId containerId = ContainerId.fromUUID(UUID.fromString(requireString(object.get("containerId"), "containerId")));
        List<ColumnMetadata> columns = requireArray(object.get("columns"), "columns").stream()
                .map(column -> decodeColumn(requireObject(column, "column")))
                .toList();
        return new TableMetadata(tableName, containerId, new Schema(columns));
    }

    private static ColumnMetadata decodeColumn(Map<String, Object> object) {
        return new ColumnMetadata(
                requireString(object.get("name"), "name"),
                parseType(requireString(object.get("type"), "type")),
                requireInt(object.get("ordinalPosition"), "ordinalPosition"),
                requireBoolean(object.get("nullable"), "nullable")
        );
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> requireObject(Object value, String field) {
        if (value instanceof Map<?, ?> object) {
            return (Map<String, Object>) object;
        }
        throw new IllegalArgumentException(field + " must be an object");
    }

    @SuppressWarnings("unchecked")
    private static List<Object> requireArray(Object value, String field) {
        if (value instanceof List<?> array) {
            return (List<Object>) array;
        }
        throw new IllegalArgumentException(field + " must be an array");
    }

    private static String requireString(Object value, String field) {
        if (value instanceof String string) {
            return string;
        }
        throw new IllegalArgumentException(field + " must be a string");
    }

    private static boolean requireBoolean(Object value, String field) {
        if (value instanceof Boolean bool) {
            return bool;
        }
        throw new IllegalArgumentException(field + " must be a boolean");
    }

    private static int requireInt(Object value, String field) {
        if (value instanceof Long number && number >= Integer.MIN_VALUE && number <= Integer.MAX_VALUE) {
            return number.intValue();
        }
        if (value instanceof Integer number) {
            return number;
        }
        if (value instanceof Double number
                && Double.isFinite(number)
                && number % 1 == 0
                && number >= Integer.MIN_VALUE
                && number <= Integer.MAX_VALUE) {
            return number.intValue();
        }
        throw new IllegalArgumentException(field + " must be an integer");
    }

    private void persist() {
        if (catalogPath == null) {
            return;
        }

        try {
            Path parent = catalogPath.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            Path tempPath = catalogPath.resolveSibling(catalogPath.getFileName() + ".tmp");
            Files.writeString(tempPath, toJson());
            moveIntoPlace(tempPath);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to persist catalog " + catalogPath, e);
        }
    }

    private void moveIntoPlace(Path tempPath) throws IOException {
        try {
            Files.move(tempPath, catalogPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(tempPath, catalogPath, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private String toJson() {
        Map<String, Object> root = new LinkedHashMap<>();
        List<Object> tableObjects = new ArrayList<>();
        for (TableMetadata table : tables.values()) {
            Map<String, Object> tableObject = new LinkedHashMap<>();
            tableObject.put("tableName", table.tableName());
            tableObject.put("containerId", table.containerId().toString());
            final List<Object> columnObjects = getColumnObjects(table);
            tableObject.put("columns", columnObjects);
            tableObjects.add(tableObject);
        }
        root.put("tables", tableObjects);
        return CatalogJsonParser.stringify(root) + "\n";
    }

    private static List<Object> getColumnObjects(TableMetadata table) {
        List<Object> columnObjects = new ArrayList<>();
        for (ColumnMetadata column : table.schema().columns()) {
            Map<String, Object> columnObject = new LinkedHashMap<>();
            columnObject.put("name", column.name());
            columnObject.put("type", formatType(column.type()));
            columnObject.put("ordinalPosition", (long) column.ordinalPosition());
            columnObject.put("nullable", column.nullable());
            columnObjects.add(columnObject);
        }
        return columnObjects;
    }

    private static String formatType(AkitaType type) {
        return switch (type) {
            case AkitaType.Integer ignored -> "INTEGER";
            case AkitaType.BigInt ignored -> "BIGINT";
            case AkitaType.Double ignored -> "DOUBLE";
            case AkitaType.Boolean ignored -> "BOOLEAN";
            case AkitaType.Varchar varchar -> "VARCHAR(" + varchar.maxLength() + ")";
        };
    }

    private static AkitaType parseType(String source) {
        switch (source) {
            case "INTEGER" -> {
                return new AkitaType.Integer();
            }
            case "BIGINT" -> {
                return new AkitaType.BigInt();
            }
            case "DOUBLE" -> {
                return new AkitaType.Double();
            }
            case "BOOLEAN" -> {
                return new AkitaType.Boolean();
            }
        }
        if (source.startsWith("VARCHAR(") && source.endsWith(")")) {
            String maxLength = source.substring("VARCHAR(".length(), source.length() - 1);
            if (maxLength.isEmpty() || !maxLength.chars().allMatch(Character::isDigit)) {
                throw new IllegalArgumentException("unknown catalog type: " + source);
            }
            return new AkitaType.Varchar(Integer.parseInt(maxLength));
        }
        throw new IllegalArgumentException("unknown catalog type: " + source);
    }

}
