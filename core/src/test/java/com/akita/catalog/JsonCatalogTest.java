package com.akita.catalog;

import com.akita.datatype.AkitaType;
import com.akita.datatype.ColumnMetadata;
import com.akita.datatype.Schema;
import com.akita.storage.ContainerId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JsonCatalogTest {

    @TempDir
    Path tempDir;

    @Test
    void storesTablesInMemoryCaseInsensitively() {
        JsonCatalog catalog = new JsonCatalog();
        TableMetadata users = usersTable();

        catalog.createTable(users);

        assertThat(catalog.getTable("USERS")).isEqualTo(users);
    }

    @Test
    void persistsTablesToJsonFile() {
        Path path = tempDir.resolve("catalog.json");
        TableMetadata users = usersTable();

        new JsonCatalog(path).createTable(users);

        JsonCatalog reloaded = new JsonCatalog(path);
        assertThat(reloaded.getTable("users")).isEqualTo(users);
    }

    @Test
    void loadsCatalogWithReorderedFieldsAndEscapedNames() throws Exception {
        Path path = tempDir.resolve("catalog.json");
        Files.writeString(path, """
                {
                  "tables": [
                    {
                      "columns": [
                        {
                          "nullable": false,
                          "ordinalPosition": 0,
                          "type": "VARCHAR(64)",
                          "name": "line\\nbreak\\u005fcol"
                        }
                      ],
                      "containerId": "00000000-0000-0000-0000-000000000002",
                      "tableName": "order_{status}"
                    }
                  ]
                }
                """);

        JsonCatalog catalog = new JsonCatalog(path);

        TableMetadata table = catalog.getTable("ORDER_{STATUS}");
        assertThat(table.tableName()).isEqualTo("order_{status}");
        assertThat(table.schema().columns().getFirst().name()).isEqualTo("line\nbreak_col");
    }

    @Test
    void rejectsMalformedCatalogFiles() throws Exception {
        Path path = tempDir.resolve("catalog.json");
        Files.writeString(path, "{\"tables\":[{\"tableName\":\"users\"}]}");

        assertThatThrownBy(() -> new JsonCatalog(path))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("invalid catalog file")
                .hasMessageContaining("containerId must be a string");
    }

    @Test
    void rejectsDuplicateTablesAndMissingDrops() {
        JsonCatalog catalog = new JsonCatalog();
        TableMetadata users = usersTable();

        catalog.createTable(users);

        assertThatThrownBy(() -> catalog.createTable(users))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("table already exists: users");
        assertThatThrownBy(() -> catalog.dropTable(new TableMetadata("missing", ContainerId.generate(), new Schema(List.of()))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("unknown table: missing");
    }

    private static TableMetadata usersTable() {
        return new TableMetadata(
                "users",
                ContainerId.fromUUID(java.util.UUID.fromString("00000000-0000-0000-0000-000000000001")),
                new Schema(List.of(
                        new ColumnMetadata("id", new AkitaType.Integer(), 0, false),
                        new ColumnMetadata("name", new AkitaType.Varchar(255), 1, false),
                        new ColumnMetadata("active", new AkitaType.Boolean(), 2, false)
                ))
        );
    }
}
