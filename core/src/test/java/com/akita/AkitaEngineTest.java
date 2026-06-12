package com.akita;

import com.akita.datatype.AkitaType;
import com.akita.datatype.AkitaValue;
import com.akita.datatype.ColumnMetadata;
import com.akita.query.QueryResult;
import com.akita.query.execution.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class AkitaEngineTest {

    @Test
    void opensDatabaseDirectoryAndCreatesEmptyCatalog(@TempDir Path tempDir) throws Exception {
        Path dataDirectory = tempDir.resolve("akita-data");

        try (AkitaEngine engine = AkitaEngine.open(dataDirectory)) {
            assertThat(engine.dataDirectory()).isEqualTo(dataDirectory);
            assertThat(engine.queryEngine()).isNotNull();
        }

        assertThat(Files.readString(dataDirectory.resolve(AkitaEngine.CATALOG_FILE)))
                .isEqualTo("{\"tables\":[]}\n");
    }

    @Test
    void queryEngineUsesFacadeOwnedStorageEndToEnd(@TempDir Path dataDirectory) {
        try (AkitaEngine engine = AkitaEngine.open(dataDirectory)) {
            engine.queryEngine().execute("""
                    CREATE TABLE users (
                        id INTEGER NOT NULL,
                        name VARCHAR(32) NOT NULL
                    )
                    """);
            engine.queryEngine().execute("INSERT INTO users VALUES (1, 'Ada')");

            QueryResult result = engine.queryEngine().execute("SELECT id, name FROM users");

            assertThat(result.schema().columns()).containsExactly(
                    new ColumnMetadata("id", new AkitaType.Integer(), 0, true),
                    new ColumnMetadata("name", new AkitaType.Varchar(32), 1, true)
            );
            assertThat(result.rows()).containsExactly(
                    Row.of(new AkitaValue.IntVal(1), new AkitaValue.VarcharVal("Ada"))
            );
        }
    }
}
