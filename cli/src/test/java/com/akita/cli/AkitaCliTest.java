package com.akita.cli;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class AkitaCliTest {
    @TempDir
    Path tempDir;

    @Test
    void initializesMissingDatabaseDirectoryForInteractiveShell() throws Exception {
        Path dataDirectory = tempDir.resolve("akita-data");
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        ByteArrayOutputStream error = new ByteArrayOutputStream();

        int exitCode = AkitaCli.run(
                new String[]{dataDirectory.toString()},
                input(".exit\n"),
                printStream(output),
                printStream(error)
        );

        assertThat(exitCode).isZero();
        assertThat(Files.readString(dataDirectory.resolve(AkitaDatabase.CATALOG_FILE)))
                .isEqualTo("{\"tables\":[]}\n");
        assertThat(output.toString(StandardCharsets.UTF_8))
                .contains("Akita shell")
                .contains("akita> ");
        assertThat(error.toString(StandardCharsets.UTF_8)).isEmpty();
    }

    @Test
    void reusesExistingDatabaseDirectory() throws Exception {
        Path dataDirectory = tempDir.resolve("akita-data");
        Files.createDirectories(dataDirectory);
        Files.writeString(dataDirectory.resolve(AkitaDatabase.CATALOG_FILE), "{\"tables\":[]}\n");

        ByteArrayOutputStream error = new ByteArrayOutputStream();
        int exitCode = AkitaCli.run(
                new String[]{dataDirectory.toString()},
                input(".quit\n"),
                printStream(new ByteArrayOutputStream()),
                printStream(error)
        );

        assertThat(exitCode).isZero();
        assertThat(Files.readString(dataDirectory.resolve(AkitaDatabase.CATALOG_FILE)))
                .isEqualTo("{\"tables\":[]}\n");
        assertThat(error.toString(StandardCharsets.UTF_8)).isEmpty();
    }

    @Test
    void oneShotQueryReportsQueryFacadeErrors() {
        Path dataDirectory = tempDir.resolve("akita-data");
        ByteArrayOutputStream error = new ByteArrayOutputStream();

        int exitCode = AkitaCli.run(
                new String[]{dataDirectory.toString(), "SELECT id FROM missing;"},
                input(""),
                printStream(new ByteArrayOutputStream()),
                printStream(error)
        );

        assertThat(exitCode).isEqualTo(1);
        assertThat(error.toString(StandardCharsets.UTF_8))
                .contains("error [bind]: Unknown table: missing");
    }

    @Test
    void interactiveShellExecutesStatementWhenSemicolonArrives() {
        Path dataDirectory = tempDir.resolve("akita-data");
        ByteArrayOutputStream error = new ByteArrayOutputStream();

        int exitCode = AkitaCli.run(
                new String[]{dataDirectory.toString()},
                input("SELECT FROM missing;\n.exit\n"),
                printStream(new ByteArrayOutputStream()),
                printStream(error)
        );

        assertThat(exitCode).isZero();
        assertThat(error.toString(StandardCharsets.UTF_8))
                .contains("error [parse]: Expected expression");
    }

    private static ByteArrayInputStream input(String source) {
        return new ByteArrayInputStream(source.getBytes(StandardCharsets.UTF_8));
    }

    private static PrintStream printStream(ByteArrayOutputStream output) {
        return new PrintStream(output, true, StandardCharsets.UTF_8);
    }
}
