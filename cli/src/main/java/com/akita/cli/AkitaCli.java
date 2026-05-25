package com.akita.cli;

import com.akita.datatype.AkitaValue;
import com.akita.datatype.ColumnMetadata;
import com.akita.query.QueryException;
import com.akita.query.QueryResult;
import com.akita.query.execution.Row;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public final class AkitaCli {
    private AkitaCli() {
    }

    public static void main(String[] args) {
        int exitCode = run(args, System.in, System.out, System.err);
        if (exitCode != 0) {
            System.exit(exitCode);
        }
    }

    public static int run(String[] args, InputStream input, PrintStream output, PrintStream error) {
        if (args.length < 1 || args.length > 2 || "--help".equals(args[0]) || "-h".equals(args[0])) {
            printUsage(error);
            return args.length == 1 && ("--help".equals(args[0]) || "-h".equals(args[0])) ? 0 : 2;
        }

        Path dataDirectory = Path.of(args[0]);
        try (AkitaDatabase database = AkitaDatabase.open(dataDirectory)) {
            if (args.length == 2) {
                return executeSql(database, args[1], output, error);
            }
            return repl(database, input, output, error);
        } catch (UncheckedIOException | IllegalStateException e) {
            error.println("error: " + e.getMessage());
            return 1;
        }
    }

    private static int repl(AkitaDatabase database, InputStream input, PrintStream output, PrintStream error) {
        output.println("Akita shell");
        output.println("Connected to " + database.dataDirectory().toAbsolutePath());
        output.println("Enter SQL terminated by ';'. Use .exit or .quit to leave.");

        BufferedReader reader = new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8));
        StringBuilder sql = new StringBuilder();
        while (true) {
            output.print(sql.isEmpty() ? "akita> " : "   ...> ");
            output.flush();

            String line;
            try {
                line = reader.readLine();
            } catch (IOException e) {
                throw new UncheckedIOException("failed to read terminal input", e);
            }
            if (line == null) {
                output.println();
                return 0;
            }

            String trimmed = line.trim();
            if (sql.isEmpty() && (".exit".equals(trimmed) || ".quit".equals(trimmed))) {
                return 0;
            }
            if (trimmed.isEmpty() && sql.isEmpty()) {
                continue;
            }

            sql.append(line).append('\n');
            if (trimmed.endsWith(";")) {
                executeSql(database, stripTrailingSemicolon(sql.toString()), output, error);
                sql.setLength(0);
            }
        }
    }

    private static int executeSql(AkitaDatabase database, String sql, PrintStream output, PrintStream error) {
        try {
            QueryResult result = database.queryEngine().execute(stripTrailingSemicolon(sql));
            printResult(result, output);
            return 0;
        } catch (QueryException e) {
            error.println("error [" + e.kind().name().toLowerCase() + "]: " + e.getMessage());
            return 1;
        } catch (RuntimeException e) {
            error.println("error: " + e.getMessage());
            return 1;
        }
    }

    private static void printResult(QueryResult result, PrintStream output) {
        List<List<String>> rows = new ArrayList<>();
        List<String> header = result.schema().columns().stream()
                .map(ColumnMetadata::name)
                .toList();
        rows.add(header);
        for (Row row : result.rows()) {
            List<String> values = new ArrayList<>();
            for (AkitaValue value : row) {
                values.add(formatValue(value));
            }
            rows.add(values);
        }

        int columnCount = header.size();
        int[] widths = new int[columnCount];
        for (List<String> row : rows) {
            for (int i = 0; i < columnCount; i++) {
                widths[i] = Math.max(widths[i], row.get(i).length());
            }
        }

        printRow(header, widths, output);
        printSeparator(widths, output);
        for (int i = 1; i < rows.size(); i++) {
            printRow(rows.get(i), widths, output);
        }
        output.println("(" + result.rows().size() + " row" + (result.rows().size() == 1 ? "" : "s") + ")");
    }

    private static void printRow(List<String> values, int[] widths, PrintStream output) {
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) {
                output.print(" | ");
            }
            output.print(pad(values.get(i), widths[i]));
        }
        output.println();
    }

    private static void printSeparator(int[] widths, PrintStream output) {
        for (int i = 0; i < widths.length; i++) {
            if (i > 0) {
                output.print("-+-");
            }
            output.print("-".repeat(widths[i]));
        }
        output.println();
    }

    private static String pad(String value, int width) {
        return value + " ".repeat(width - value.length());
    }

    private static String formatValue(AkitaValue value) {
        return switch (value) {
            case AkitaValue.IntVal intVal -> Integer.toString(intVal.value());
            case AkitaValue.BigIntVal bigIntVal -> Long.toString(bigIntVal.value());
            case AkitaValue.DoubleVal doubleVal -> Double.toString(doubleVal.value());
            case AkitaValue.VarcharVal varcharVal -> varcharVal.value();
            case AkitaValue.BoolVal boolVal -> Boolean.toString(boolVal.value());
            case AkitaValue.Null ignored -> "NULL";
        };
    }

    private static String stripTrailingSemicolon(String sql) {
        String trimmed = sql.trim();
        if (trimmed.endsWith(";")) {
            return trimmed.substring(0, trimmed.length() - 1);
        }
        return trimmed;
    }

    private static void printUsage(PrintStream error) {
        error.println("usage:");
        error.println("  akita <data-directory>");
        error.println("  akita <data-directory> \"SELECT ...;\"");
    }
}
