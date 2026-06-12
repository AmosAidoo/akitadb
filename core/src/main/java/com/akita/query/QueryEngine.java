package com.akita.query;

import com.akita.buffer.BufferPoolManager;
import com.akita.catalog.Catalog;
import com.akita.datatype.Schema;
import com.akita.query.bind.BindException;
import com.akita.query.bind.Binder;
import com.akita.query.bind.BoundStatement;
import com.akita.sql.ast.CreateTableStatement;
import com.akita.query.execution.ExecutionContext;
import com.akita.query.execution.Executor;
import com.akita.query.execution.Row;
import com.akita.query.logical.LogicalPlan;
import com.akita.query.logical.LogicalPlanner;
import com.akita.query.optimizer.PhysicalPlanner;
import com.akita.query.physical.PhysicalPlan;
import com.akita.query.storage.HeapTableAccess;
import com.akita.query.storage.TableAccess;
import com.akita.query.storage.TableManager;
import com.akita.sql.ast.Statement;
import com.akita.sql.parser.ParseException;
import com.akita.sql.parser.Parser;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class QueryEngine {
    private final Catalog catalog;
    private final TableManager tableManager;
    private final TableAccess tableAccess;
    private final LogicalPlanner logicalPlanner;
    private final PhysicalPlanner physicalPlanner;

    public QueryEngine(Catalog catalog, BufferPoolManager bufferPoolManager) {
        this(catalog, new TableManager(catalog, bufferPoolManager), new HeapTableAccess(bufferPoolManager));
    }

    public QueryEngine(Catalog catalog, TableManager tableManager, TableAccess tableAccess) {
        this(catalog, tableManager, tableAccess, new LogicalPlanner(), new PhysicalPlanner());
    }

    QueryEngine(
            Catalog catalog,
            TableManager tableManager,
            TableAccess tableAccess,
            LogicalPlanner logicalPlanner,
            PhysicalPlanner physicalPlanner
    ) {
        this.catalog = catalog;
        this.tableManager = tableManager;
        this.tableAccess = tableAccess;
        this.logicalPlanner = logicalPlanner;
        this.physicalPlanner = physicalPlanner;
    }

    /**
     * Executes a SQL statement and materializes the full result set in memory.
     * <p>
     * This is a convenience API for tests, small manual queries, and early CLI
     * usage where collecting all rows before returning is acceptable. Long-lived
     * callers, server integrations, and protocol adapters should prefer
     * {@link #query(String)} so rows can be consumed incrementally.
     *
     * @param sql SQL text to parse, bind, plan, and execute
     * @return result schema and all rows produced by the query
     * @throws QueryException when parsing, binding, planning, or execution fails
     */
    public QueryResult execute(String sql) {
        try (QueryCursor cursor = query(sql)) {
            return new QueryResult(cursor.schema(), collectRows(cursor));
        }
    }

    /**
     * Executes a SQL statement and returns a cursor over the result rows.
     * <p>
     * This is the primary query API for surfaces that should stream results,
     * such as a future Akita server, PostgreSQL wire protocol adapter, or CLI
     * mode that prints rows as they are produced. The returned cursor exposes
     * output schema immediately and advances the executor tree one row at a
     * time through {@link QueryCursor#next()}.
     * <p>
     * Callers should close the cursor when they are done. The current cursor
     * implementation has no external resources to release yet, but keeping the
     * lifecycle explicit gives future scans, sessions, and transactions a clear
     * cleanup point.
     *
     * @param sql SQL text to parse, bind, plan, and execute
     * @return cursor exposing result schema and iterator-style row access
     * @throws QueryException when parsing, binding, planning, or executor setup fails;
     *                        execution failures while reading rows are surfaced by
     *                        {@link QueryCursor#next()}
     */
    public QueryCursor query(String sql) {
        try {
            Statement statement = new Parser(sql).parse();
            if (statement instanceof CreateTableStatement createTableStatement) {
                tableManager.createTable(createTableStatement.tableName(), createTableStatement.columns());
                return new ExecutorQueryCursor(new Schema(List.of()), new Executor() {
                    @Override
                    public Optional<Row> next() {
                        return Optional.empty();
                    }
                });
            }
            BoundStatement boundStatement = new Binder(catalog).bind(statement);
            LogicalPlan logicalPlan = logicalPlanner.plan(boundStatement);
            PhysicalPlan physicalPlan = physicalPlanner.plan(logicalPlan);
            Executor executor = new PhysicalExecutorFactory(new ExecutionContext(tableAccess)).create(physicalPlan);
            return new ExecutorQueryCursor(physicalPlan.outputSchema(), executor);
        } catch (ParseException exception) {
            throw new QueryException(QueryException.Kind.PARSE, exception.getMessage(), exception);
        } catch (BindException exception) {
            throw new QueryException(QueryException.Kind.BIND, exception.getMessage(), exception);
        } catch (IllegalArgumentException exception) {
            throw new QueryException(QueryException.Kind.BIND, exception.getMessage(), exception);
        } catch (QueryException exception) {
            throw exception;
        } catch (Exception exception) {
            throw new QueryException(QueryException.Kind.EXECUTION, exception.getMessage(), exception);
        }
    }

    private static List<Row> collectRows(QueryCursor cursor) {
        List<Row> rows = new ArrayList<>();
        Optional<Row> row;
        while ((row = cursor.next()).isPresent()) {
            rows.add(row.get());
        }
        return rows;
    }
}
