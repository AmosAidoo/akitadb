package com.akita.query;

import com.akita.buffer.BufferPoolManager;
import com.akita.catalog.Catalog;
import com.akita.query.bind.BindException;
import com.akita.query.bind.Binder;
import com.akita.query.bind.BoundStatement;
import com.akita.query.execution.ExecutionContext;
import com.akita.query.execution.Executor;
import com.akita.query.execution.Row;
import com.akita.query.logical.LogicalPlan;
import com.akita.query.logical.LogicalPlanner;
import com.akita.query.optimizer.PhysicalPlanner;
import com.akita.query.physical.PhysicalPlan;
import com.akita.sql.ast.Statement;
import com.akita.sql.parser.ParseException;
import com.akita.sql.parser.Parser;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class QueryEngine {
    private final Catalog catalog;
    private final BufferPoolManager bufferPoolManager;
    private final LogicalPlanner logicalPlanner;
    private final PhysicalPlanner physicalPlanner;

    public QueryEngine(Catalog catalog, BufferPoolManager bufferPoolManager) {
        this(catalog, bufferPoolManager, new LogicalPlanner(), new PhysicalPlanner());
    }

    QueryEngine(
            Catalog catalog,
            BufferPoolManager bufferPoolManager,
            LogicalPlanner logicalPlanner,
            PhysicalPlanner physicalPlanner
    ) {
        this.catalog = catalog;
        this.bufferPoolManager = bufferPoolManager;
        this.logicalPlanner = logicalPlanner;
        this.physicalPlanner = physicalPlanner;
    }

    public QueryResult execute(String sql) {
        try {
            Statement statement = new Parser(sql).parse();
            BoundStatement boundStatement = new Binder(catalog).bind(statement);
            LogicalPlan logicalPlan = logicalPlanner.plan(boundStatement);
            PhysicalPlan physicalPlan = physicalPlanner.plan(logicalPlan);
            Executor executor = new PhysicalExecutorFactory(new ExecutionContext(bufferPoolManager)).create(physicalPlan);
            return new QueryResult(physicalPlan.outputSchema(), collectRows(executor));
        } catch (ParseException exception) {
            throw new QueryException(QueryException.Kind.PARSE, exception.getMessage(), exception);
        } catch (BindException exception) {
            throw new QueryException(QueryException.Kind.BIND, exception.getMessage(), exception);
        } catch (QueryException exception) {
            throw exception;
        } catch (Exception exception) {
            throw new QueryException(QueryException.Kind.EXECUTION, exception.getMessage(), exception);
        }
    }

    private static List<Row> collectRows(Executor executor) throws Exception {
        List<Row> rows = new ArrayList<>();
        Optional<Row> row;
        while ((row = executor.next()).isPresent()) {
            rows.add(row.get());
        }
        return rows;
    }
}
