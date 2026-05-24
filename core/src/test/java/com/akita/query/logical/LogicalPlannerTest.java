package com.akita.query.logical;

import com.akita.catalog.JsonCatalog;
import com.akita.catalog.TableMetadata;
import com.akita.datatype.AkitaType;
import com.akita.datatype.ColumnMetadata;
import com.akita.datatype.Schema;
import com.akita.query.bind.Binder;
import com.akita.query.bind.BoundBinaryExpression;
import com.akita.query.bind.BoundColumnReference;
import com.akita.query.bind.BoundSelectStatement;
import com.akita.sql.parser.Parser;
import com.akita.storage.ContainerId;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class LogicalPlannerTest {

    @Test
    void plansProjectionOverScanWithoutWhereClause() {
        LogicalPlan plan = plan("SELECT id, name FROM users");

        LogicalProjection projection = (LogicalProjection) plan;
        assertThat(projection.selectItems()).hasSize(2);
        assertThat(projection.outputSchema().columns())
                .extracting(ColumnMetadata::name)
                .containsExactly("id", "name");
        assertThat(projection.input()).isInstanceOf(LogicalScan.class);

        LogicalScan scan = (LogicalScan) projection.input();
        assertThat(scan.table().metadata().tableName()).isEqualTo("users");
        assertThat(scan.outputSchema()).isEqualTo(usersTable().schema());
    }

    @Test
    void plansProjectionOverFilterOverScanWithWhereClause() {
        LogicalPlan plan = plan("SELECT id, name FROM users WHERE age > 18");

        LogicalProjection projection = (LogicalProjection) plan;
        assertThat(projection.input()).isInstanceOf(LogicalFilter.class);

        LogicalFilter filter = (LogicalFilter) projection.input();
        assertThat(filter.predicate()).isInstanceOf(BoundBinaryExpression.class);
        BoundColumnReference left = (BoundColumnReference) ((BoundBinaryExpression) filter.predicate()).left();
        assertThat(left.columnName()).isEqualTo("age");
        assertThat(filter.input()).isInstanceOf(LogicalScan.class);
    }

    @Test
    void printsLogicalPlanForDebugging() {
        LogicalPlan plan = plan("SELECT id, name FROM users WHERE age > 18");

        assertThat(LogicalPlanPrinter.print(plan)).isEqualTo("""
                Projection[id, name] output=[id:INTEGER, name:VARCHAR(255)]
                  Filter[predicate=(age GREATER_THAN 18)] output=[id:INTEGER, name:VARCHAR(255), age:INTEGER]
                    Scan[table=users] output=[id:INTEGER, name:VARCHAR(255), age:INTEGER]
                """);
    }

    private static LogicalPlan plan(String sql) {
        JsonCatalog catalog = new JsonCatalog();
        catalog.createTable(usersTable());
        BoundSelectStatement statement = (BoundSelectStatement) new Binder(catalog).bind(new Parser(sql).parse());
        return new LogicalPlanner().plan(statement);
    }

    private static TableMetadata usersTable() {
        return new TableMetadata(
                "users",
                ContainerId.generate(),
                new Schema(List.of(
                        new ColumnMetadata("id", new AkitaType.Integer(), 0, false),
                        new ColumnMetadata("name", new AkitaType.Varchar(255), 1, false),
                        new ColumnMetadata("age", new AkitaType.Integer(), 2, true)
                ))
        );
    }
}
