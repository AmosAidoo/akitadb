package com.akita.query.optimizer;

import com.akita.catalog.JsonCatalog;
import com.akita.catalog.TableMetadata;
import com.akita.datatype.AkitaType;
import com.akita.datatype.ColumnMetadata;
import com.akita.datatype.Schema;
import com.akita.query.bind.Binder;
import com.akita.query.bind.BoundBinaryExpression;
import com.akita.query.bind.BoundColumnReference;
import com.akita.query.bind.BoundSelectStatement;
import com.akita.query.logical.LogicalPlan;
import com.akita.query.logical.LogicalPlanner;
import com.akita.query.physical.FilterPlan;
import com.akita.query.physical.PhysicalPlan;
import com.akita.query.physical.ProjectionPlan;
import com.akita.query.physical.SeqScanPlan;
import com.akita.sql.parser.Parser;
import com.akita.storage.ContainerId;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class PhysicalPlannerTest {

    @Test
    void plansProjectionOverSeqScanWithoutWhereClause() {
        PhysicalPlan plan = plan("SELECT id, name FROM users");

        ProjectionPlan projection = (ProjectionPlan) plan;
        assertThat(projection.selectItems()).hasSize(2);
        assertThat(projection.outputSchema().columns())
                .extracting(ColumnMetadata::name)
                .containsExactly("id", "name");
        assertThat(projection.input()).isInstanceOf(SeqScanPlan.class);

        SeqScanPlan scan = (SeqScanPlan) projection.input();
        assertThat(scan.table().metadata().tableName()).isEqualTo("users");
        assertThat(scan.outputSchema()).isEqualTo(usersTable().schema());
    }

    @Test
    void plansProjectionOverFilterOverSeqScanWithWhereClause() {
        PhysicalPlan plan = plan("SELECT id, name FROM users WHERE age > 18");

        ProjectionPlan projection = (ProjectionPlan) plan;
        assertThat(projection.input()).isInstanceOf(FilterPlan.class);

        FilterPlan filter = (FilterPlan) projection.input();
        assertThat(filter.predicate()).isInstanceOf(BoundBinaryExpression.class);
        BoundColumnReference left = (BoundColumnReference) ((BoundBinaryExpression) filter.predicate()).left();
        assertThat(left.columnName()).isEqualTo("age");
        assertThat(filter.outputSchema()).isEqualTo(usersTable().schema());
        assertThat(filter.input()).isInstanceOf(SeqScanPlan.class);
    }

    private static PhysicalPlan plan(String sql) {
        JsonCatalog catalog = new JsonCatalog();
        catalog.createTable(usersTable());
        BoundSelectStatement statement = (BoundSelectStatement) new Binder(catalog).bind(new Parser(sql).parse());
        LogicalPlan logicalPlan = new LogicalPlanner().plan(statement);
        return new PhysicalPlanner().plan(logicalPlan);
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
