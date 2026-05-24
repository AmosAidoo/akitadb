package com.akita.query.bind;

import com.akita.catalog.JsonCatalog;
import com.akita.catalog.TableMetadata;
import com.akita.datatype.AkitaType;
import com.akita.datatype.ColumnMetadata;
import com.akita.datatype.Schema;
import com.akita.sql.parser.Parser;
import com.akita.storage.ContainerId;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BinderTest {

    @Test
    void resolvesSelectColumnsAgainstCatalog() {
        BoundSelectStatement statement = bind("SELECT id FROM users");

        BoundColumnReference column = (BoundColumnReference) statement.selectItems().getFirst().expression();
        assertThat(statement.table().metadata().tableName()).isEqualTo("users");
        assertThat(column.columnName()).isEqualTo("id");
        assertThat(column.ordinalPosition()).isZero();
        assertThat(column.type()).isEqualTo(new AkitaType.Integer());
    }

    @Test
    void resolvesWhereClauseToBooleanExpression() {
        BoundSelectStatement statement = bind("SELECT id FROM users WHERE age > 18");

        assertThat(statement.where()).isInstanceOf(BoundBinaryExpression.class);
        BoundBinaryExpression where = (BoundBinaryExpression) statement.where();
        assertThat(where.type()).isEqualTo(new AkitaType.Boolean());
        BoundColumnReference left = (BoundColumnReference) where.left();
        assertThat(left.columnName()).isEqualTo("age");
        assertThat(left.ordinalPosition()).isEqualTo(2);
    }

    @Test
    void rejectsUnknownTables() {
        assertThatThrownBy(() -> bind("SELECT id FROM accounts"))
                .isInstanceOf(BindException.class)
                .hasMessage("Unknown table: accounts");
    }

    @Test
    void rejectsUnknownColumns() {
        assertThatThrownBy(() -> bind("SELECT missing FROM users"))
                .isInstanceOf(BindException.class)
                .hasMessage("Unknown column: missing");
    }

    @Test
    void rejectsNonBooleanWhereClause() {
        assertThatThrownBy(() -> bind("SELECT id FROM users WHERE age + 1"))
                .isInstanceOf(BindException.class)
                .hasMessage("WHERE expression must be boolean");
    }

    @Test
    void resolvesQualifiedColumnsAgainstTableName() {
        BoundSelectStatement statement = bind("SELECT users.id FROM users WHERE users.age > 18");

        BoundColumnReference column = (BoundColumnReference) statement.selectItems().getFirst().expression();
        assertThat(column.columnName()).isEqualTo("id");
    }

    private static BoundSelectStatement bind(String sql) {
        JsonCatalog catalog = new JsonCatalog();
        catalog.createTable(usersTable());
        return (BoundSelectStatement) new Binder(catalog).bind(new Parser(sql).parse());
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
