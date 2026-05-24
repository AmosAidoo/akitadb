package com.akita.sql.ast;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SqlAstTest {

    @Test
    void representsSelectListAndTableReference() {
        SelectStatement statement = new SelectStatement(
                List.of(
                        new SelectItem(identifier("id")),
                        new SelectItem(identifier("name"))
                ),
                new TableRef(QualifiedName.of("users"))
        );

        assertThat(statement.selectItems())
                .extracting(SelectItem::expression)
                .containsExactly(identifier("id"), identifier("name"));
        assertThat(statement.from()).isEqualTo(new TableRef(QualifiedName.of("users")));
        assertThat(statement.optionalWhere()).isEmpty();
    }

    @Test
    void representsWhereExpression() {
        SelectStatement statement = new SelectStatement(
                List.of(new SelectItem(identifier("id"))),
                new TableRef(QualifiedName.of("users")),
                new BinaryExpression(
                        identifier("age"),
                        BinaryOperator.GREATER_THAN,
                        LiteralExpression.integer(18)
                )
        );

        assertThat(statement.optionalWhere())
                .contains(new BinaryExpression(
                        identifier("age"),
                        BinaryOperator.GREATER_THAN,
                        LiteralExpression.integer(18)
                ));
    }

    @Test
    void representsQualifiedNamesAndNestedExpressionGrouping() {
        Expression groupedExpression = new BinaryExpression(
                new BinaryExpression(
                        identifier("age"),
                        BinaryOperator.ADD,
                        LiteralExpression.integer(1)
                ),
                BinaryOperator.GREATER_THAN,
                LiteralExpression.integer(18)
        );

        assertThat(identifier("users", "id").name()).isEqualTo(QualifiedName.of("users", "id"));
        assertThat(groupedExpression).isEqualTo(
                new BinaryExpression(
                        new BinaryExpression(identifier("age"), BinaryOperator.ADD, LiteralExpression.integer(1)),
                        BinaryOperator.GREATER_THAN,
                        LiteralExpression.integer(18)
                )
        );
    }

    @Test
    void copiesMutableCollections() {
        List<SelectItem> selectItems = new ArrayList<>();
        selectItems.add(new SelectItem(identifier("id")));
        SelectStatement statement = new SelectStatement(selectItems, new TableRef(QualifiedName.of("users")));

        selectItems.add(new SelectItem(identifier("name")));

        assertThat(statement.selectItems()).containsExactly(new SelectItem(identifier("id")));
        assertThatThrownBy(() -> statement.selectItems().add(new SelectItem(identifier("name"))))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void rejectsEmptyQualifiedNames() {
        assertThatThrownBy(() -> new QualifiedName(List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("qualified name");
    }

    private static IdentifierExpression identifier(String first, String... rest) {
        return new IdentifierExpression(QualifiedName.of(first, rest));
    }
}
