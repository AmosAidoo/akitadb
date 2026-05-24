package com.akita.sql.parser;

import com.akita.sql.ast.BinaryExpression;
import com.akita.sql.ast.BinaryOperator;
import com.akita.sql.ast.Expression;
import com.akita.sql.ast.IdentifierExpression;
import com.akita.sql.ast.LiteralExpression;
import com.akita.sql.ast.QualifiedName;
import com.akita.sql.ast.SelectItem;
import com.akita.sql.ast.SelectStatement;
import com.akita.sql.ast.TableRef;
import com.akita.sql.ast.UnaryExpression;
import com.akita.sql.ast.UnaryOperator;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ParserTest {

    @Test
    void parsesSimpleSelectFrom() {
        SelectStatement statement = parseSelect("SELECT id FROM users");

        assertThat(statement).isEqualTo(new SelectStatement(
                List.of(new SelectItem(identifier("id"))),
                new TableRef(QualifiedName.of("users"))
        ));
    }

    @Test
    void parsesSelectListAndWhereClause() {
        SelectStatement statement = parseSelect("SELECT id, name FROM users WHERE age > 18");

        assertThat(statement).isEqualTo(new SelectStatement(
                List.of(new SelectItem(identifier("id")), new SelectItem(identifier("name"))),
                new TableRef(QualifiedName.of("users")),
                new BinaryExpression(identifier("age"), BinaryOperator.GREATER_THAN, LiteralExpression.integer(18))
        ));
    }

    @Test
    void parsesQualifiedIdentifiersAndTableNames() {
        SelectStatement statement = parseSelect("SELECT users.id FROM main.users WHERE users.age >= 18;");

        assertThat(statement.selectItems()).containsExactly(new SelectItem(identifier("users", "id")));
        assertThat(statement.from()).isEqualTo(new TableRef(QualifiedName.of("main", "users")));
        assertThat(statement.where()).isEqualTo(new BinaryExpression(
                identifier("users", "age"),
                BinaryOperator.GREATER_THAN_OR_EQUAL,
                LiteralExpression.integer(18)
        ));
    }

    @Test
    void parsesLiterals() {
        SelectStatement statement = parseSelect("SELECT 'akita', TRUE, false, NULL FROM users");

        assertThat(statement.selectItems())
                .extracting(SelectItem::expression)
                .containsExactly(
                        LiteralExpression.string("akita"),
                        LiteralExpression.bool(true),
                        LiteralExpression.bool(false),
                        LiteralExpression.nullLiteral()
                );
    }

    @Test
    void parsesExpressionPrecedence() {
        SelectStatement statement = parseSelect("SELECT age + 1 * 2 > 18 AND active = TRUE OR score <> 0 FROM users");

        assertThat(statement.selectItems()).containsExactly(new SelectItem(
                new BinaryExpression(
                        new BinaryExpression(
                                new BinaryExpression(
                                        new BinaryExpression(
                                                identifier("age"),
                                                BinaryOperator.ADD,
                                                new BinaryExpression(
                                                        LiteralExpression.integer(1),
                                                        BinaryOperator.MULTIPLY,
                                                        LiteralExpression.integer(2)
                                                )
                                        ),
                                        BinaryOperator.GREATER_THAN,
                                        LiteralExpression.integer(18)
                                ),
                                BinaryOperator.AND,
                                new BinaryExpression(identifier("active"), BinaryOperator.EQUAL, LiteralExpression.bool(true))
                        ),
                        BinaryOperator.OR,
                        new BinaryExpression(identifier("score"), BinaryOperator.NOT_EQUAL, LiteralExpression.integer(0))
                )
        ));
    }

    @Test
    void parsesParenthesesAndUnaryOperators() {
        SelectStatement statement = parseSelect("SELECT -(age + 1) * +2 FROM users");

        assertThat(statement.selectItems()).containsExactly(new SelectItem(
                new BinaryExpression(
                        new UnaryExpression(
                                UnaryOperator.MINUS,
                                new BinaryExpression(identifier("age"), BinaryOperator.ADD, LiteralExpression.integer(1))
                        ),
                        BinaryOperator.MULTIPLY,
                        new UnaryExpression(UnaryOperator.PLUS, LiteralExpression.integer(2))
                )
        ));
    }

    @Test
    void rejectsMissingSelectList() {
        assertThatThrownBy(() -> parseSelect("SELECT FROM users"))
                .isInstanceOf(ParseException.class)
                .hasMessageContaining("Expected expression")
                .hasMessageContaining("found 'FROM'")
                .hasMessageContaining("position 7");
    }

    @Test
    void rejectsMissingFrom() {
        assertThatThrownBy(() -> parseSelect("SELECT id users"))
                .isInstanceOf(ParseException.class)
                .hasMessageContaining("Expected FROM after select list")
                .hasMessageContaining("found 'users'");
    }

    @Test
    void rejectsTrailingTokens() {
        assertThatThrownBy(() -> parseSelect("SELECT id FROM users WHERE age > 18 id"))
                .isInstanceOf(ParseException.class)
                .hasMessageContaining("Expected end of input")
                .hasMessageContaining("found 'id'");
    }

    @Test
    void rejectsUnclosedParentheses() {
        assertThatThrownBy(() -> parseSelect("SELECT (age + 1 FROM users"))
                .isInstanceOf(ParseException.class)
                .hasMessageContaining("Expected ')' after expression")
                .hasMessageContaining("found 'FROM'");
    }

    private static SelectStatement parseSelect(String source) {
        return (SelectStatement) new Parser(source).parse();
    }

    private static Expression identifier(String first, String... rest) {
        return new IdentifierExpression(QualifiedName.of(first, rest));
    }
}
