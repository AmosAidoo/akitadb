package com.akita.query.execution;

import com.akita.datatype.AkitaType;
import com.akita.datatype.AkitaValue;
import com.akita.query.bind.BoundBinaryExpression;
import com.akita.query.bind.BoundColumnReference;
import com.akita.query.bind.BoundLiteral;
import com.akita.query.bind.BoundSelectItem;
import com.akita.sql.ast.BinaryOperator;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ExpressionEvaluatorTest {

    private final ExpressionEvaluator evaluator = new ExpressionEvaluator();

    @Test
    void evaluatesColumnReferencesByOrdinalPosition() {
        Row row = Row.of(new AkitaValue.IntVal(1), new AkitaValue.VarcharVal("Ada"));

        AkitaValue value = evaluator.evaluate(column("name", 1, new AkitaType.Varchar(255)), row);

        assertThat(value).isEqualTo(new AkitaValue.VarcharVal("Ada"));
    }

    @Test
    void evaluatesAgeGreaterThanEighteen() {
        Row row = Row.of(new AkitaValue.IntVal(1), new AkitaValue.IntVal(42));
        BoundBinaryExpression predicate = new BoundBinaryExpression(
                column("age", 1, new AkitaType.Integer()),
                BinaryOperator.GREATER_THAN,
                new BoundLiteral(18, new AkitaType.Integer()),
                new AkitaType.Boolean()
        );

        AkitaValue value = evaluator.evaluate(predicate, row);

        assertThat(value).isEqualTo(new AkitaValue.BoolVal(true));
    }

    @Test
    void evaluatesBooleanAndOr() {
        Row row = Row.of(new AkitaValue.IntVal(20), new AkitaValue.BoolVal(false));
        BoundBinaryExpression ageCheck = new BoundBinaryExpression(
                column("age", 0, new AkitaType.Integer()),
                BinaryOperator.GREATER_THAN,
                new BoundLiteral(18, new AkitaType.Integer()),
                new AkitaType.Boolean()
        );
        BoundBinaryExpression activeCheck = new BoundBinaryExpression(
                column("active", 1, new AkitaType.Boolean()),
                BinaryOperator.EQUAL,
                new BoundLiteral(true, new AkitaType.Boolean()),
                new AkitaType.Boolean()
        );
        BoundBinaryExpression predicate = new BoundBinaryExpression(
                ageCheck,
                BinaryOperator.OR,
                activeCheck,
                new AkitaType.Boolean()
        );

        AkitaValue value = evaluator.evaluate(predicate, row);

        assertThat(value).isEqualTo(new AkitaValue.BoolVal(true));
    }

    @Test
    void evaluatesProjectionExpressionsAgainstRow() {
        Row row = Row.of(
                new AkitaValue.IntVal(1),
                new AkitaValue.VarcharVal("Ada"),
                new AkitaValue.IntVal(42)
        );

        Row projected = evaluator.project(List.of(
                new BoundSelectItem(column("name", 1, new AkitaType.Varchar(255)), "name"),
                new BoundSelectItem(column("age", 2, new AkitaType.Integer()), "age")
        ), row);

        assertThat(projected).containsExactly(
                new AkitaValue.VarcharVal("Ada"),
                new AkitaValue.IntVal(42)
        );
    }

    @Test
    void rejectsNullExpressionEvaluationForMilestoneOne() {
        Row row = Row.of(new AkitaValue.Null());
        BoundBinaryExpression predicate = new BoundBinaryExpression(
                column("age", 0, new AkitaType.Integer()),
                BinaryOperator.GREATER_THAN,
                new BoundLiteral(18, new AkitaType.Integer()),
                new AkitaType.Boolean()
        );

        assertThatThrownBy(() -> evaluator.evaluate(predicate, row))
                .isInstanceOf(ExpressionEvaluationException.class)
                .hasMessage("Null expression evaluation is not supported yet");
    }

    private static BoundColumnReference column(String name, int ordinalPosition, AkitaType type) {
        return new BoundColumnReference(null, name, ordinalPosition, type);
    }
}
