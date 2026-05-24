package com.akita.query.execution;

import com.akita.datatype.AkitaValue;
import com.akita.query.physical.FilterPlan;

import java.util.Optional;

public class FilterExecutor implements Executor {
    private final Executor input;
    private final FilterPlan plan;
    private final ExpressionEvaluator evaluator;

    public FilterExecutor(Executor input, FilterPlan plan) {
        this(input, plan, new ExpressionEvaluator());
    }

    FilterExecutor(Executor input, FilterPlan plan, ExpressionEvaluator evaluator) {
        this.input = input;
        this.plan = plan;
        this.evaluator = evaluator;
    }

    @Override
    public Optional<Row> next() throws Exception {
        Optional<Row> row;
        while ((row = input.next()).isPresent()) {
            if (matches(row.get())) {
                return row;
            }
        }
        return Optional.empty();
    }

    private boolean matches(Row row) {
        AkitaValue value = evaluator.evaluate(plan.predicate(), row);
        if (value instanceof AkitaValue.BoolVal(var bool)) {
            return bool;
        }
        throw new ExpressionEvaluationException("Filter predicate must evaluate to BoolVal");
    }
}
