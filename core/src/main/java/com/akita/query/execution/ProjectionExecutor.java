package com.akita.query.execution;

import com.akita.query.physical.ProjectionPlan;

import java.util.Optional;

public class ProjectionExecutor implements Executor {
    private final Executor input;
    private final ProjectionPlan plan;
    private final ExpressionEvaluator evaluator;

    public ProjectionExecutor(Executor input, ProjectionPlan plan) {
        this(input, plan, new ExpressionEvaluator());
    }

    ProjectionExecutor(Executor input, ProjectionPlan plan, ExpressionEvaluator evaluator) {
        this.input = input;
        this.plan = plan;
        this.evaluator = evaluator;
    }

    @Override
    public Optional<Row> next() throws Exception {
        return input.next().map(row -> evaluator.project(plan.selectItems(), row));
    }
}
