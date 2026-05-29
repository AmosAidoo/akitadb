package com.akita.query;

import com.akita.query.execution.ExecutionContext;
import com.akita.query.execution.Executor;
import com.akita.query.execution.FilterExecutor;
import com.akita.query.execution.InsertExecutor;
import com.akita.query.execution.ProjectionExecutor;
import com.akita.query.execution.SeqScanExecutor;
import com.akita.query.physical.FilterPlan;
import com.akita.query.physical.InsertPlan;
import com.akita.query.physical.PhysicalPlan;
import com.akita.query.physical.ProjectionPlan;
import com.akita.query.physical.SeqScanPlan;

final class PhysicalExecutorFactory {
    private final ExecutionContext context;

    PhysicalExecutorFactory(ExecutionContext context) {
        this.context = context;
    }

    Executor create(PhysicalPlan plan) throws Exception {
        return switch (plan) {
            case SeqScanPlan scan -> new SeqScanExecutor(context, scan);
            case InsertPlan insert -> new InsertExecutor(context, insert);
            case FilterPlan filter -> new FilterExecutor(create(filter.input()), filter);
            case ProjectionPlan projection -> new ProjectionExecutor(create(projection.input()), projection);
        };
    }
}
