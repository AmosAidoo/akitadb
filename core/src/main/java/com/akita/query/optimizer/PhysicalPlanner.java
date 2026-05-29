package com.akita.query.optimizer;

import com.akita.query.logical.LogicalFilter;
import com.akita.query.logical.LogicalInsert;
import com.akita.query.logical.LogicalPlan;
import com.akita.query.logical.LogicalProjection;
import com.akita.query.logical.LogicalScan;
import com.akita.query.physical.FilterPlan;
import com.akita.query.physical.InsertPlan;
import com.akita.query.physical.PhysicalPlan;
import com.akita.query.physical.ProjectionPlan;
import com.akita.query.physical.SeqScanPlan;

public class PhysicalPlanner {

    public PhysicalPlan plan(LogicalPlan plan) {
        return switch (plan) {
            case LogicalInsert insert -> new InsertPlan(insert.statement());
            case LogicalScan scan -> new SeqScanPlan(scan.table());
            case LogicalFilter filter -> new FilterPlan(plan(filter.input()), filter.predicate());
            case LogicalProjection projection ->
                    new ProjectionPlan(plan(projection.input()), projection.selectItems(), projection.outputSchema());
        };
    }
}
