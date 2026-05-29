package com.akita.query.logical;

import com.akita.datatype.Schema;

public sealed interface LogicalPlan permits LogicalScan, LogicalFilter, LogicalProjection, LogicalInsert {

    Schema outputSchema();
}
