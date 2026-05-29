package com.akita.query.physical;

import com.akita.datatype.Schema;

public sealed interface PhysicalPlan permits SeqScanPlan, FilterPlan, ProjectionPlan, InsertPlan {

    Schema outputSchema();
}
