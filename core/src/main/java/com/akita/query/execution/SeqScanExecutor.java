package com.akita.query.execution;

import com.akita.heap.HeapFileScan;
import com.akita.page.Tuple;
import com.akita.query.physical.SeqScanPlan;

import java.util.Optional;

public class SeqScanExecutor implements Executor {
    private final SeqScanPlan plan;
    private final RowTupleCodec codec;
    private final HeapFileScan scan;

    public SeqScanExecutor(ExecutionContext context, SeqScanPlan plan) throws Exception {
        this(context, plan, new RowTupleCodec());
    }

    SeqScanExecutor(ExecutionContext context, SeqScanPlan plan, RowTupleCodec codec) throws Exception {
        this.plan = plan;
        this.codec = codec;
        this.scan = context.tableAccess().scan(plan.table().metadata());
    }

    @Override
    public Optional<Row> next() throws Exception {
        Optional<Tuple> tuple = scan.next();
        return tuple.map(value -> codec.decode(value, plan.outputSchema()));
    }
}
