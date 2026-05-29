package com.akita.query.execution;

import com.akita.datatype.AkitaValue;
import com.akita.heap.HeapFile;
import com.akita.query.physical.InsertPlan;

import java.util.Optional;

public class InsertExecutor implements Executor {
    private final InsertPlan plan;
    private final HeapFile heapFile;
    private final RowTupleCodec codec;
    private final ExpressionEvaluator evaluator;
    private boolean executed;

    public InsertExecutor(ExecutionContext context, InsertPlan plan) throws Exception {
        this(plan,
                HeapFile.open(plan.statement().table().metadata().containerId(), context.bufferPoolManager()),
                new RowTupleCodec(),
                new ExpressionEvaluator());
    }

    InsertExecutor(InsertPlan plan, HeapFile heapFile, RowTupleCodec codec, ExpressionEvaluator evaluator) {
        this.plan = plan;
        this.heapFile = heapFile;
        this.codec = codec;
        this.evaluator = evaluator;
    }

    @Override
    public Optional<Row> next() throws Exception {
        if (executed) {
            return Optional.empty();
        }
        executed = true;

        for (var values : plan.statement().values()) {
            AkitaValue[] rowValues = new AkitaValue[plan.statement().table().metadata().schema().columns().size()];
            for (int i = 0; i < values.size(); i++) {
                int ordinal = plan.statement().targetColumns().get(i).ordinalPosition();
                rowValues[ordinal] = evaluator.evaluate(values.get(i), Row.of());
            }
            heapFile.insertTuple(codec.encode(new Row(rowValues), plan.statement().table().metadata().schema()));
        }

        return Optional.empty();
    }
}
