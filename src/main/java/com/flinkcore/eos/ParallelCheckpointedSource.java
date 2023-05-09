package com.flinkcore.eos;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.h2.security.XTEA;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParallelCheckpointedSource extends RichParallelSourceFunction<Tuple3<String, Long, String>> implements CheckpointedFunction {

    private static final Logger LOG = LoggerFactory.getLogger(ParallelCheckpointedSource.class);

    protected volatile boolean running = true;

    private transient long offset;

    private transient ListState<Long> offsetState;

    private static final String OFFSET_STATE_NAME = "offset-status";

    private transient int indexOfThisTask;

    private String name = "-";

    public ParallelCheckpointedSource(String name) {
        this.name = name;
    }


    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        if (!running) {
            LOG.error("snapshotState() called on closed source");
        } else {
            // 清除上次的state
            this.offsetState.clear();
            // 持久化最新的offset
            this.offsetState.add(offset);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext ctx) throws Exception {
        indexOfThisTask = getRuntimeContext().getIndexOfThisSubtask();
        offsetState = ctx
                .getOperatorStateStore()
                .getListState(new ListStateDescriptor<>(OFFSET_STATE_NAME, Types.LONG));

        for (Long offsetValue : offsetState.get()) {
            offset = offsetValue;
            LOG.error(String.format("Current Source [%s] Restore from offset [%d]", name, offset));
        }
    }

    @Override
    public void run(SourceContext<Tuple3<String, Long, String>> ctx) throws Exception {
        while (running) {
            synchronized (ctx.getCheckpointLock()) {
                Tuple3<String, Long, String> data = new Tuple3<>("key", ++offset, this.name);
                System.out.println("source:-> " + data);
                ctx.collect(data);
            }
            Thread.sleep(10);
        }
    }

    @Override
    public void cancel() {

    }
}
