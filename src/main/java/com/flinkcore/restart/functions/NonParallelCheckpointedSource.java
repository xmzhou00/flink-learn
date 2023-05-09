package com.flinkcore.restart.functions;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NonParallelCheckpointedSource implements SourceFunction<Tuple3<String, Long, Long>>, CheckpointedFunction {

    private static final Logger LOG = LoggerFactory.getLogger(NonParallelCheckpointedSource.class);

    protected volatile boolean running = true;

    private transient long offset;

    private transient ListState<Long> offsetState;

    private static final String OFFSET_STATE_NAME = "offset-state";

    @Override
    public void run(SourceContext<Tuple3<String, Long, Long>> sourceContext) throws Exception {
        while (running) {
            sourceContext.collect(new Tuple3<>("key", ++offset, System.currentTimeMillis()));
            Thread.sleep(100);
        }
    }

    @Override
    public void cancel() {

    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        LOG.error("----> snapshot offset:{}",offset);
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
        LOG.error("initialize state...");
        this.offsetState = ctx
                .getOperatorStateStore()
                .getListState(new ListStateDescriptor<Long>(OFFSET_STATE_NAME, Types.LONG));

        for (Long offsetValue : this.offsetState.get()) {
            offset = offsetValue;
            // 跳过10和20的循环失败
            if (offset == 9 || offset == 19) {
                offset += 1;
            }
            // user error, just for test
            LOG.error(String.format("Restore from offset [%d]", offset));
        }

    }
}
