package com.flinkcore.eos;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;

public class MapFunctionWithException extends
        RichMapFunction<Tuple3<String, Long, String>, Tuple3<String, Long, String>>
        implements CheckpointListener {

    private long delay;
    private String name;
    private transient volatile boolean needFail = false;

    public MapFunctionWithException(long delay, String name) {
        this.delay = delay;
        this.name = name;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public Tuple3<String, Long, String> map(Tuple3<String, Long, String> event) throws Exception {
        Thread.sleep(delay);
        if (needFail) {
            throw new RuntimeException("Error for testing...");
        }
        return event;
    }

    @Override
    public void notifyCheckpointComplete(long l) throws Exception {
        this.needFail = true;
        System.err.println(String.format("MAP [%s] - CP SUCCESS [%d]", name, l));
    }
}