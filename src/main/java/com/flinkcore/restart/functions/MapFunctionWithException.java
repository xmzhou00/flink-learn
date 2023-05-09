package com.flinkcore.restart.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;

public class MapFunctionWithException extends RichMapFunction<Tuple3<String, Long, Long>, Tuple3<String, Long, Long>> {
    private transient int indexOfSubTask;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        indexOfSubTask = getRuntimeContext().getIndexOfThisSubtask();
    }

    @Override
    public Tuple3<String, Long, Long> map(Tuple3<String, Long, Long> event) throws Exception {
        if (event.f1 % 10 == 0) {
            String msg = String.format("Bad data [%d]...", event.f1);
            throw new RuntimeException(msg);
        }
        return new Tuple3<>(event.f0, event.f1, System.currentTimeMillis());
    }
}
