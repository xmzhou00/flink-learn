package com.flinkcore.eos;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * 记录所有处理过的数据，测试在 AT_LEAST_ONCE时候重复消费的数据
 */
public class StateProcessFunction extends KeyedProcessFunction<String, Tuple3<String, Long, String>, Tuple3<String, Long, String>> {

    private transient ListState<Tuple3<String, Long, String>> processData;

    private final String STATE_NAME = "processData";

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        processData = getRuntimeContext().getListState(
                new ListStateDescriptor<Tuple3<String, Long, String>>(STATE_NAME, Types.TUPLE(Types.STRING, Types.LONG, Types.STRING))
        );
    }

    @Override
    public void processElement(Tuple3<String, Long, String> event, KeyedProcessFunction<String, Tuple3<String, Long, String>, Tuple3<String, Long, String>>.Context context, Collector<Tuple3<String, Long, String>> out) throws Exception {
        System.out.println("process: -> " + event);
        boolean isDuplicate = false;

        Iterator<Tuple3<String, Long, String>> it = processData.get().iterator();
        while (it.hasNext()) {
            if (it.next().equals(event)) {
                isDuplicate = true;
                break;
            }
        }
        if (isDuplicate) {
            out.collect(event);
        } else {
            processData.add(event);
        }
    }
}
