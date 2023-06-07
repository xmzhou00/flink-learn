package com.flinkcore.timer;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;

public class KeyedIntProcess extends KeyedProcessFunction<String, Tuple2<String, Integer>, String> {

    final long OVER_TIME = 1000 * 5;

    private MapState<String, Long> productState;

    private SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");


    @Override
    public void processElement(Tuple2<String, Integer> value, KeyedProcessFunction<String, Tuple2<String, Integer>, String>.Context ctx, Collector<String> out) throws Exception {

        long currentTimeMillis = System.currentTimeMillis();

        long time = currentTimeMillis + OVER_TIME;

        productState.put(value.f0, time);

        Iterator<Map.Entry<String, Long>> iterator = productState.iterator();
        int size = 0;
        while (iterator.hasNext()){
            Map.Entry<String, Long> next = iterator.next();
            size ++;
        }
        System.out.println("state size: "+size);

        // 注册timer
        ctx.timerService().registerProcessingTimeTimer(time);
        System.err.println(value.f0 + " 当前时间：" + sdf.format(new Date(currentTimeMillis)) + " 过期时间 ： " + sdf.format(new Date(time)));


    }

    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<String, Tuple2<String, Integer>, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
        System.out.println("定时任务执行：" + sdf.format(new Date(timestamp)) + "--> " + ctx.getCurrentKey());
        Iterator<Map.Entry<String, Long>> iterator = productState.iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Long> next = iterator.next();
            String key = next.getKey();
            Long value = next.getValue();
            if (value == timestamp) {
                out.collect(key + "---> " + sdf.format(new Date(value)));
            }
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        MapStateDescriptor<String, Long> stateDescriptor = new MapStateDescriptor<>("productState", Types.STRING, Types.LONG);
        productState = getRuntimeContext().getMapState(stateDescriptor);
    }
}
