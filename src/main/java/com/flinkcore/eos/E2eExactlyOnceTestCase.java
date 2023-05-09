package com.flinkcore.eos;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.awt.*;
import java.util.concurrent.TimeUnit;

/**
 * 功能描述: 本测试核心是演示流计算语义 at-most-once, at-least-once, exactly-once, e2e-exactly-once.
 * 操作步骤: 1. 直接运行程序，观察atMostOnce语义效果；
 * 2. 打开atLeastOnce(env)，观察atLeastOnce效果,主要是要和exactlyOnce进行输出对比。
 * 3. 打开exactlyOnce(env)，观察exactlyOnce效果，主要是要和atLeastOnce进行输出对比。
 * 4. 打开exactlyOnce2(env)，观察print效果(相当于sink），主要是要和e2eExactlyOnce进行输出对比。
 * 5. 打开e2eExactlyOnce(env)，观察print效果(相当于sink），主要是要和exactlyOnce2(env)进行输出对比。
 */
public class E2eExactlyOnceTestCase {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.enableCheckpointing(10);

        env.setRestartStrategy(
                RestartStrategies.fixedDelayRestart(3, Time.of(1, TimeUnit.SECONDS))
        );

//        atMostOnce(env);
//        atLeastOnce(env);
        exactlyOnce(env);



        env.execute();

    }

    /**
     * 模拟无状态的数据源，同时数据是根据时间进行推移的，所以一旦
     * 流计算发生异常，那么异常期间的数据就丢失了，也就是at-most-once
     */
    private static void atMostOnce(StreamExecutionEnvironment env) {
        env.addSource(new SourceFunction<Tuple2<String, Long>>() {
            private Long index = 0L;

            @Override
            public void run(SourceContext<Tuple2<String, Long>> sourceContext) throws Exception {
                while (true) {
                    sourceContext.collect(new Tuple2<>("key", ++index));
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {

            }
        }).map(new MapFunction<Tuple2<String, Long>, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(Tuple2<String, Long> event) throws Exception {
                if (event.f1 % 10 == 0) {
                    String msg = String.format("Bad data [%d]...", event.f1);
                    throw new RuntimeException(msg);
                }
                return event;
            }
        }).print();
    }


    private static void atLeastOnce(StreamExecutionEnvironment env) {
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
        basicLogic(env).process(new StateProcessFunction()).print("->");
    }

    private static void exactlyOnce(StreamExecutionEnvironment env) {
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        basicLogic(env).process(new StateProcessFunction()).print();
    }

    private static void e2eExactlyOnce(StreamExecutionEnvironment env ){
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        basicLogic(env).addSink(new )
    }


    private static KeyedStream<Tuple3<String, Long, String>, String> basicLogic(StreamExecutionEnvironment env) {
        DataStreamSource<Tuple3<String, Long, String>> source1 = env.addSource(new ParallelCheckpointedSource("s1"));
        DataStreamSource<Tuple3<String, Long, String>> source2 = env.addSource(new ParallelCheckpointedSource("s2"));

        SingleOutputStreamOperator<Tuple3<String, Long, String>> ds1 = source1.map(new MapFunctionWithException(10,"s1"));
        SingleOutputStreamOperator<Tuple3<String, Long, String>> ds2 = source2.map(new MapFunctionWithException(200,"s2"));
        return ds1.union(ds2).keyBy(t -> t.f0);
    }

}
