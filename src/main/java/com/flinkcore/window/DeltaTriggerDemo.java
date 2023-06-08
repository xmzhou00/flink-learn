package com.flinkcore.window;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.delta.DeltaFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.triggers.DeltaTrigger;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Iterator;

/**
 * DeltaTrigger: 可以触发多次。当前element与与上次触发trigger的element做delta计算，超过threshold触发窗口
 * 应用：车辆区间测速，车辆分每分钟上报当前位置与车速，每进行10公里，计算区间内的最高速度。
 */
public class DeltaTriggerDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Tuple4<String, Integer, Integer, Integer>> carData = env.fromElements(
                Tuple4.of("car1", 1, 999, 111),
                Tuple4.of("car1", 2, 3000, 191),
                Tuple4.of("car1", 3, 8000, 131),
                Tuple4.of("car1", 4, 11000, 144),
                Tuple4.of("car1", 5, 13000, 105),
                Tuple4.of("car1", 6, 16000, 95),
                Tuple4.of("car1", 7, 21001, 124),
                Tuple4.of("car1", 8, 23000, 134));

        carData.assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple4<String, Integer, Integer, Integer>>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple4<String, Integer, Integer, Integer>>() {
                                    @Override
                                    public long extractTimestamp(Tuple4<String, Integer, Integer, Integer> element, long recordTimestamp) {
                                        return element.f1 * 60 * 1000;
                                    }
                                })
                ).keyBy(t -> t.f0)
                .window(GlobalWindows.create())
                .evictor(new Evictor<Tuple4<String, Integer, Integer, Integer>, GlobalWindow>() {
                    @Override
                    public void evictBefore(Iterable<TimestampedValue<Tuple4<String, Integer, Integer, Integer>>> elements, int size, GlobalWindow window, EvictorContext evictorContext) {
                        System.out.println("evictor before");
                    }

                    @Override
                    public void evictAfter(Iterable<TimestampedValue<Tuple4<String, Integer, Integer, Integer>>> elements, int size, GlobalWindow window, EvictorContext evictorContext) {
                        System.out.println("evictor after");
                    }
                })
                .trigger(PurgingTrigger.of(DeltaTrigger.of(10000, new DeltaFunction<Tuple4<String, Integer, Integer, Integer>>() {
                    @Override
                    public double getDelta(
                            Tuple4<String, Integer, Integer, Integer> oldDataPoint,
                            Tuple4<String, Integer, Integer, Integer> newDataPoint) {
                        System.err.println("oldDataPoint: " + oldDataPoint + " newDataPoint: " + newDataPoint);
                        return newDataPoint.f2 - oldDataPoint.f2;
                    }
                }, carData.getType().createSerializer(env.getConfig()))))
//                        .max(3).print();
                .process(new ProcessWindowFunction<Tuple4<String, Integer, Integer, Integer>, Object, String, GlobalWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<Tuple4<String, Integer, Integer, Integer>, Object, String, GlobalWindow>.Context context, Iterable<Tuple4<String, Integer, Integer, Integer>> elements, Collector<Object> out) throws Exception {
                        Iterator<Tuple4<String, Integer, Integer, Integer>> iterator = elements.iterator();
//                        int maxSpeed = 0;
                        while (iterator.hasNext()) {
                            Tuple4<String, Integer, Integer, Integer> next = iterator.next();
                            System.out.println(next);
//                            maxSpeed = Math.max(maxSpeed, next.f3);
                        }

                        System.out.println("==============================");
//                        out.collect(maxSpeed);
                    }
                });
//                .print();

        env.execute();


    }
}
