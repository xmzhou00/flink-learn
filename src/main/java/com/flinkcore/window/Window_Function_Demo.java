package com.flinkcore.window;

import com.flinkcore.EventLog;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Iterator;

/**
 * @Author:xmzhou
 * @Date: 2022/7/29 16:16
 * @Description: Window Function
 * 测试数据
 * 1,e01,10000,p01
 * 1,e02,11000,p02
 * 1,e02,12000,p03
 * 1,e03,20000,p02
 * 1,e01,21000,p03
 * 1,e04,22000,p04
 * 1,e06,28000,p05
 * 1,e07,30000,p02
 */
public class Window_Function_Demo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);// 设置环境模式

        // 1,e01,3000,p01
        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);


        SingleOutputStreamOperator<EventLog> eventStream = source.map(new MapFunction<String, EventLog>() {
            @Override
            public EventLog map(String value) throws Exception {
                String[] fields = value.split(",");
                return new EventLog(Long.parseLong(fields[0]), fields[1], Long.parseLong(fields[2]), fields[3]);
            }

        }).returns(EventLog.class)
                // 抽取watermark
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<EventLog>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner(new SerializableTimestampAssigner<EventLog>() {
                            @Override
                            public long extractTimestamp(EventLog element, long recordTimestamp) {
                                return element.getTimeStamp();
                            }
                        }));

        // testAggregateFunction(eventStream);
        // testIncrementAggregateFunction(eventStream);
        // testReduceFunction(eventStream);
        // testProcessWindowFunction(eventStream);
        testApplyFunction(eventStream);

        env.execute();
    }

    /**
     * aggregate算子
     * <p>
     * 滚动聚合api使用实示例
     * 需求一：每隔10s，统计最近30s的数据中，每个用户的行为事件条数
     * 使用aggregate算子实现
     */

    private static void testAggregateFunction(DataStream<EventLog> eventStream) {
        eventStream
                .keyBy(EventLog::getGuid)
                // 参数1：窗口大小  参数2：滑动步长
                .window(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(10)))
                .aggregate(new AggregateFunction<EventLog, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
                    /**
                     * 初始化累加器
                     * @return
                     */
                    @Override
                    public Tuple2<String, Integer> createAccumulator() {
                        return Tuple2.of(null, 0);
                    }

                    /**
                     * 滚动聚合（拿到一条数据，就去更新累加器）
                     * @param value
                     * @param accumulator
                     * @return
                     */
                    @Override
                    public Tuple2<String, Integer> add(EventLog value, Tuple2<String, Integer> accumulator) {
                        if (accumulator.f0 == null) {
                            accumulator.f0 = value.getGuid() + "<=";
                        }
                        accumulator.f1 += 1;
                        return accumulator;
                    }

                    /**
                     * 从累加器中，计算出最重要输出的窗口结果
                     * @param accumulator
                     * @return
                     */
                    @Override
                    public Tuple2<String, Integer> getResult(Tuple2<String, Integer> accumulator) {
                        return accumulator;
                    }

                    /**
                     * 批计算模式下，可能需要将上游的数据进行局部聚合
                     * 流计算模式下，不需要进行合并
                     * @param a
                     * @param b
                     * @return
                     */
                    @Override
                    public Tuple2<String, Integer> merge(Tuple2<String, Integer> a, Tuple2<String, Integer> b) {
                        return null;
                    }
                }).print();
    }

    /**
     * ReduceFunction的用法
     */
    private static void testReduceFunction(DataStream<EventLog> eventStream) {
        eventStream
                .keyBy(EventLog::getEventId)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<EventLog>() {
                    @Override
                    public EventLog reduce(EventLog value1, EventLog value2) throws Exception {
                        return new EventLog(value1.getGuid() + value2.getGuid(), value1.getEventId(), 0L, "return");
                    }
                }).print();

    }

    /**
     * ProcessWindowFunction
     *
     * @param eventStream
     */
    private static void testProcessWindowFunction(DataStream<EventLog> eventStream) {
        eventStream
                .keyBy(EventLog::getEventId)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                /**
                 * 泛型参数：
                 * <IN> 输入的类型，对应aggregate function的输出类型
                 * <OUT> 输出类型.
                 * <KEY> 分组key的类型
                 * <W> 窗口类型
                 */
                .process(new ProcessWindowFunction<EventLog, Tuple2<String, Integer>, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<EventLog> elements, Collector<Tuple2<String, Integer>> out) throws Exception {
                        int sum = 0;
                        Iterator<EventLog> iterator = elements.iterator();
                        while (iterator.hasNext()) {
                            sum += iterator.next().getGuid();
                        }

                        out.collect(Tuple2.of(key, sum));
                    }
                })
                .print();
    }

    /**
     * apply function的用法
     */
    private static void testApplyFunction(DataStream<EventLog> eventStream) {
        eventStream
                .keyBy(EventLog::getEventId)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                /**
                 * 泛型参数：
                 * <IN> 输入的类型，对应aggregate function的输出类型
                 * <OUT> 输出类型.
                 * <KEY> 分组key的类型
                 * <W> 窗口类型
                 */
                .apply(new WindowFunction<EventLog, Tuple2<String, Integer>, String, TimeWindow>() {
                    /**
                     *
                     * @param s 本次传给窗口的时属于哪个key的
                     * @param window 窗口的元数据信息
                     * @param input 窗口中所有数据的迭代器
                     * @param out 结果输出器
                     * @throws Exception
                     */
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<EventLog> input, Collector<Tuple2<String, Integer>> out) throws Exception {
                        int sum = 0;
                        for (EventLog eventLog : input) {
                            sum += eventLog.getGuid();
                        }
                        out.collect(Tuple2.of(s, sum));
                    }
                }).print();
    }

    /**
     * 增量aggregate + ProcessWindowFunction
     *
     * @param eventStream
     */
    private static void testIncrementAggregateFunction(DataStream<EventLog> eventStream) {

        eventStream
                .keyBy(EventLog::getGuid)
                .window(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(10)))
                .aggregate(new AverageAggregate(), new MyProcessWindowFunction())
                .print();
    }


    /**
     * 自定义增量aggregate function
     */
    private static class AverageAggregate implements AggregateFunction<EventLog, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(EventLog value, Long accumulator) {
            System.out.println("------获取到一条数据，执行增量聚合-" + accumulator + "------");
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    /**
     * 泛型
     *
     * <IN> 输入的类型，对应aggregate function的输出类型
     * <OUT> 输出类型.
     * <KEY> 分组key的类型
     * <W> 窗口类型
     */
    private static class MyProcessWindowFunction extends ProcessWindowFunction<Long, Tuple2<String, Long>, Long, TimeWindow> {

        @Override
        public void process(Long key, Context context, Iterable<Long> elements, Collector<Tuple2<String, Long>> out) throws Exception {
            Long count = elements.iterator().next();
            System.out.println("----最终输出------");
            out.collect(Tuple2.of(key + "<-", count));
        }
    }


}
