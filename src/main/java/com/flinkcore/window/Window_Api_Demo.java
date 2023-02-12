package com.flinkcore.window;

import com.flinkcore.EventLog;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * @Author:xmzhou
 * @Date: 2022/7/30 9:25
 * @Description: flink中各种窗口用法
 */
public class Window_Api_Demo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

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

        /**
         * 一、各种全局窗口开窗api
         */

        // 全局 计数滚动窗口
      /*  eventStream.countWindowAll(3)
                .process(new ProcessAllWindowFunction<EventLog, EventLog, GlobalWindow>() {
                    @Override
                    public void process(Context context, Iterable<EventLog> elements, Collector<EventLog> out) throws Exception {
                        for (EventLog element : elements) {
                            System.out.println("==>" + element);
                            if (element.getGuid() == 100) {
                                out.collect(element);
                            }
                        }
                    }
                }).print();*/

        //  全局 计数滑动窗口
    /*    eventStream
                .countWindowAll(3, 1)
                .process(new ProcessAllWindowFunction<EventLog, EventLog, GlobalWindow>() {
                    @Override
                    public void process(Context context, Iterable<EventLog> elements, Collector<EventLog> out) throws Exception {
                        for (EventLog element : elements) {
                            System.out.println("==>" + element);
                            if (element.getGuid() == 100) {
                                out.collect(element);
                            }
                        }
                    }
                }).print();*/

        // 全局 事件时间滚动窗口
       /* eventStream
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new ProcessAllWindowFunction<EventLog, String, TimeWindow>() {

                    @Override
                    public void process(Context context, Iterable<EventLog> elements, Collector<String> out) throws Exception {
                        long start = context.window().getStart();
                        long end = context.window().getEnd();

                        for (EventLog element : elements) {
                            if (element.getGuid() == 100) {
                                out.collect(element.toString() + "<==>" + start + " : " + end);
                            }
                        }
                    }
                }).print();*/

               /* eventStream
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new ProcessAllWindowFunction<EventLog, String, TimeWindow>() {

                    @Override
                    public void process(Context context, Iterable<EventLog> elements, Collector<String> out) throws Exception {
                        long start = context.window().getStart();
                        long end = context.window().getEnd();

                        for (EventLog element : elements) {
                            if (element.getGuid() == 100) {
                                out.collect(element.toString() + "<==>" + start + " : " + end);
                            }
                        }
                    }
                }).print();*/

        // 全局 事件时间滑动窗口
        /*eventStream.windowAll(SlidingEventTimeWindows.of(Time.seconds(30),Time.seconds(10))).apply()*/

        // 全局 事件时间会话窗口
        // 前后两个事件的间隙超过30s就划分窗口
        /*eventStream.windowAll(EventTimeSessionWindows.withGap(Time.seconds(30))).apply()*/


        /**
         * 各种keyed窗口开窗api
         */

        KeyedStream<EventLog, String> keyedStream = eventStream.keyBy(EventLog::getEventId);

        // keyed 计数滚动窗口
        /*keyedStream.countWindow(10);*/

        // keyed 计数滑动窗口
        /*keyedStream.countWindow(30,10);*/

        // keyed 事件时间滚动窗口
        /*keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10)));*/

        // keyed 事件事件滑动窗口
        /*keyedStream.window(SlidingEventTimeWindows.of(Time.seconds(30),Time.seconds(10)));*/

        // keyed 事件时间会话窗口
        /*keyedStream.window(EventTimeSessionWindows.withGap(Time.seconds(30)));*/


        // keyedStream.sum("guid").print();
        // keyedStream.max("guid").print();
        // keyedStream.minBy("guid").print();

        env.execute();
    }
}
