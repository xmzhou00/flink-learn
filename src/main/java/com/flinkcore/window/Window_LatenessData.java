package com.flinkcore.window;

import com.flinkcore.EventLog;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @Author:xmzhou
 * @Date: 2022/7/30 13:03
 * @Description: Flink 窗口中延迟数据的处理
 */
public class Window_LatenessData {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        SingleOutputStreamOperator<EventLog> eventStream = env.socketTextStream("localhost", 9999)
                .map(new MapFunction<String, EventLog>() {
                    @Override
                    public EventLog map(String value) throws Exception {
                        String[] fields = value.split(",");
                        return new EventLog(Long.parseLong(fields[0]), fields[1], Long.parseLong(fields[2]), fields[3]);
                    }

                }).returns(EventLog.class)
                // 抽取watermark
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<EventLog>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<EventLog>() {
                            @Override
                            public long extractTimestamp(EventLog element, long recordTimestamp) {
                                return element.getTimeStamp();
                            }
                        }));

        OutputTag<EventLog> outputTag = new OutputTag<>("LateData", TypeInformation.of(EventLog.class));

        SingleOutputStreamOperator<String> resultStream = eventStream
                .keyBy(EventLog::getEventId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .allowedLateness(Time.seconds(2)) // 允许迟到2s
                .sideOutputLateData(outputTag)
                .process(new ProcessWindowFunction<EventLog, String, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<EventLog> elements, Collector<String> out) throws Exception {

                        int sum = 0;
                        for (EventLog element : elements) {
                            sum += element.getGuid();
                        }
                        long currentWatermark = context.currentWatermark();
                        long windowStart = context.window().getStart();
                        long windowEnd = context.window().getEnd();

                        out.collect("key: " + key + " sum: " + sum + " start: " + windowStart + " end: " + windowEnd + " watermark: " + currentWatermark);
                    }
                });

        resultStream
                .print("正常窗口数据：");

        resultStream.getSideOutput(outputTag).print("延迟数据： ");


        env.execute();

    }
}
