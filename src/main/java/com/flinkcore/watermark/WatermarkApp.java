package com.flinkcore.watermark;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;


/**
 * @Author:xmzhou
 * @Date: 2022/6/3 12:13
 * @Description: <p>
 * 从 Flink 1.12及其以后，系统默认采用Event Time
 * </p>
 */
public class WatermarkApp {
    public static void main(String[] args) throws Exception {
        Configuration conf  = new Configuration();
        conf.setInteger("rest.port",8088);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(conf);
        env.setParallelism(1);
        SingleOutputStreamOperator<Tuple3<String,Integer,Long>> stream  = env
                .socketTextStream("localhost", 9999)

                .map(new MapFunction<String, Tuple3<String,Integer,Long>>() {
                    public Tuple3<String,Integer,Long> map(String s) throws Exception {
                        String[] fields = s.split(",");
                        return Tuple3.of(fields[0], Integer.parseInt(fields[1]), Long.parseLong(fields[2]));
                    }
                }).returns(new TypeHint<Tuple3<String,Integer,Long>>() {})
                /**
                 * 生成watermark策略：
                 *   1. forBoundedOutOfOrderness：允许乱序
                 *   2. noWatermarks：不生成watermark，禁用事件时间的推进
                 *   3. forMonotonousTimestamps：紧跟最大的事件时间生成watermark，不容忍乱序
                 *   4. forGenerator：自定义
                 */
                .assignTimestampsAndWatermarks( // 本质上也是一个算子
                        WatermarkStrategy.<Tuple3<String,Integer,Long>>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String,Integer,Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple3 tuple3, long l) {
                                        return (long) tuple3.f2;
                                    }
                                })
                ).setParallelism(2);

        SingleOutputStreamOperator<String> result = stream.process(new ProcessFunction<Tuple3<String,Integer,Long>, String>() {

            @Override
            public void processElement(Tuple3<String,Integer,Long> tuple3, Context context, Collector<String> collector) throws Exception {
            // 打印此刻的watermark
                long watermark = context.timerService().currentWatermark();
                long currentProcessingTime = context.timerService().currentProcessingTime();
                System.out.println("数据中的时间戳:"+tuple3.f2);
                System.out.println("此刻的watermark："+watermark);
                System.out.println("此刻的处理时间："+currentProcessingTime);
                collector.collect("hello");
            }
        }).setParallelism(1);

        result.print();
        env.execute();
    }
}
