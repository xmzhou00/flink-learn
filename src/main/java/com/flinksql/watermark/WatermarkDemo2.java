package com.flinksql.watermark;

import com.alibaba.fastjson.JSON;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Author:xmzhou
 * @Date: 2022/8/2 12:50
 * @Description: flink sql eventTime and watermark
 * 流  ===>  表 过程中如何传递事件事件和watermark
 */
public class WatermarkDemo2 {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // {"guid":1,"eventId":"e02","eventTime":1655017433000,"pageId":"p001"}
        SingleOutputStreamOperator<Event> eventStream  = env.socketTextStream("localhost", 9999)
                .map(new MapFunction<String, Event>() {
                    @Override
                    public Event map(String value) throws Exception {
                        return JSON.parseObject(value, Event.class);
                    }
                }).assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event element, long recordTimestamp) {
                                        return element.eventTime;
                                    }
                                })
                );



        tEnv.createTemporaryView("t_events",eventStream);

        // 将表转换成流 观察watermark是否存在
        // 通过观察发现，watermark消失了
        Table table = tEnv.from("t_events");
        tEnv.toDataStream(table).process(new ProcessFunction<Row, String>() {
            @Override
            public void processElement(Row value, Context ctx, Collector<String> out) throws Exception {
                out.collect(value+"  ==>"+ctx.timerService().currentWatermark());
            }
        }).print();


        // 在流转表的时候，显示的指定watermark策略
        tEnv.createTemporaryView("t_events2",eventStream, Schema.newBuilder()
                .column("guid", DataTypes.INT())
                .column("eventId", DataTypes.STRING())
                .column("eventTime", DataTypes.BIGINT())
                .column("pageId", DataTypes.STRING())

                 .columnByExpression("rt","to_timestamp_ltz(eventTime,3)") // 重新利用一个字段 作为事件时间属性
                 .watermark("rt","rt - interval '0.001' second") // 重新定义表上的watermark策略

//                .columnByMetadata("rt",DataTypes.TIMESTAMP_LTZ(3),"rowtime") // 利用底层流连接器暴露的rowtime元数据（代表的就是底层流中每条数据的eventTime）
//                .watermark("rt","source_watermark()") // 声明watermark直接引用底层流的watermark
                .build());



        tEnv.executeSql("desc t_events2").print();
        tEnv.executeSql("select guid,eventId,eventTime,pageId,rt,current_watermark(rt) from t_events2").print();




        env.execute();

    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Event {
        public int guid;
        public String eventId;
        public Long eventTime;
        public String pageId;
    }
}
