package com.flinksql.watermark;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

/**
 * @Author:xmzhou
 * @Date: 2022/8/2 12:50
 * @Description: flink sql eventTime and watermark
 * 测试  表 转 流 watermark的变化
 * <p>
 * 测试数据：
 * {"guid":1,"eventId":"e02","eventTime":1655017433000,"pageId":"p001"}
 * {"guid":1,"eventId":"e03","eventTime":1655017434000,"pageId":"p001"}
 * {"guid":1,"eventId":"e04","eventTime":1655017435000,"pageId":"p001"}
 * {"guid":1,"eventI d":"e05","eventTime":1655017436000,"pageId":"p001"}
 * {"guid":1,"eventId":"e06","eventTime":1655017437000,"pageId":"p001"}
 * {"guid":1,"eventId":"e07","eventTime":1655017438000,"pageId":"p001"}
 * {"guid":1,"eventId":"e08","eventTime":1655017439000,"pageId":"p001"}
 * </p>
 */
public class WatermarkDemo3 {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.executeSql("" +
                "create table t_event(\n" +
                "\tguid int,\n" +
                "\teventId string,\n" +
                "\teventTime bigint,\n" +
                "\tpageId string,\n" +
                "\trt as to_timestamp_ltz(eventTime,3),\n" +
                "\twatermark for rt as rt - interval '0.001' second\n" + // 用watermark for xx 来将一个已定义的TIMESTAMP/TIMESTAMP_LTZ字段声明成 eventTime属性及指定watermark策略
                ")WITH(\n" +
                "\t'connector' = 'kafka',\n" +
                "\t'topic' = 'event',\n" +
                "\t'format' = 'json',\n" +
                "\t'properties.bootstrap.servers' = 'hdp01:9092',\n" +
                "\t'scan.startup.mode' = 'earliest-offset',\n" +
                "\t'properties.group.id' = 'g1'\n" +
                ")");
        // tEnv.executeSql("desc t_event").print();
        // tEnv.executeSql("select guid,eventId,eventTime,pageId,rt,current_watermark(rt) wm from t_event").print();


        Table t_event = tEnv.from("t_event");


        tEnv.toDataStream(t_event)
                .process(new ProcessFunction<Row, String>() {
                    @Override
                    public void processElement(Row value, Context ctx, Collector<String> out) throws Exception {
                        out.collect(value + "--> " + ctx.timerService().currentWatermark());
                    }
                })
                .print();

        env.execute();
    }
}
