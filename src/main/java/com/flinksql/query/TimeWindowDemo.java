package com.flinksql.query;

import com.flinkcore.EventLog;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @Author: xianmingZhou
 * @Date: 2022/8/3 15:17
 * @Description: flink 各种窗口
 */
public class TimeWindowDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        SingleOutputStreamOperator<Event> eventStream = env.socketTextStream("localhost", 9999)
                .map(new MapFunction<String, Event>() {
                    @Override
                    public Event map(String value) throws Exception {
                        String[] fields = value.split(",");
                        return new Event(Long.parseLong(fields[0]), fields[1], Long.parseLong(fields[2]), fields[3]);
                    }
                });


        tEnv.createTemporaryView("event", eventStream, Schema.newBuilder()
                .column("guid", DataTypes.BIGINT())
                .column("eventId", DataTypes.STRING())
                .column("ts", DataTypes.BIGINT())
                .column("pageId", DataTypes.STRING())
                .columnByExpression("rt", "to_timestamp_ltz(ts,3)")
                .watermark("rt", "rt - interval '0' second")
                .build());

//        tEnv.executeSql("desc event").print();
//        tEnv.executeSql("select * from event").print();


        // 滚动窗口
        tEnv.executeSql("" +
                "select \n" +
                "window_start,window_end,sum(guid)\n" +
                "from \n" +
                "TABLE(tumble(table event,descriptor(rt),interval '5' seconds))\n" +
                "group by window_start,window_end")/*.print()*/;


// 滑动窗口
        tEnv.executeSql("" +
                "select \n" +
                "window_start,window_end,sum(guid) as cnt\n" +
                "FROM TABLE(\n" +
                "\tHOP(table event,descriptor(rt),interval '1' seconds,interval '5' seconds)\n" +
                ")\n" +
                "group by window_start,window_end")/*.print()*/;


        // 累计窗口
        /**
         * 窗口大小为4秒，每2秒统计一次，在一个窗口内，统计会进行累计，而新窗口中，又会回到初始值
         * 场景：比如每隔1小时，查看当天的累计销售额，就可以使用累计窗口，
         */
        tEnv.executeSql("" +
                "SELECT\n" +
                "window_start,window_end,sum(guid)\n" +
                "FROM TABLE(\n" +
                "\tcumulate(table event,descriptor(rt),interval '2' seconds,interval '4' seconds)\n" +
                ")group by window_start,window_end").print();


    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Event {
        private long guid;
        private String eventId;
        private long ts;
        private String pageId;
    }
}
