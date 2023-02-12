package com.flinksql.query;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author: xianmingZhou
 * @Date: 2022/8/3 21:00
 * @Description: window topn
 */
public class WindowTopN {
    public static void main(String[] args) {
        StreamExecutionEnvironment env  = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv  = StreamTableEnvironment.create(env);
        /**
         * 测试数据：
         * 1000,4.00,C,supplier1
         * 2000,8.00,A,supplier1
         * 3000,6.00,B,supplier1
         * 4000,4.00,C,supplier1
         * 5000,4.00,C,supplier1
         */
        SingleOutputStreamOperator<Event> eventStream  = env.socketTextStream("localhost", 9999)
                .map(new MapFunction<String, Event>() {
                    @Override
                    public Event map(String value) throws Exception {
                        String[] fields = value.split(",");
                        return new Event(Long.parseLong(fields[0]), Double.parseDouble(fields[1]), fields[2], fields[3]);
                    }
                });

        tEnv.createTemporaryView("event",eventStream, Schema.newBuilder()
                        .column("bidtime", DataTypes.BIGINT())
                        .column("price", DataTypes.DOUBLE())
                        .column("item", DataTypes.STRING())
                        .column("supplier_id", DataTypes.STRING())
//                        .columnByExpression("rt","to_timestamp_ltz(bidtime,3)")
//                        .watermark("rt","rt - interval '1' second")
                 .columnByExpression("rt","to_timestamp_ltz(bidtime,3)") // 重新利用一个字段 作为事件时间属性
                 .watermark("rt","rt - interval '0' second") // 重新定义表上的watermark策略
                .build());


        tEnv.executeSql("desc event").print();

//        tEnv.executeSql("select bidtime,price,item,supplier_id,rt,current_watermark(rt) wm from event").print();

        // 5秒滚动窗口求价格最高的前两位
            /*tEnv.executeSql("" +
                    "select \n" +
                    " *\n" +
                    " from(\n" +
                    "\tSELECT\n" +
                    "\t bidtime,\n" +
                    "\t price,\n" +
                    "\t window_start,\n" +
                    "\t window_end,\n" +
                    "\t supplier_id,\n" +
                    "\t row_number() over(partition by window_start,window_end order by price desc)as rn\n" +
                    "\tfrom table(tumble(table event,descriptor(rt),interval '5' seconds))\n" +
                    ") where rn<=2")*//*.print()*//*;*/





            // 10秒滚动窗口中，统计交易总额最高的前两家供应商，及其交易总额和交易单数
            tEnv.executeSql("" +
                    "select * from \n" +
                    "(\n" +
                    "\tSELECT\n" +
                    "\t\tsupplier_id,window_start,window_end,total_price,total_cnt,\n" +
                    "\trow_number() over(partition by window_start,window_end ORDER BY total_price DESC,total_cnt DESC) as rn\n" +
                    "\tFROM(\n" +
                    "\t\tSELECT\n" +
                    "\t\t\tsupplier_id,\n" +
                    "\t\t\twindow_start,\n" +
                    "\t\t\twindow_end,\n" +
                    "\t\t\tsum(price) as total_price,\n" +
                    "\t\t\tcount(*) as total_cnt\n" +
                    "\t\tFROM TABLE(\n" +
                    "\t\t\tTUMBLE(TABLE event,descriptor(rt),interval '10' seconds))\n" +
                    "\t\tGROUP BY window_start,window_end,supplier_id\n" +
                    "\t)\n" +
                    ")where rn<=2").print();



    }


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Event{
//        String bidtime;
        long bidtime;
        double price;
        String item;
        String supplier_id;
    }
}
