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
 * @Date: 2022/8/5 14:50
 * @Description: 时态join
 */
public class TemporalJoinDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        SingleOutputStreamOperator<Order> orderStream  = env.socketTextStream("localhost", 9999)
                .map(new MapFunction<String, Order>() {
                    @Override
                    public Order map(String value) throws Exception {
                        String[] arr = value.split(",");
                        return new Order(Integer.parseInt(arr[0]), arr[1], Double.parseDouble(arr[2]), Long.parseLong(arr[3]));
                    }
                });

        // 创建主表（需要声明处理时间属性字段）
        tenv.createTemporaryView("orders", orderStream, Schema.newBuilder()
                .column("orderId", DataTypes.INT())
                .column("currency", DataTypes.STRING())
                .column("price", DataTypes.DOUBLE())
                .column("orderTime", DataTypes.BIGINT())
                .columnByExpression("rt", "to_timestamp_ltz(orderTime,3)")  // 定义处理时间属性字段
                .watermark("rt","rt")
                .build());




    }


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Order {
        // 订单Id，币种，金额，订单时间
        public int orderId;
        public String currency;
        public double price;
        public long orderTime;

    }
}
